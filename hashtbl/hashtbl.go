package hashtbl

import (
	"math"
	"math/bits"

	"github.com/histdb/histdb/rwutils"
	"github.com/histdb/histdb/sizeof"
)

type Key interface {
	comparable
	Digest() uint64
}

type E struct{}

func (E) ReadFrom(r *rwutils.R)  {}
func (*E) AppendTo(w *rwutils.W) {}

type U64 uint64

func (u U64) Digest() uint64         { return uint64(u) }
func (u *U64) ReadFrom(r *rwutils.R) { *u = U64(r.Uint64()) }
func (u U64) AppendTo(w *rwutils.W)  { w.Uint64(uint64(u)) }

type U32 uint64

func (u U32) Digest() uint64         { return uint64(u) }
func (u *U32) ReadFrom(r *rwutils.R) { *u = U32(r.Uint32()) }
func (u U32) AppendTo(w *rwutils.W)  { w.Uint32(uint32(u)) }

type U16 uint64

func (u U16) Digest() uint64         { return uint64(u) }
func (u *U16) ReadFrom(r *rwutils.R) { *u = U16(r.Uint16()) }
func (u U16) AppendTo(w *rwutils.W)  { w.Uint16(uint16(u)) }

const (
	flagsEmpty    = 0b00000000
	flagsReserved = 0b01111110
	flagsHit      = 0b10000000
	flagsList     = 0b01000000

	maskHit      = 0b10000000
	maskDistance = 0b00111111

	maxLoadFactor = 0.9
)

var jumpDistances = [64]uint16{
	0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
	21, 28, 36, 45, 55, 66, 78, 91, 105, 120, 136, 153, 171, 190, 210, 231,
	253, 276, 300, 325, 351, 378, 406, 435, 465, 496, 528, 561, 595, 630,
	666, 703, 741, 780, 820, 861, 903, 946, 990, 1035, 1081, 1128, 1176,
	1225, 1275, 1326, 1378, 1431,
}

func max(x, y uint64) uint64 {
	if x > y {
		return x
	}
	return y
}

func np2(x uint64) uint64  { return 1 << (uint(bits.Len64(x-1)) % 64) }
func log2(x uint64) uint64 { return uint64(bits.Len64(x)-1) % 64 }

type slot[K, V any] struct {
	_ [0]func() // no equality

	k K
	v V
}

type slotIndex[K, V any] struct {
	_ [0]func() // no equality

	s *slot[K, V]
	m *uint8
	i uint64
}

func (si slotIndex[K, V]) slot() slot[K, V]     { return *si.s }
func (si slotIndex[K, V]) setSlot(s slot[K, V]) { *si.s = s }

func (si slotIndex[K, V]) meta() uint8     { return *si.m }
func (si slotIndex[K, V]) setMeta(m uint8) { *si.m = m }

func (si slotIndex[K, V]) setJump(ji uint8) { si.setMeta(si.meta()&^maskDistance | ji) }
func (si slotIndex[K, V]) hasJump() bool    { return si.meta()&maskDistance != 0 }
func (si slotIndex[K, V]) jump() uint8      { return si.meta() & maskDistance }

type T[K Key, V any] struct {
	_ [0]func() // no equality

	slots []slot[K, V]
	metas []uint8
	mask  uint64
	shift uint64
	eles  int
	full  int
}

func (t *T[K, V]) Len() int { return t.eles }

func (t *T[K, V]) Size() uint64 {
	return 0 +
		/* slots */ sizeof.Slice(t.slots) +
		/* metas */ sizeof.Slice(t.metas) +
		/* mask  */ 8 +
		/* shift */ 8 +
		/* eles  */ 8 +
		/* full  */ 8 +
		0
}

func (t *T[K, V]) Load() float64 {
	return float64(t.eles) / float64(t.mask+1)
}

func (t *T[K, V]) getSlotIndex(i uint64) slotIndex[K, V] {
	return slotIndex[K, V]{
		s: &t.slots[i],
		m: &t.metas[i],
		i: i,
	}
}

func (t *T[K, V]) next(si slotIndex[K, V], ji uint8) slotIndex[K, V] {
	next := (si.i + uint64(jumpDistances[ji])) & t.mask
	return t.getSlotIndex(next)
}

func (t *T[K, V]) index(k K) uint64 {
	return (11400714819323198485 * k.Digest()) >> (t.shift % 64)
}

func (t *T[K, V]) Find(k K) (v V, ok bool) {
	if t.eles == 0 {
		return v, false
	}
	si := t.getSlotIndex(t.index(k))
	if si.meta()&maskHit != flagsHit {
		return v, false
	}
	for {
		if s := si.slot(); s.k == k {
			return s.v, true
		}
		ji := si.jump()
		if ji == 0 {
			return v, false
		}
		si = t.next(si, ji)
	}
}

func (t *T[K, V]) Insert(k K, v V) (V, bool) {
	if t.isFull() {
		t.grow()
	}
	si := t.getSlotIndex(t.index(k))
	if si.meta()&maskHit != flagsHit {
		return t.insertDirectHit(si, k, v)
	}
	for {
		if s := si.slot(); s.k == k {
			return s.v, true
		}
		ji := si.jump()
		if ji == 0 {
			return t.insertNew(si, k, v)
		}
		si = t.next(si, ji)
	}
}

func (t *T[K, V]) insertDirectHit(si slotIndex[K, V], k K, v V) (V, bool) {
	if si.meta() == flagsEmpty {
		si.setSlot(slot[K, V]{k: k, v: v})
		si.setMeta(flagsHit)
		t.eles++
		return v, false
	}

	parent := t.findParent(si)
	free, ji := t.findFree(parent)
	if ji == 0 {
		t.grow()
		return t.Insert(k, v)
	}

	for it := si; ; {
		free.setSlot(it.slot())
		free.setMeta(it.meta() | flagsList)
		parent.setJump(ji)

		if !it.hasJump() {
			it.setMeta(flagsEmpty)
			break
		}

		next := t.next(it, it.jump())
		it.setMeta(flagsEmpty)
		si.setMeta(flagsReserved)
		it, parent = next, free

		free, ji = t.findFree(free)
		if ji == 0 {
			t.grow()
			return t.Insert(k, v)
		}
	}

	si.setSlot(slot[K, V]{k: k, v: v})
	si.setMeta(flagsHit)
	t.eles++
	return v, false
}

func (t *T[K, V]) insertNew(si slotIndex[K, V], k K, v V) (V, bool) {
	free, ji := t.findFree(si)
	if ji == 0 {
		t.grow()
		return t.Insert(k, v)
	}

	free.setSlot(slot[K, V]{k: k, v: v})
	free.setMeta(flagsHit | flagsList)
	si.setJump(ji)
	t.eles++
	return v, false
}

func (t *T[K, V]) isFull() bool {
	return t.eles >= t.full
}

func (t *T[K, V]) findDirectHit(si slotIndex[K, V]) slotIndex[K, V] {
	return t.getSlotIndex(t.index(si.slot().k))
}

func (t *T[K, V]) findParent(si slotIndex[K, V]) slotIndex[K, V] {
	parent := t.findDirectHit(si)
	for {
		next := t.next(parent, parent.jump())
		if next.s == si.s {
			return parent
		}
		parent = next
	}
}

func (t *T[K, V]) findFree(si slotIndex[K, V]) (slotIndex[K, V], uint8) {
	for ji := uint8(1); ji < uint8(len(jumpDistances)); ji++ {
		if si := t.next(si, ji); si.meta() == flagsEmpty {
			return si, ji
		}
	}
	return slotIndex[K, V]{}, 0
}

// TODO: maybe we can do background growth to avoid latency spikes
// past the initial memory allocation.

func (t *T[K, V]) grow() {
	nslots := max(16, 2*t.mask)
	nslots = max(nslots, uint64(math.Ceil(float64(t.eles)/maxLoadFactor)))
	nslots = np2(nslots)

	slots, metas := t.slots, t.metas

	t.shift = 64 - log2(nslots)
	t.slots = make([]slot[K, V], nslots)
	t.metas = make([]uint8, nslots)
	t.mask = nslots - 1
	t.eles = 0
	t.full = int(float64(nslots) * maxLoadFactor)

	for i, m := range metas {
		if m != flagsEmpty && m != flagsReserved {
			s := &slots[i]
			t.Insert(s.k, s.v)
		}
	}
}
