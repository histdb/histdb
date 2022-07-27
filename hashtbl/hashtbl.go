package hashtbl

import (
	"math"
	"math/bits"
	"unsafe"

	"github.com/histdb/histdb/rwutils"
)

type Key interface {
	comparable
	Digest() uint64
}

type RWKey[K Key] interface {
	*K
	rwutils.RW
}

type U64 uint64

func (u U64) Digest() uint64         { return uint64(u) }
func (u *U64) ReadFrom(r *rwutils.R) { *u = U64(r.Uint64()) }
func (u U64) AppendTo(w *rwutils.W)  { w.Uint64(uint64(u)) }

const (
	flagsEmpty    = 0b00000000
	flagsReserved = 0b01111110
	flagsHit      = 0b10000000
	flagsList     = 0b01000000

	maskHit      = 0b10000000
	maskDistance = 0b00111111

	maxLoadFactor = 0.8
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

type slot[K Key, RWK RWKey[K]] struct {
	k K
	v uint32
	m uint8
}

type slotIndex[K Key, RWK RWKey[K]] struct {
	s *slot[K, RWK]
	i uint64
}

func (si slotIndex[K, RWK]) slot() slot[K, RWK]     { return *si.s }
func (si slotIndex[K, RWK]) setSlot(s slot[K, RWK]) { *si.s = s }
func (si slotIndex[K, RWK]) meta() uint8            { return si.s.m }
func (si slotIndex[K, RWK]) setMeta(m uint8)        { si.s.m = m }
func (si slotIndex[K, RWK]) setJump(ji uint8)       { si.setMeta(si.meta()&^maskDistance | ji) }
func (si slotIndex[K, RWK]) hasJump() bool          { return si.meta()&maskDistance != 0 }
func (si slotIndex[K, RWK]) jump() uint8            { return si.meta() & maskDistance }

type T[K Key, RWK RWKey[K]] struct {
	slots []slot[K, RWK]
	mask  uint64
	shift uint64
	eles  int
	full  int
}

func (t *T[K, RWK]) Len() int { return t.eles }

func (t *T[K, RWK]) Size() uint64 {
	return 0 +
		/* slots */ 24 + uint64(unsafe.Sizeof(slot[K, RWK]{}))*uint64(len(t.slots)) +
		/* mask  */ 8 +
		/* shift */ 8 +
		/* eles  */ 8 +
		/* full  */ 8 +
		0
}

func (t *T[K, RWK]) Load() float64 {
	return float64(t.eles) / float64(t.mask+1)
}

func (t *T[K, RWK]) getSlotIndex(i uint64) slotIndex[K, RWK] {
	return slotIndex[K, RWK]{
		s: &t.slots[i],
		i: i,
	}
}

func (t *T[K, RWK]) next(si slotIndex[K, RWK], ji uint8) slotIndex[K, RWK] {
	next := (si.i + uint64(jumpDistances[ji])) & t.mask
	return t.getSlotIndex(next)
}

func (t *T[K, RWK]) index(k K) uint64 {
	return (11400714819323198485 * k.Digest()) >> (t.shift % 64)
}

func (t *T[K, RWK]) Find(k K) (uint32, bool) {
	si := t.getSlotIndex(t.index(k))
	if si.meta()&maskHit != flagsHit {
		return 0, false
	}
	for {
		if s := si.slot(); s.k == k {
			return s.v, true
		}
		ji := si.jump()
		if ji == 0 {
			return 0, false
		}
		si = t.next(si, ji)
	}
}

func (t *T[K, RWK]) Insert(k K, v uint32) (uint32, bool) {
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

func (t *T[K, RWK]) insertDirectHit(si slotIndex[K, RWK], k K, v uint32) (uint32, bool) {
	if si.meta() == flagsEmpty {
		si.setSlot(slot[K, RWK]{k, v, flagsHit})
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
		parent.setJump(ji)
		free.setMeta(flagsList)

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

	si.setSlot(slot[K, RWK]{k, v, flagsHit})
	t.eles++
	return v, false
}

func (t *T[K, RWK]) insertNew(si slotIndex[K, RWK], k K, v uint32) (uint32, bool) {
	free, ji := t.findFree(si)
	if ji == 0 {
		t.grow()
		return t.Insert(k, v)
	}

	free.setSlot(slot[K, RWK]{k, v, flagsList})
	si.setJump(ji)
	t.eles++
	return v, false
}

func (t *T[K, RWK]) isFull() bool {
	return t.eles >= t.full
}

func (t *T[K, RWK]) findDirectHit(si slotIndex[K, RWK]) slotIndex[K, RWK] {
	return t.getSlotIndex(t.index(si.slot().k))
}

func (t *T[K, RWK]) findParent(si slotIndex[K, RWK]) slotIndex[K, RWK] {
	parent := t.findDirectHit(si)
	for {
		next := t.next(parent, parent.jump())
		if next == si {
			return parent
		}
		parent = next
	}
}

func (t *T[K, RWK]) findFree(si slotIndex[K, RWK]) (slotIndex[K, RWK], uint8) {
	for ji := uint8(1); ji < uint8(len(jumpDistances)); ji++ {
		if si := t.next(si, ji); si.meta() == flagsEmpty {
			return si, ji
		}
	}
	return slotIndex[K, RWK]{}, 0
}

// TODO: maybe we can do background growth to avoid latency spikes
// past the initial memory allocation.

func (t *T[K, RWK]) grow() {
	nslots := max(10, 2*t.mask)
	nslots = max(nslots, uint64(math.Ceil(float64(t.eles)/maxLoadFactor)))
	nslots = max(128, np2(nslots))

	slots := t.slots
	t.shift = 64 - log2(nslots)
	t.slots = make([]slot[K, RWK], nslots)
	t.mask = nslots - 1
	t.eles = 0
	t.full = int(float64(nslots) * maxLoadFactor)

	for i := range slots {
		s := &slots[i]
		if m := s.m; m != flagsEmpty && m != flagsReserved {
			t.Insert(s.k, s.v)
		}
	}
}

func (t *T[K, RWK]) AppendTo(w *rwutils.W) {
	w.Uint64(uint64(len(t.slots)))
	w.Uint64(t.mask)
	w.Uint64(t.shift)
	w.Uint64(uint64(t.eles))
	w.Uint64(uint64(t.full))

	for i := range t.slots {
		s := &t.slots[i]

		RWK(&s.k).AppendTo(w)
		w.Uint32(s.v)
		w.Uint32(uint32(s.m))
	}
}

func (t *T[K, RWK]) ReadFrom(r *rwutils.R) {
	n := r.Uint64()
	t.mask = r.Uint64()
	t.shift = r.Uint64()
	t.eles = int(r.Uint64())
	t.full = int(r.Uint64())

	t.slots = make([]slot[K, RWK], n)
	for i := range t.slots {
		s := &t.slots[i]

		RWK(&s.k).ReadFrom(r)
		s.v = r.Uint32()
		s.m = uint8(r.Uint32())
	}
}
