package petname

import (
	"math"
	"math/bits"
	"unsafe"
)

const (
	flagsEmpty    = 0b00000000
	flagsReserved = 0b01111110
	flagsHit      = 0b10000000
	flagsList     = 0b01000000

	maskHit      = 0b10000000
	maskDistance = 0b00111111

	maxLoadFactor = 0.80
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

type slot struct {
	k struct{ Hi, Lo uint64 }
	v uint32
	m uint8
}

type slotIndex struct {
	s *slot
	i uint64
}

func (si slotIndex) slot() slot       { return *si.s }
func (si slotIndex) setSlot(s slot)   { *si.s = s }
func (si slotIndex) meta() uint8      { return si.s.m }
func (si slotIndex) setMeta(m uint8)  { si.s.m = m }
func (si slotIndex) setJump(ji uint8) { si.setMeta(si.meta()&^maskDistance | ji) }
func (si slotIndex) hasJump() bool    { return si.meta()&maskDistance != 0 }
func (si slotIndex) jump() uint8      { return si.meta() & maskDistance }

type table struct {
	slots []slot
	mask  uint64
	shift uint64
	eles  int
}

var emptySlots = []slot{{}, {}}

func newTable() *table {
	return &table{
		slots: emptySlots,
		shift: 63,
	}
}

func (t *table) Len() int { return t.eles }

func (t *table) Size() uint64 {
	if t == nil {
		return 0
	}
	return uint64(unsafe.Sizeof(slot{})) * uint64(len(t.slots))
}

func (t *table) Load() float64 {
	return float64(t.eles) / float64(t.mask+1)
}

func (t *table) getSlotIndex(i uint64) slotIndex {
	return slotIndex{
		s: &t.slots[i],
		i: i,
	}
}

func (t *table) next(si slotIndex, ji uint8) slotIndex {
	next := (si.i + uint64(jumpDistances[ji])) & t.mask
	return t.getSlotIndex(next)
}

func (t *table) index(k Hash) uint64 {
	return (11400714819323198485 * (k.Lo + k.Hi)) >> (t.shift % 64)
}

func (t *table) Find(k Hash) (uint32, bool) {
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

func (t *table) Insert(k Hash, v uint32) (uint32, bool) {
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

func (t *table) insertDirectHit(si slotIndex, k Hash, v uint32) (uint32, bool) {
	if t.isFull() {
		t.grow()
		return t.Insert(k, v)
	}

	if si.meta() == flagsEmpty {
		si.setSlot(slot{k, v, flagsHit})
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

	si.setSlot(slot{k, v, flagsHit})
	t.eles++
	return v, false
}

func (t *table) insertNew(si slotIndex, k Hash, v uint32) (uint32, bool) {
	if t.isFull() {
		t.grow()
		return t.Insert(k, v)
	}

	free, ji := t.findFree(si)
	if ji == 0 {
		t.grow()
		return t.Insert(k, v)
	}

	free.setSlot(slot{k, v, flagsList})
	si.setJump(ji)
	t.eles++
	return v, false
}

func (t *table) isFull() bool {
	return t.mask == 0 || t.eles+1 > int(float64(t.mask+1)*maxLoadFactor)
}

func (t *table) findDirectHit(si slotIndex) slotIndex {
	return t.getSlotIndex(t.index(si.slot().k))
}

func (t *table) findParent(si slotIndex) slotIndex {
	parent := t.findDirectHit(si)
	for {
		next := t.next(parent, parent.jump())
		if next == si {
			return parent
		}
		parent = next
	}
}

func (t *table) findFree(si slotIndex) (slotIndex, uint8) {
	for ji := uint8(1); ji < uint8(len(jumpDistances)); ji++ {
		if si := t.next(si, ji); si.meta() == flagsEmpty {
			return si, ji
		}
	}
	return slotIndex{}, 0
}

// TODO: maybe we can do background growth to avoid latency spikes
// past the initial memory allocation.

func (t *table) grow() {
	nslots := max(10, 2*t.mask)
	nslots = max(nslots, uint64(math.Ceil(float64(t.eles)/maxLoadFactor)))
	nslots = max(128, np2(nslots))

	slots := t.slots
	t.shift = 64 - log2(nslots)
	t.slots = make([]slot, nslots)
	t.mask = nslots - 1
	t.eles = 0

	for i := range slots {
		s := &slots[i]
		if m := s.m; m != flagsEmpty && m != flagsReserved {
			t.Insert(s.k, s.v)
		}
	}
}
