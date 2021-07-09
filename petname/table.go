package petname

import (
	"math"
	"math/bits"
	"unsafe"
)

const (
	magicForEmpty     = 0b00000000
	magicForReserved  = 0b01111110
	magicForDirectHit = 0b10000000
	magicForListEntry = 0b01000000

	bitsForDirectHit = 0b10000000
	bitsForDistance  = 0b00111111

	maxLoadFactor = 0.9375
)

var jumpDistances = [64]uint64{
	0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,

	21, 28, 36, 45, 55, 66, 78, 91, 105, 120, 136, 153, 171, 190, 210, 231,
	253, 276, 300, 325, 351, 378, 406, 435, 465, 496, 528, 561, 595, 630,
	666, 703, 741, 780, 820, 861, 903, 946, 990, 1035, 1081, 1128, 1176,
	1225, 1275, 1326, 1378, 1431,

	// 1485, 1540, 1596, 1653, 1711, 1770, 1830,
	// 1891, 1953, 2016, 2080, 2145, 2211, 2278, 2346, 2415, 2485, 2556,

	// 3741, 8385, 18915, 42486, 95703, 215496, 485605, 1091503, 2456436,
	// 5529475, 12437578, 27986421, 62972253, 141700195, 318819126, 717314626,
	// 1614000520, 3631437253, 8170829695, 18384318876, 41364501751,
	// 93070021080, 209407709220, 471167588430, 1060127437995, 2385287281530,
	// 5366895564381, 12075513791265, 27169907873235, 61132301007778,
	// 137547673121001, 309482258302503, 696335090510256, 1566753939653640,
	// 3525196427195653, 7931691866727775, 17846306747368716,
	// 40154190394120111, 90346928493040500, 203280588949935750,
	// 457381324898247375, 1029107980662394500, 2315492957028380766,
	// 5209859150892887590,
}

func max(x, y uint64) uint64 {
	if x > y {
		return x
	}
	return y
}

func np2(x uint64) uint64 {
	return 1 << (uint(bits.Len64(x-1)) % 64)
}

func log2(x uint64) uint64 {
	return uint64(bits.Len64(x)-1) % 64
}

type fibonacci struct{ shift uint64 }

func (f fibonacci) index(hash uint64) uint64 {
	return (11400714819323198485 * hash) >> (f.shift % 64)
}

type slot struct {
	k Hash
	v uint32
	m uint8
}

const blockSize = 8

type block struct {
	// meta  [blockSize]byte
	slots [blockSize]slot
}

type blockIndex struct {
	blk *block
	idx uint64
}

func (bi blockIndex) next(t *table) blockIndex {
	dist := bi.meta() & bitsForDistance
	next := (bi.idx + jumpDistances[dist]) & t.slots
	return t.getBlockIndex(next)
}

func (bi blockIndex) slot() slot     { return bi.blk.slots[bi.idx%blockSize] }
func (bi blockIndex) setSlot(s slot) { bi.blk.slots[bi.idx%blockSize] = s }

func (bi blockIndex) meta() byte     { return bi.blk.slots[bi.idx%blockSize].m }
func (bi blockIndex) setMeta(m byte) { bi.blk.slots[bi.idx%blockSize].m = m }

func (bi blockIndex) setNext(ji uint8) { bi.setMeta(bi.meta()&^bitsForDistance | ji) }
func (bi blockIndex) hasNext() bool    { return bi.meta()&bitsForDistance != 0 }

type table struct {
	blocks []block
	slots  uint64
	fib    fibonacci
	eles   int
}

var emptyBlocks = []block{{}}

func newTable() *table {
	return &table{
		blocks: emptyBlocks,
		fib:    fibonacci{shift: 63},
	}
}

func (t *table) size() uint64 {
	if t == nil {
		return 0
	}
	return uint64(unsafe.Sizeof(block{})) * uint64(len(t.blocks))
}

func (t *table) find(k Hash) (uint32, bool) {
	idx := t.fib.index(k.Lo)
	bi := t.getBlockIndex(idx)
	meta := bi.meta()

	if meta&bitsForDirectHit != magicForDirectHit {
		return 0, false
	}

	for {
		if s := bi.slot(); s.k == k {
			return s.v, true
		}

		next := meta & bitsForDistance
		if next == 0 {
			return 0, false
		}
		idx = (idx + jumpDistances[next]) & t.slots

		bi = t.getBlockIndex(idx)
		meta = bi.meta()
	}
}

func (t *table) insert(k Hash, v uint32) (uint32, bool) {
	idx := t.fib.index(k.Lo)
	bi := t.getBlockIndex(idx)
	meta := bi.meta()

	if meta&bitsForDirectHit != magicForDirectHit {
		return t.insertDirectHit(bi, k, v)
	}

	for {
		if s := bi.slot(); s.k == k {
			return s.v, true
		}

		next := meta & bitsForDistance
		if next == 0 {
			return t.insertNew(bi, k, v)
		}
		idx = (idx + jumpDistances[next]) & t.slots

		bi = t.getBlockIndex(idx)
		meta = bi.meta()
	}
}

func (t *table) insertDirectHit(bi blockIndex, k Hash, v uint32) (uint32, bool) {
	if t.isFull() {
		t.grow()
		return t.insert(k, v)
	}

	if bi.meta() == magicForEmpty {
		bi.setSlot(slot{k, v, magicForDirectHit})
		// bi.setMeta(magicForDirectHit)
		t.eles++
		return v, false
	}

	parent := t.findParent(bi)
	free, ji := t.findFree(parent)
	if ji == 0 {
		t.grow()
		return t.insert(k, v)
	}

	for it := bi; ; {
		free.setSlot(it.slot())
		parent.setNext(ji)
		free.setMeta(magicForListEntry)

		if !it.hasNext() {
			it.setMeta(magicForEmpty)
			break
		}

		next := it.next(t)
		it.setMeta(magicForEmpty)
		bi.setMeta(magicForReserved)
		it, parent = next, free

		free, ji = t.findFree(free)
		if ji == 0 {
			t.grow()
			return t.insert(k, v)
		}
	}

	bi.setSlot(slot{k, v, magicForDirectHit})
	// bi.setMeta(magicForDirectHit)
	t.eles++
	return v, false
}

func (t *table) insertNew(bi blockIndex, k Hash, v uint32) (uint32, bool) {
	if t.isFull() {
		t.grow()
		return t.insert(k, v)
	}

	free, ji := t.findFree(bi)
	if ji == 0 {
		t.grow()
		return t.insert(k, v)
	}

	free.setSlot(slot{k, v, magicForListEntry})
	// free.setMeta(magicForListEntry)
	bi.setNext(ji)
	t.eles++
	return v, false
}

func (t *table) getBlockIndex(idx uint64) blockIndex {
	blk := &t.blocks[idx/blockSize]
	return blockIndex{blk, idx}
}

func (t *table) isFull() bool {
	return t.slots == 0 || t.eles+1 > int(float64(t.slots+1)*maxLoadFactor)
}

func (t *table) findDirectHit(bi blockIndex) blockIndex {
	return t.getBlockIndex(t.fib.index(bi.slot().k.Lo))
}

func (t *table) findParent(bi blockIndex) blockIndex {
	parent := t.findDirectHit(bi)
	for {
		next := parent.next(t)
		if next == bi {
			return parent
		}
		parent = next
	}
}

func (t *table) findFree(bi blockIndex) (blockIndex, uint8) {
	for ji := uint8(1); ji < uint8(len(jumpDistances))-1; ji++ {
		idx := (bi.idx + jumpDistances[ji]) & t.slots
		bi := t.getBlockIndex(idx)
		if bi.meta() == magicForEmpty {
			return bi, ji
		}
	}
	return blockIndex{}, 0
}

func (t *table) grow() {
	nitems := max(10, 2*t.slots)
	nitems = max(nitems, uint64(math.Ceil(float64(t.eles)/maxLoadFactor)))
	nitems = max(2, np2(nitems))

	nblocks := nitems/blockSize + 1
	if nitems%blockSize != 0 {
		nblocks++
	}
	oldBlocks := t.blocks

	t.fib.shift = 64 - log2(nitems)
	t.blocks = make([]block, nblocks)
	t.slots = nitems - 1

	for i := range oldBlocks {
		blk := &oldBlocks[i]
		for j := 0; j < blockSize; j++ {
			meta := blk.slots[j].m
			if meta != magicForEmpty && meta != magicForReserved {
				s := blk.slots[j]
				t.insert(s.k, s.v)
			}
		}
	}
}
