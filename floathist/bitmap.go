package floathist

import (
	"math/bits"
	"sync/atomic"
)

type l0Bitmap [1]uint64

func (b *l0Bitmap) UnsafeClone() l0Bitmap   { return *b }
func (b *l0Bitmap) UnsafeUint() uint64      { return b[0] }
func (b *l0Bitmap) UnsafeSetUint(v uint64)  { b[0] = v }
func (b *l0Bitmap) UnsafeSetIdx(idx uint32) { b[0] += 1 << (idx & l0Bitmask) }

func (b *l0Bitmap) Clone() l0Bitmap        { return l0Bitmap{atomic.LoadUint64(&b[0])} }
func (b *l0Bitmap) SetIdx(idx uint32)      { atomic.AddUint64(&b[0], 1<<(idx&l0Bitmask)) }
func (b *l0Bitmap) HasIdx(idx uint32) bool { return atomic.LoadUint64(&b[0])&(1<<(idx&l0Bitmask)) > 0 }

func (b *l0Bitmap) Next() (idx uint32, ok bool) {
	u := b[0]
	c := u & (u - 1)
	idx = uint32(bits.Len64(u ^ c))
	b[0] = c
	return (idx - 1) % l0Size, u > 0
}

type l1Bitmap [1]uint64

func (b *l1Bitmap) UnsafeClone() l1Bitmap   { return *b }
func (b *l1Bitmap) UnsafeUint() uint64      { return b[0] }
func (b *l1Bitmap) UnsafeSetUint(v uint64)  { b[0] = v }
func (b *l1Bitmap) UnsafeSetIdx(idx uint32) { b[0] += 1 << (idx & l1Bitmask) }

func (b *l1Bitmap) Clone() l1Bitmap        { return l1Bitmap{atomic.LoadUint64(&b[0])} }
func (b *l1Bitmap) SetIdx(idx uint32)      { atomic.AddUint64(&b[0], 1<<(idx&l1Bitmask)) }
func (b *l1Bitmap) HasIdx(idx uint32) bool { return atomic.LoadUint64(&b[0])&(1<<(idx&l1Bitmask)) > 0 }

func (b *l1Bitmap) Next() (idx uint32, ok bool) {
	u := b[0]
	c := u & (u - 1)
	idx = uint32(bits.Len64(u ^ c))
	b[0] = c
	return (idx - 1) % l1Size, u > 0
}

type l2Bitmap [1]uint64

func (b *l2Bitmap) UnsafeClone() l2Bitmap   { return *b }
func (b *l2Bitmap) UnsafeUint() uint64      { return b[0] }
func (b *l2Bitmap) UnsafeSetUint(v uint64)  { b[0] = v }
func (b *l2Bitmap) UnsafeSetIdx(idx uint32) { b[0] += 1 << (idx & l2Bitmask) }

func (b *l2Bitmap) Clone() l2Bitmap        { return l2Bitmap{atomic.LoadUint64(&b[0])} }
func (b *l2Bitmap) SetIdx(idx uint32)      { atomic.AddUint64(&b[0], 1<<(idx&l2Bitmask)) }
func (b *l2Bitmap) HasIdx(idx uint32) bool { return atomic.LoadUint64(&b[0])&(1<<(idx&l2Bitmask)) > 0 }

func (b *l2Bitmap) Next() (idx uint32, ok bool) {
	u := b[0]
	c := u & (u - 1)
	idx = uint32(bits.Len64(u ^ c))
	b[0] = c
	return (idx - 1) % l2Size, u > 0
}
