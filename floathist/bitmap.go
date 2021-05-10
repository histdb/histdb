package floathist

import (
	"math/bits"
	"sync/atomic"
)

type l0Bitmap [1]uint64

func (b *l0Bitmap) UnsafeSetUint(v uint64)  { b[0] = v & l0Mask }
func (b *l0Bitmap) UnsafeSetIdx(idx uint32) { b[0] += 1 << (idx & l0Bitmask) }

func (b *l0Bitmap) Clone() l0Bitmap        { return l0Bitmap{atomic.LoadUint64(&b[0])} }
func (b *l0Bitmap) uint64() uint64         { return b[0] }
func (b *l0Bitmap) SetIdx(idx uint32)      { atomic.AddUint64(&b[0], 1<<(idx&l0Bitmask)) }
func (b *l0Bitmap) HasIdx(idx uint32) bool { return atomic.LoadUint64(&b[0])&(1<<(idx&l0Bitmask)) > 0 }

func (b *l0Bitmap) Next()          { b[0] &= b[0] - 1 }
func (b l0Bitmap) Empty() bool     { return b[0] == 0 }
func (b l0Bitmap) Lowest() uint32  { return uint32(bits.TrailingZeros64(b[0])) % l0Size }
func (b l0Bitmap) Highest() uint32 { return uint32(63-bits.LeadingZeros64(b[0])) % l0Size }

type l1Bitmap [1]uint64

func (b *l1Bitmap) UnsafeSetUint(v uint64)  { b[0] = v & l1Mask }
func (b *l1Bitmap) UnsafeSetIdx(idx uint32) { b[0] += 1 << (idx & l1Bitmask) }

func (b *l1Bitmap) Clone() l1Bitmap        { return l1Bitmap{atomic.LoadUint64(&b[0])} }
func (b *l1Bitmap) Uint64() uint64         { return b[0] }
func (b *l1Bitmap) SetIdx(idx uint32)      { atomic.AddUint64(&b[0], 1<<(idx&l1Bitmask)) }
func (b *l1Bitmap) HasIdx(idx uint32) bool { return atomic.LoadUint64(&b[0])&(1<<(idx&l1Bitmask)) > 0 }

func (b *l1Bitmap) Next()          { b[0] &= b[0] - 1 }
func (b l1Bitmap) Empty() bool     { return b[0] == 0 }
func (b l1Bitmap) Lowest() uint32  { return uint32(bits.TrailingZeros64(b[0])) % l1Size }
func (b l1Bitmap) Highest() uint32 { return uint32(63-bits.LeadingZeros64(b[0])) % l1Size }

type l2Bitmap [1]uint64

func (b *l2Bitmap) UnsafeSetUint(v uint64)  { b[0] = v & l2Mask }
func (b *l2Bitmap) UnsafeSetIdx(idx uint32) { b[0] += 1 << (idx & l2Bitmask) }

func (b *l2Bitmap) Clone() l2Bitmap        { return l2Bitmap{atomic.LoadUint64(&b[0])} }
func (b *l2Bitmap) Uint64() uint64         { return b[0] }
func (b *l2Bitmap) SetIdx(idx uint32)      { atomic.AddUint64(&b[0], 1<<(idx&l2Bitmask)) }
func (b *l2Bitmap) HasIdx(idx uint32) bool { return atomic.LoadUint64(&b[0])&(1<<(idx&l2Bitmask)) > 0 }

func (b *l2Bitmap) Next()          { b[0] &= b[0] - 1 }
func (b l2Bitmap) Empty() bool     { return b[0] == 0 }
func (b l2Bitmap) Lowest() uint32  { return uint32(bits.TrailingZeros64(b[0])) % l2Size }
func (b l2Bitmap) Highest() uint32 { return uint32(63-bits.LeadingZeros64(b[0])) % l2Size }
