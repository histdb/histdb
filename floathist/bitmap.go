package floathist

import (
	"fmt"
	"math/bits"
	"sync/atomic"
)

type l0Bitmap struct{ b uint64 }

func newL0Bitmap(v uint64) l0Bitmap { return l0Bitmap{v & l0SM} }

func (b *l0Bitmap) unsafeSetIdx(idx uint32) { b.b += 1 << (idx & l0BM) }

func (b *l0Bitmap) AtomicClone() l0Bitmap     { return l0Bitmap{atomic.LoadUint64(&b.b)} }
func (b *l0Bitmap) AtomicSetIdx(idx uint32)   { atomic.AddUint64(&b.b, 1<<(idx&l0BM)) }
func (b *l0Bitmap) AtomicHas(idx uint32) bool { return atomic.LoadUint64(&b.b)&(1<<(idx&l0BM)) > 0 }

func (b *l0Bitmap) ClearLowest() { b.b &= b.b - 1 }

func (b l0Bitmap) Uint64() uint64  { return b.b }
func (b l0Bitmap) Empty() bool     { return b.b == 0 }
func (b l0Bitmap) Lowest() uint32  { return uint32(bits.TrailingZeros64(b.b)) % l0S }
func (b l0Bitmap) Highest() uint32 { return uint32(63-bits.LeadingZeros64(b.b)) % l0S }
func (b l0Bitmap) String() string  { return fmt.Sprintf("%064b", b.b) }

type l1Bitmap struct{ b uint64 }

func newL1Bitmap(v uint64) l1Bitmap { return l1Bitmap{v & l1SM} }

func (b *l1Bitmap) unsafeSetIdx(idx uint32) { b.b += 1 << (idx & l1BM) }

func (b *l1Bitmap) AtomicClone() l1Bitmap        { return l1Bitmap{atomic.LoadUint64(&b.b)} }
func (b *l1Bitmap) AtomicSetIdx(idx uint32)      { atomic.AddUint64(&b.b, 1<<(idx&l1BM)) }
func (b *l1Bitmap) AtomicHasIdx(idx uint32) bool { return atomic.LoadUint64(&b.b)&(1<<(idx&l1BM)) > 0 }

func (b *l1Bitmap) ClearLowest() { b.b &= b.b - 1 }

func (b l1Bitmap) Uint64() uint64  { return b.b }
func (b l1Bitmap) Empty() bool     { return b.b == 0 }
func (b l1Bitmap) Lowest() uint32  { return uint32(bits.TrailingZeros64(b.b)) % l1S }
func (b l1Bitmap) Highest() uint32 { return uint32(63-bits.LeadingZeros64(b.b)) % l1S }
func (b l1Bitmap) String() string  { return fmt.Sprintf("%064b", b.b) }

type l2Bitmap struct{ b uint64 }

func newL2Bitmap(v uint64) l2Bitmap { return l2Bitmap{v & l2SM} }

func (b *l2Bitmap) unsafeSetIdx(idx uint32) { b.b += 1 << (idx & l2BM) }

func (b *l2Bitmap) AtomicClone() l2Bitmap        { return l2Bitmap{atomic.LoadUint64(&b.b)} }
func (b *l2Bitmap) AtomicSetIdx(idx uint32)      { atomic.AddUint64(&b.b, 1<<(idx&l2BM)) }
func (b *l2Bitmap) AtomicHasIdx(idx uint32) bool { return atomic.LoadUint64(&b.b)&(1<<(idx&l2BM)) > 0 }

func (b *l2Bitmap) ClearLowest() { b.b &= b.b - 1 }

func (b l2Bitmap) Uint64() uint64  { return b.b }
func (b l2Bitmap) Empty() bool     { return b.b == 0 }
func (b l2Bitmap) Lowest() uint32  { return uint32(bits.TrailingZeros64(b.b)) % l2S }
func (b l2Bitmap) Highest() uint32 { return uint32(63-bits.LeadingZeros64(b.b)) % l2S }
func (b l2Bitmap) String() string  { return fmt.Sprintf("%064b", b.b) }
