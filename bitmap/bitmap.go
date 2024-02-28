package bitmap

import (
	"fmt"
	"math/bits"
	"sync/atomic"
)

type T64 struct{ b uint64 }

func New64(v uint64) T64 { return T64{v} }

func (b *T64) AtomicClone() T64        { return T64{atomic.LoadUint64(&b.b)} }
func (b *T64) AtomicAddIdx(idx uint)   { atomic.AddUint64(&b.b, 1<<(idx&63)) }
func (b *T64) AtomicHas(idx uint) bool { return atomic.LoadUint64(&b.b)&(1<<(idx&63)) > 0 }
func (b *T64) ClearLowest()            { b.b &= b.b - 1 }
func (b *T64) AddIdx(idx uint32)       { b.b += 1 << (idx & 63) }
func (b T64) Uint64() uint64           { return b.b }
func (b T64) Empty() bool              { return b.b == 0 }
func (b T64) Lowest() uint             { return uint(bits.TrailingZeros64(b.b)) % 64 }
func (b T64) Highest() uint            { return uint(63-bits.LeadingZeros64(b.b)) % 64 }
func (b T64) String() string           { return fmt.Sprintf("%064b", b.b) }

type T32 struct{ b uint32 }

func New32(v uint32) T32 { return T32{v} }

func (b *T32) AtomicClone() T32        { return T32{atomic.LoadUint32(&b.b)} }
func (b *T32) AtomicAddIdx(idx uint)   { atomic.AddUint32(&b.b, 1<<(idx&31)) }
func (b *T32) AtomicHas(idx uint) bool { return atomic.LoadUint32(&b.b)&(1<<(idx&31)) > 0 }
func (b *T32) ClearLowest()            { b.b &= b.b - 1 }
func (b *T32) AddIdx(idx uint32)       { b.b += 1 << (idx & 31) }
func (b T32) Uint32() uint32           { return b.b }
func (b T32) Empty() bool              { return b.b == 0 }
func (b T32) Lowest() uint             { return uint(bits.TrailingZeros32(b.b)) % 32 }
func (b T32) Highest() uint            { return uint(31-bits.LeadingZeros32(b.b)) % 32 }
func (b T32) String() string           { return fmt.Sprintf("%032b", b.b) }
