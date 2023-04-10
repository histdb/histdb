package bitmap

import (
	"math/bits"
	"sync/atomic"
)

type T struct{ b uint64 }

func (t *T) AtomicClone() T          { return T{atomic.LoadUint64(&t.b)} }
func (t *T) AtomicSetIdx(idx uint)   { atomic.AddUint64(&t.b, 1<<(idx%64)) }
func (t *T) AtomicHas(idx uint) bool { return atomic.LoadUint64(&t.b)&(1<<(idx%64)) != 0 }

func (t *T) Next() { t.b &= t.b - 1 }

func (t T) Empty() bool   { return t.b == 0 }
func (t T) Lowest() uint  { return uint(bits.TrailingZeros64(t.b)) % 64 }
func (t T) Highest() uint { return uint(63-bits.LeadingZeros64(t.b)) % 64 }
