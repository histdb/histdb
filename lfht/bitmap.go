package lfht

import (
	"math/bits"
	"sync/atomic"
)

type bmap struct{ b uint64 }

func (t *bmap) AtomicClone() bmap       { return bmap{atomic.LoadUint64(&t.b)} }
func (t *bmap) AtomicSetIdx(idx uint)   { atomic.AddUint64(&t.b, 1<<(idx%64)) }
func (t *bmap) AtomicHas(idx uint) bool { return atomic.LoadUint64(&t.b)&(1<<(idx%64)) != 0 }

func (t *bmap) ClearLowest() { t.b &= t.b - 1 }

func (t bmap) Empty() bool   { return t.b == 0 }
func (t bmap) Lowest() uint  { return uint(bits.TrailingZeros64(t.b)) % 64 }
func (t bmap) Highest() uint { return uint(63-bits.LeadingZeros64(t.b)) % 64 }
