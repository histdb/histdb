package bitmap

import (
	"math/bits"
	"sync/atomic"
)

//
// 16 bits
//

type B16 [1]uint64

func (b B16) UnsafeClone() B16         { return b }
func (b B16) UnsafeUint() uint64       { return b[0] }
func (b *B16) UnsafeSetUint(v uint64)  { b[0] = v }
func (b *B16) UnsafeSetIdx(idx uint32) { b[0] += 1 << (idx & 15) }

func (b *B16) Clone() B16           { return B16{atomic.LoadUint64(&b[0])} }
func (b *B16) SetIdx(idx uint)      { atomic.AddUint64(&b[0], 1<<(idx&15)) }
func (b *B16) HasIdx(idx uint) bool { return atomic.LoadUint64(&b[0])&(1<<(idx&15)) > 0 }

func (b *B16) Next() (idx uint32, ok bool) {
	u := b[0]
	c := u & (u - 1)
	idx = uint32(bits.Len64(u ^ c))
	b[0] = c
	return (idx - 1) % 16, u > 0
}

//
// 32 bits
//

type B32 [1]uint64

func (b B32) UnsafeClone() B32         { return b }
func (b B32) UnsafeUint() uint64       { return b[0] }
func (b *B32) UnsafeSetUint(v uint64)  { b[0] = v }
func (b *B32) UnsafeSetIdx(idx uint32) { b[0] += 1 << (idx & 31) }

func (b *B32) Clone() B32             { return B32{atomic.LoadUint64(&b[0])} }
func (b *B32) SetIdx(idx uint32)      { atomic.AddUint64(&b[0], 1<<(idx&31)) }
func (b *B32) HasIdx(idx uint32) bool { return atomic.LoadUint64(&b[0])&(1<<(idx&31)) > 0 }

func (b *B32) Next() (idx uint32, ok bool) {
	u := b[0]
	c := u & (u - 1)
	idx = uint32(bits.Len64(u ^ c))
	b[0] = c
	return (idx - 1) % 32, u > 0
}

//
// 64 bits
//

type B64 [1]uint64

func (b B64) UnsafeClone() B64         { return b }
func (b B64) UnsafeUint() uint64       { return b[0] }
func (b *B64) UnsafeSetUint(v uint64)  { b[0] = v }
func (b *B64) UnsafeSetIdx(idx uint32) { b[0] += 1 << (idx & 63) }

func (b *B64) Clone() B64             { return B64{atomic.LoadUint64(&b[0])} }
func (b *B64) SetIdx(idx uint32)      { atomic.AddUint64(&b[0], 1<<(idx&63)) }
func (b *B64) HasIdx(idx uint32) bool { return atomic.LoadUint64(&b[0])&(1<<(idx&63)) > 0 }

func (b *B64) Next() (idx uint32, ok bool) {
	u := b[0]
	c := u & (u - 1)
	idx = uint32(bits.Len64(u ^ c))
	b[0] = c
	return (idx - 1) % 64, u > 0
}
