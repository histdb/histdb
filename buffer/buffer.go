package buffer

import (
	"reflect"
	"unsafe"
)

type (
	ptr  = unsafe.Pointer
	uptr = uintptr
)

//
// custom slice support :sonic:
//

type T struct {
	base ptr
	pos  uptr
	cap  uptr
}

func OfCap(n []byte) T {
	return T{
		base: *(*ptr)(ptr(&n)),
		pos:  0,
		cap:  uptr(cap(n)),
	}
}

func OfLen(n []byte) T {
	return T{
		base: *(*ptr)(ptr(&n)),
		pos:  0,
		cap:  uptr(len(n)),
	}
}

func (buf T) Trim() T {
	buf.cap = buf.pos
	return buf
}

func (buf T) Valid() bool {
	return buf.pos < buf.cap
}

func (buf T) Base() ptr {
	return buf.base
}

func (buf T) Pos() uptr {
	return buf.pos
}

func (buf T) Cap() uptr {
	return buf.cap
}

func (buf T) SetPos(pos uintptr) T {
	buf.pos = pos
	return buf
}

func (buf T) Prefix() []byte {
	return *(*[]byte)(unsafe.Pointer(&buf))
}

func (buf T) At(n uptr) ptr {
	return ptr(uptr(buf.base) + buf.pos + n)
}

func (buf T) Reset() T {
	buf.pos = 0
	return buf
}

func (buf T) Front() *byte {
	return (*byte)(ptr(uptr(buf.base) + buf.pos))
}

func (buf T) Front2() *[2]byte {
	return (*[2]byte)(ptr(uptr(buf.base) + buf.pos))
}

func (buf T) Front4() *[4]byte {
	return (*[4]byte)(ptr(uptr(buf.base) + buf.pos))
}

func (buf T) Front8() *[8]byte {
	return (*[8]byte)(ptr(uptr(buf.base) + buf.pos))
}

func (buf T) Front9() *[9]byte {
	return (*[9]byte)(ptr(uptr(buf.base) + buf.pos))
}

func (buf T) Front12() *[12]byte {
	return (*[12]byte)(ptr(uptr(buf.base) + buf.pos))
}

func (buf T) Front16() *[16]byte {
	return (*[16]byte)(ptr(uptr(buf.base) + buf.pos))
}

func (buf T) Front20() *[20]byte {
	return (*[20]byte)(ptr(uptr(buf.base) + buf.pos))
}

func (buf T) FrontN(n int) (x []byte) {
	xh := (*reflect.SliceHeader)(ptr(&x))
	xh.Data = uptr(buf.At(0))
	xh.Cap = n
	xh.Len = n
	return
}

func (buf T) Suffix() (x []byte) {
	xh := (*reflect.SliceHeader)(ptr(&x))
	xh.Data = uptr(buf.At(0))
	xh.Cap = int(buf.Remaining())
	xh.Len = int(buf.Remaining())
	return
}

func (buf T) Remaining() uptr {
	return buf.cap - buf.pos
}

//go:noinline
func (buf T) grow(n uintptr) T {
	buf.cap = buf.cap*2 + n
	nb := make([]byte, buf.cap)
	copy(nb, buf.Prefix())
	buf.base = *(*ptr)(ptr(&nb))
	return buf
}

func (buf T) Grow9() T {
	return buf.Grow(9)
}

func (buf T) Grow(n uintptr) T {
	if buf.cap-buf.pos < n {
		return buf.grow(n)
	}
	return buf
}

func (buf T) Index(n uintptr) *byte {
	return (*byte)(ptr(uptr(buf.base) + n))
}

func (buf T) Index2(n uintptr) *[2]byte {
	return (*[2]byte)(ptr(uptr(buf.base) + n))
}

func (buf T) Index4(n uintptr) *[4]byte {
	return (*[4]byte)(ptr(uptr(buf.base) + n))
}

func (buf T) Index8(n uintptr) *[8]byte {
	return (*[8]byte)(ptr(uptr(buf.base) + n))
}

func (buf T) Index9(n uintptr) *[9]byte {
	return (*[9]byte)(ptr(uptr(buf.base) + n))
}

func (buf T) Advance(n uptr) T {
	buf.pos += n
	return buf
}

func (buf T) Retreat(n uptr) T {
	buf.pos -= n
	return buf
}
