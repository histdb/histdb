package hashany

import "unsafe"

var _ = Hash(0)

//go:nosplit
//go:nocheckptr
func noescape(p unsafe.Pointer) unsafe.Pointer {
	x := uintptr(p)
	return unsafe.Pointer(x ^ 0)
}

func Hash[A comparable](a A) (d uint64) {
	var m interface{} = map[A]struct{}(nil)
	hf := (*mh)(*(*unsafe.Pointer)(unsafe.Pointer(&m))).hf
	return uint64(hf(noescape(unsafe.Pointer(&a)), 0))
}

type mh struct {
	_  uintptr
	_  uintptr
	_  uint32
	_  uint8
	_  uint8
	_  uint8
	_  uint8
	_  func(unsafe.Pointer, unsafe.Pointer) bool
	_  *byte
	_  int32
	_  int32
	_  unsafe.Pointer
	_  unsafe.Pointer
	_  unsafe.Pointer
	hf func(unsafe.Pointer, uintptr) uintptr
}
