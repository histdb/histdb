package hashset

import (
	"reflect"
	"unsafe"

	"github.com/histdb/histdb/rwutils"
)

func AppendTo[K Key](t *T[K], w *rwutils.W) {
	w.Varint(uint64(len(t.list)))

	var buf []byte
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&buf))
	hdr.Data = uintptr(unsafe.Pointer(&t.list[0]))
	hdr.Cap = len(t.list) * len(*new(K))
	hdr.Len = len(t.list) * len(*new(K))
	w.Bytes(buf)
}

func ReadFrom[K Key](t *T[K], r *rwutils.R) {
	buf := r.Bytes(int(r.Varint()) * len(*new(K)))
	if len(buf) > 0 {
		t.list = nil
		hdr := (*reflect.SliceHeader)(unsafe.Pointer(&t.list))
		hdr.Data = uintptr(unsafe.Pointer(&buf[0]))
		hdr.Cap = len(buf) / len(*new(K))
		hdr.Len = len(buf) / len(*new(K))
	}
}
