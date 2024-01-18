package hashset

import (
	"reflect"
	"unsafe"

	"github.com/histdb/histdb/num"
	"github.com/histdb/histdb/rwutils"
)

type RW[K Key, V num.T] T[K, V]

func (rw *RW[K, V]) AppendTo(w *rwutils.W) { AppendTo((*T[K, V])(rw), w) }
func (rw *RW[K, V]) ReadFrom(r *rwutils.R) { ReadFrom((*T[K, V])(rw), r) }

func AppendTo[K Key, V num.T](t *T[K, V], w *rwutils.W) {
	w.Varint(uint64(len(t.list)))

	var buf []byte
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&buf))
	hdr.Data = uintptr(unsafe.Pointer(&t.list[0]))
	hdr.Cap = len(t.list) * len(*new(K))
	hdr.Len = len(t.list) * len(*new(K))
	w.Bytes(buf)
}

func ReadFrom[K Key, V num.T](t *T[K, V], r *rwutils.R) {
	buf := r.Bytes(int(r.Varint()) * len(*new(K)))
	if len(buf) > 0 {
		t.list = nil
		hdr := (*reflect.SliceHeader)(unsafe.Pointer(&t.list))
		hdr.Data = uintptr(unsafe.Pointer(&buf[0]))
		hdr.Cap = len(buf) / len(*new(K))
		hdr.Len = len(buf) / len(*new(K))
	}
}
