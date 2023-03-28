package memindex

import (
	"reflect"
	"unsafe"

	"github.com/histdb/histdb"
	"github.com/histdb/histdb/hashtbl"
	"github.com/histdb/histdb/rwutils"
)

type hashSet struct {
	set  hashtbl.T[histdb.Hash, *histdb.Hash]
	list []histdb.Hash
}

func (hs *hashSet) AppendTo(w *rwutils.W) {
	w.Varint(uint64(len(hs.list)))

	var buf []byte
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&buf))
	hdr.Data = uintptr(unsafe.Pointer(&hs.list[0]))
	hdr.Cap = len(hs.list) * histdb.HashSize
	hdr.Len = len(hs.list) * histdb.HashSize
	w.Bytes(buf)
}

func (hs *hashSet) ReadFrom(r *rwutils.R) {
	buf := r.Bytes(int(r.Varint()) * histdb.HashSize)
	if len(buf) > 0 {
		hs.list = nil
		hdr := (*reflect.SliceHeader)(unsafe.Pointer(&hs.list))
		hdr.Data = uintptr(unsafe.Pointer(&buf[0]))
		hdr.Cap = len(buf) / histdb.HashSize
		hdr.Len = len(buf) / histdb.HashSize
	}
}

func (hs *hashSet) Len() int { return len(hs.list) }

func (hs *hashSet) Size() uint64 {
	return 0 +
		hs.set.Size() +
		24 + histdb.HashSize*uint64(len(hs.list)) +
		0
}

func (hs *hashSet) Fix() {
	hs.set = hashtbl.T[histdb.Hash, *histdb.Hash]{}
}

func (hs *hashSet) Insert(hash histdb.Hash) (uint32, bool) {
	idx, ok := hs.set.Insert(hash, uint32(hs.set.Len()))
	if ok {
		return idx, ok
	}
	hs.list = append(hs.list, hash)
	return idx, false
}

func (hs *hashSet) Hash(idx uint32) (hash histdb.Hash) {
	return hs.list[idx]
}
