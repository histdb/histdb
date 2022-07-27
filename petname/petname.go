package petname

import (
	"unsafe"

	"github.com/histdb/histdb/hashtbl"
)

type span struct {
	begin uint32
	end   uint32
}

type T struct {
	buf   []byte
	idxs  hashtbl.T[hashtbl.U64, *hashtbl.U64]
	spans []span
}

func (t *T) Len() int {
	if t == nil {
		return 0
	}
	return t.idxs.Len()
}

func (t *T) Size() uint64 {
	return 0 +
		/* buf   */ 24 + 1*uint64(len(t.buf)) +
		/* idxs  */ t.idxs.Size() +
		/* spans */ 24 + uint64(unsafe.Sizeof(span{}))*uint64(len(t.spans)) +
		0
}

func (t *T) Put(h uint64, v string) uint32 {
	n, ok := t.idxs.Insert(hashtbl.U64(h), uint32(t.idxs.Len()))
	if !ok && len(v) > 0 {
		t.spans = append(t.spans, span{
			begin: uint32(len(t.buf)),
			end:   uint32(len(t.buf) + len(v)),
		})
		t.buf = append(t.buf, v...)
	}
	return n
}

func (t *T) Find(h uint64) (uint32, bool) {
	return t.idxs.Find(hashtbl.U64(h))
}

func (t *T) Get(n uint32) string {
	if uint64(n) < uint64(len(t.spans)) {
		s := t.spans[n]
		b, e := uint64(s.begin), uint64(s.end)
		if b < uint64(len(t.buf)) && e <= uint64(len(t.buf)) && b <= e {
			v := t.buf[b:e]
			return *(*string)(unsafe.Pointer(&v))
		}
	}
	return ""
}
