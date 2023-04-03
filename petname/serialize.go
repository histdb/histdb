package petname

import (
	"github.com/histdb/histdb/hashtbl"
	"github.com/histdb/histdb/rwutils"
)

type RW[K hashtbl.Key, RWK rwutils.RW[K]] T[K]

func (rw *RW[K, RWK]) AppendTo(w *rwutils.W) { AppendTo[K, RWK]((*T[K])(rw), w) }
func (rw *RW[K, RWK]) ReadFrom(r *rwutils.R) { ReadFrom[K, RWK]((*T[K])(rw), r) }

func AppendTo[K hashtbl.Key, RWK rwutils.RW[K]](t *T[K], w *rwutils.W) {
	w.Varint(uint64(len(t.buf)))
	w.Bytes(t.buf)
	hashtbl.AppendTo[K, RWK](&t.idxs, w)
	w.Varint(uint64(len(t.spans)))
	for _, span := range t.spans {
		w.Uint32(span.begin)
		w.Uint32(span.end)
	}
}

func ReadFrom[K hashtbl.Key, RWK rwutils.RW[K]](t *T[K], r *rwutils.R) {
	t.buf = r.Bytes(int(r.Varint()))
	hashtbl.ReadFrom[K, RWK](&t.idxs, r)
	t.spans = make([]span, r.Varint())
	for i := range t.spans {
		begin := r.Uint32()
		end := r.Uint32()
		t.spans[i] = span{begin: begin, end: end}
	}
}
