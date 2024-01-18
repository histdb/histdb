package petname

import (
	"math/bits"

	"github.com/zeebo/errs/v2"

	"github.com/histdb/histdb/hashtbl"
	"github.com/histdb/histdb/num"
	"github.com/histdb/histdb/rwutils"
)

type RW[K hashtbl.Key, RWK rwutils.RW[K], V num.T, RWV rwutils.RW[V]] T[K, V]

func (rw *RW[K, RWK, V, RWV]) AppendTo(w *rwutils.W) { AppendTo[K, RWK, V, RWV]((*T[K, V])(rw), w) }
func (rw *RW[K, RWK, V, RWV]) ReadFrom(r *rwutils.R) { ReadFrom[K, RWK, V, RWV]((*T[K, V])(rw), r) }

func AppendTo[K hashtbl.Key, RWK rwutils.RW[K], V num.T, RWV rwutils.RW[V]](t *T[K, V], w *rwutils.W) {
	w.Varint(uint64(len(t.buf)))
	w.Bytes(t.buf)
	hashtbl.AppendTo[K, RWK, V, RWV](&t.idxs, w)
	w.Varint(uint64(len(t.spans)))
	for _, span := range t.spans {
		w.Uint32(span.begin)
		w.Uint32(span.end)
	}
}

func ReadFrom[K hashtbl.Key, RWK rwutils.RW[K], V num.T, RWV rwutils.RW[V]](t *T[K, V], r *rwutils.R) {
	t.buf = r.Bytes(int(r.Varint()))
	hashtbl.ReadFrom[K, RWK, V, RWV](&t.idxs, r)

	n := r.Varint()
	if hi, lo := bits.Mul64(n, 8); hi > 0 || lo > uint64(r.Remaining()) {
		r.Invalid(errs.Errorf("petname has too many spans: %d", n))
		t.spans = nil
		return
	}

	t.spans = make([]span, n)
	for i := range t.spans {
		begin := r.Uint32()
		end := r.Uint32()
		t.spans[i] = span{begin: begin, end: end}
	}
}
