package petname

import (
	"math/bits"

	"github.com/zeebo/errs/v2"

	"github.com/histdb/histdb/hashtbl"
	"github.com/histdb/histdb/num"
	"github.com/histdb/histdb/rwutils"
)

func AppendBTo[V num.T, RWV rwutils.RW[V]](b *B[V], w *rwutils.W) {
	w.Varint(uint64(len(b.buf)))
	w.Bytes(b.buf)
	w.Varint(uint64(len(b.spans)))
	for _, span := range b.spans {
		w.Uint32(span.begin)
		w.Uint32(span.end)
	}
}

func AppendTTo[K hashtbl.Key[K], RWK rwutils.RW[K], V num.T, RWV rwutils.RW[V]](t *T[K, V], w *rwutils.W) {
	hashtbl.AppendTo[K, RWK, V, RWV](&t.idxs, w)
	AppendBTo[V, RWV](&t.buf, w)
}

func ReadBFrom[V num.T, RWV rwutils.RW[V]](b *B[V], r *rwutils.R) {
	b.buf = r.Bytes(int(r.Varint()))
	n := r.Varint()
	if hi, lo := bits.Mul64(n, 8); hi > 0 || lo > uint64(r.Remaining()) {
		r.Invalid(errs.Errorf("petname has too many spans: %d", n))
		b.spans = nil
		return
	}

	b.spans = make([]span, n)
	for i := range b.spans {
		begin := r.Uint32()
		end := r.Uint32()
		b.spans[i] = span{begin: begin, end: end}
	}
}

func ReadTFrom[K hashtbl.Key[K], RWK rwutils.RW[K], V num.T, RWV rwutils.RW[V]](t *T[K, V], r *rwutils.R) {
	hashtbl.ReadFrom[K, RWK, V, RWV](&t.idxs, r)
	ReadBFrom[V, RWV](&t.buf, r)
}
