package petname

import "github.com/histdb/histdb/rwutils"

func (t *T[K, RWK]) AppendTo(w *rwutils.W) {
	w.Varint(uint64(len(t.buf)))
	w.Bytes(t.buf)
	t.idxs.AppendTo(w)
	w.Varint(uint64(len(t.spans)))
	for _, span := range t.spans {
		w.Uint32(span.begin)
		w.Uint32(span.end)
	}
}

func (t *T[K, KRW]) ReadFrom(r *rwutils.R) {
	t.buf = r.Bytes(int(r.Varint()))
	t.idxs.ReadFrom(r)
	t.spans = make([]span, r.Varint())
	for i := range t.spans {
		begin := r.Uint32()
		end := r.Uint32()
		t.spans[i] = span{begin: begin, end: end}
	}
}
