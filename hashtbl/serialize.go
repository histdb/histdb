package hashtbl

import "github.com/histdb/histdb/rwutils"

func AppendTo[K Key, RWK rwutils.RW[K], V any, RWV rwutils.RW[V]](t *T[K, V], w *rwutils.W) {
	w.Uint64(uint64(len(t.slots)))
	w.Uint64(t.mask)
	w.Uint64(t.shift)
	w.Uint64(uint64(t.eles))
	w.Uint64(uint64(t.full))

	for i := range t.slots {
		s := &t.slots[i]
		RWK(&s.k).AppendTo(w)
		RWV(&s.v).AppendTo(w)
	}

	w.Bytes(t.metas)
}

func ReadFrom[K Key, RWK rwutils.RW[K], V any, RWV rwutils.RW[V]](t *T[K, V], r *rwutils.R) {
	n := r.Uint64()
	t.mask = r.Uint64()
	t.shift = r.Uint64()
	t.eles = int(r.Uint64())
	t.full = int(r.Uint64())

	t.slots = make([]slot[K, V], n)
	for i := range t.slots {
		s := &t.slots[i]
		RWK(&s.k).ReadFrom(r)
		RWV(&s.v).ReadFrom(r)
	}

	t.metas = r.Bytes(int(n))
}
