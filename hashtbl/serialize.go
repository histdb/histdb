package hashtbl

import (
	"github.com/zeebo/errs/v2"

	"github.com/histdb/histdb/rwutils"
)

type RW[K Key[K], RWK rwutils.RW[K], V any, RWV rwutils.RW[V]] T[K, V]

func (rw *RW[K, RWK, V, RWV]) AppendTo(w *rwutils.W) { AppendTo[K, RWK, V, RWV]((*T[K, V])(rw), w) }
func (rw *RW[K, RWK, V, RWV]) ReadFrom(r *rwutils.R) { ReadFrom[K, RWK, V, RWV]((*T[K, V])(rw), r) }

func AppendTo[K Key[K], KRW rwutils.RW[K], V any, VRW rwutils.RW[V]](t *T[K, V], w *rwutils.W) {
	w.Int(len(t.slots))
	w.Uint64(t.mask)
	w.Uint64(t.shift)
	w.Int(t.eles)
	w.Int(t.full)

	for i := range t.slots {
		s := &t.slots[i]
		KRW(&s.k).AppendTo(w)
		VRW(&s.v).AppendTo(w)
	}

	w.Bytes(t.metas)
}

func ReadFrom[K Key[K], KRW rwutils.RW[K], V any, VRW rwutils.RW[V]](t *T[K, V], r *rwutils.R) {
	n := r.Int()
	t.mask = r.Uint64()
	t.shift = r.Uint64()
	t.eles = r.Int()
	t.full = r.Int()

	if uint64(n) > uint64(r.Remaining()) {
		r.Invalid(errs.Errorf("hash table has too many slots: %d", n))
		t.slots = nil
		t.metas = nil
		return
	}

	if n == 0 {
		t.slots = nil
		t.metas = nil
		return
	}

	t.slots = make([]slot[K, V], n)
	for i := range t.slots {
		s := &t.slots[i]
		(KRW)(&s.k).ReadFrom(r)
		(VRW)(&s.v).ReadFrom(r)
	}
	t.metas = r.Bytes(int(n))
}
