package memindex

import (
	"github.com/zeebo/errs/v2"

	"github.com/histdb/histdb/rwutils"
)

type tkeySet []Id

func (t tkeySet) Digest() (d uint64) {
	// fnv
	d = 14695981039346656037
	for _, id := range t {
		d *= 1099511628211
		d ^= uint64(id)
	}
	return d
}

func (t tkeySet) Equal(s tkeySet) bool {
	if len(t) != len(s) {
		return false
	}
	for i := range t {
		if t[i] != s[i] {
			return false
		}
	}
	return true
}

func (t tkeySet) AppendTo(w *rwutils.W) {
	w.Varint(uint64(len(t)))
	for _, id := range t {
		w.Varint(uint64(id))
	}
}

func (t *tkeySet) ReadFrom(r *rwutils.R) {
	n := r.Varint()

	if n > uint64(r.Remaining()) {
		r.Invalid(errs.Errorf("tkeySet has too many elements: %d", n))
		*t = nil
		return
	}

	if n == 0 {
		*t = nil
		return
	}

	ts := make(tkeySet, n)
	for i := range ts {
		ts[i] = Id(r.Varint())
	}
	*t = ts
}
