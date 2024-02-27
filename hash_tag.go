package histdb

import (
	"github.com/zeebo/xxh3"

	"github.com/histdb/histdb/rwutils"
)

type TagHash [TagHashSize]byte

func NewTagHash(tag []byte) (mh TagHash) {
	s := xxh3.Hash128(tag)
	le.PutUint64(mh[0:8], s.Lo)
	le.PutUint32(mh[8:12], uint32(s.Hi))
	return mh
}

func NewTagHashParts(tkey, value []byte) (mh TagHash) {
	var h xxh3.Hasher
	h.Write(tkey)
	h.WriteString("=")
	h.Write(value)
	s := h.Sum128()
	le.PutUint64(mh[0:8], s.Lo)
	le.PutUint32(mh[8:12], uint32(s.Hi))
	return mh
}

func (h TagHash) Equal(g TagHash) bool { return h == g }

func (h TagHash) Digest() uint64 {
	return 0 +
		le.Uint64(h[0:8]) +
		uint64(le.Uint32(h[8:12])) +
		0
}

func (h TagHash) AppendTo(w *rwutils.W)  { w.Bytes12(h) }
func (h *TagHash) ReadFrom(r *rwutils.R) { *h = r.Bytes12() }

func (h *TagHash) Add(mh TagHash) {
	le.PutUint64(h[0:8], le.Uint64(h[0:8])+le.Uint64(mh[0:8]))
	le.PutUint32(h[8:12], le.Uint32(h[8:12])+le.Uint32(mh[8:12]))
}
