package histdb

import (
	"github.com/zeebo/xxh3"

	"github.com/histdb/histdb/rwutils"
)

type TagHash [TagHashSize]byte

func NewTagHash(tag []byte) (mh TagHash) {
	s := xxh3.Hash128(tag)
	le.PutUint64(mh[0:8], s.Lo)
	le.PutUint16(mh[8:10], uint16(s.Hi))
	return mh
}

func (h TagHash) Equal(g TagHash) bool { return h == g }

func (h TagHash) Digest() uint64 {
	return 0 +
		le.Uint64(h[0:8]) +
		uint64(le.Uint16(h[8:10])) +
		0
}

func (h TagHash) AppendTo(w *rwutils.W)  { w.Bytes10(h) }
func (h *TagHash) ReadFrom(r *rwutils.R) { *h = r.Bytes10() }

func (h *TagHash) Add(mh TagHash) {
	le.PutUint64(h[0:8], le.Uint64(h[0:8])+le.Uint64(mh[0:8]))
	le.PutUint16(h[8:10], le.Uint16(h[8:10])+le.Uint16(mh[8:10]))
}
