package histdb

import (
	"github.com/zeebo/xxh3"

	"github.com/histdb/histdb/rwutils"
)

type TagHash [TagHashSize]byte

func NewTagHash(tag []byte) (mh TagHash) {
	s := xxh3.Hash128(tag)
	le.PutUint64(mh[0:8], s.Lo)
	le.PutUint64(mh[8:16], s.Hi)
	return mh
}

func (h TagHash) Equal(g TagHash) bool { return h == g }

func (h TagHash) Digest() uint64 {
	return 0 +
		le.Uint64(h[0:8]) +
		le.Uint64(h[8:16]) +
		0
}

func (h TagHash) AppendTo(w *rwutils.W)  { w.Bytes16(h) }
func (h *TagHash) ReadFrom(r *rwutils.R) { *h = r.Bytes16() }

func (h *TagHash) Add(mh TagHash) {
	le.PutUint64(h[0:8], le.Uint64(h[0:8])+le.Uint64(mh[0:8]))
	le.PutUint64(h[8:16], le.Uint64(h[8:16])+le.Uint64(mh[8:16]))
}
