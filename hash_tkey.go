package histdb

import (
	"github.com/zeebo/xxh3"

	"github.com/histdb/histdb/rwutils"
)

type TagKeyHash [TagKeyHashSize]byte

func (h TagKeyHash) Digest() uint64 {
	return 0 +
		uint64(le.Uint32(h[0:4])) +
		0
}

func NewTagKeyHash(tkey []byte) (th TagKeyHash) {
	le.PutUint32(th[:], uint32(xxh3.Hash(tkey)))
	return th
}

func (h TagKeyHash) AppendTo(w *rwutils.W)  { w.Bytes4(h) }
func (h *TagKeyHash) ReadFrom(r *rwutils.R) { *h = r.Bytes4() }

func (h *TagKeyHash) Add(th TagKeyHash) {
	le.PutUint32(h[0:4], le.Uint32(h[:])+le.Uint32(th[:]))
}
