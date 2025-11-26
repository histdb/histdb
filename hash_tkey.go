package histdb

import (
	"github.com/zeebo/xxh3"

	"github.com/histdb/histdb/rwutils"
)

type TagKeyHash [TagKeyHashSize]byte

func NewTagKeyHash(tkey []byte) (th TagKeyHash) {
	le.PutUint64(th[:], uint64(xxh3.Hash(tkey)))
	return th
}

func (h TagKeyHash) AppendTo(w *rwutils.W)  { w.Bytes8(h) }
func (h *TagKeyHash) ReadFrom(r *rwutils.R) { *h = r.Bytes8() }
func (h TagKeyHash) Hash() uint64           { return xxh3.Hash(h[:]) }

func (h *TagKeyHash) Add(th TagKeyHash) {
	le.PutUint64(h[0:8], le.Uint64(h[:])+le.Uint64(th[:]))
}
