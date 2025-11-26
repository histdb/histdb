package histdb

import (
	"fmt"

	"github.com/zeebo/xxh3"

	"github.com/histdb/histdb/rwutils"
)

type Hash [HashSize]byte

func (h Hash) String() string {
	return fmt.Sprintf("(hash %016x %032x)", *h.TagKeyHashPtr(), *h.TagHashPtr())
}

func (h Hash) AppendTo(w *rwutils.W)  { w.Bytes24(h) }
func (h *Hash) ReadFrom(r *rwutils.R) { *h = r.Bytes24() }
func (h Hash) Hash() uint64           { return xxh3.Hash(h[:]) }

func (h *Hash) TagKeyHashPtr() (th *TagKeyHash) {
	return (*TagKeyHash)(h[TagKeyHashStart:TagKeyHashEnd])
}

func (h *Hash) TagHashPtr() (mh *TagHash) {
	return (*TagHash)(h[TagHashStart:TagHashEnd])
}
