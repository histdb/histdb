package histdb

import (
	"fmt"

	"github.com/histdb/histdb/rwutils"
)

type Hash [HashSize]byte

func (h Hash) Equal(g Hash) bool { return h == g }

func (h Hash) Digest() uint64 {
	return 0 +
		le.Uint64(h[0:8]) +
		le.Uint64(h[8:16]) +
		le.Uint64(h[16:24]) +
		0
}

func (h Hash) String() string {
	return fmt.Sprintf("(hash %016x %032x)", *h.TagKeyHashPtr(), *h.TagHashPtr())
}

func (h Hash) AppendTo(w *rwutils.W)  { w.Bytes24(h) }
func (h *Hash) ReadFrom(r *rwutils.R) { *h = r.Bytes24() }

func (h *Hash) TagKeyHashPtr() (th *TagKeyHash) {
	return (*TagKeyHash)(h[TagKeyHashStart:TagKeyHashEnd])
}

func (h *Hash) TagHashPtr() (mh *TagHash) {
	return (*TagHash)(h[TagHashStart:TagHashEnd])
}
