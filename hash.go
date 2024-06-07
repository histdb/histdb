package histdb

import (
	"fmt"
	"unsafe"

	"github.com/histdb/histdb/rwutils"
)

type Hash [HashSize]byte

func (h Hash) Equal(g Hash) bool { return h == g }

func (h Hash) Digest() uint64 {
	return 0 +
		le.Uint64(h[0:8]) +
		le.Uint64(h[8:16]) +
		uint64(le.Uint16(h[16:18])) +
		0
}

func (h Hash) String() string {
	return fmt.Sprintf("(hash %016x %020x)", *h.TagKeyHashPtr(), *h.TagHashPtr())
}

func (h Hash) AppendTo(w *rwutils.W)  { w.Bytes18(h) }
func (h *Hash) ReadFrom(r *rwutils.R) { *h = r.Bytes18() }

func (h *Hash) TagKeyHashPtr() (th *TagKeyHash) {
	return (*TagKeyHash)(unsafe.Pointer(&h[TagKeyHashStart]))
}

func (h *Hash) TagHashPtr() (mh *TagHash) {
	return (*TagHash)(unsafe.Pointer(&h[TagHashStart]))
}
