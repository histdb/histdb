package histdb

import (
	"unsafe"

	"github.com/histdb/histdb/rwutils"
)

type Hash [HashSize]byte

func (h Hash) Digest() uint64 {
	return 0 +
		le.Uint64(h[0:8]) +
		le.Uint64(h[8:16]) +
		0
}

func (h Hash) AppendTo(w *rwutils.W)  { w.Bytes16(h) }
func (h *Hash) ReadFrom(r *rwutils.R) { *h = r.Bytes16() }

func (h *Hash) SetTagKeyHash(th TagKeyHash) {
	copy(h[tagHashStart:tagHashEnd], th[0:TagKeyHashSize])
}

func (h Hash) TagKeyHash() (th TagKeyHash) {
	copy(th[0:TagKeyHashSize], h[tagHashStart:tagHashEnd])
	return th
}

func (h *Hash) TagKeyHashPtr() (th *TagKeyHash) {
	return (*TagKeyHash)(unsafe.Pointer(&h[tagHashStart]))
}

func (h *Hash) SetTagHash(mh TagHash) {
	copy(h[metricHashStart:metricHashEnd], mh[0:TagHashSize])
}

func (h Hash) TagHash() (mh TagHash) {
	copy(mh[0:TagHashSize], h[metricHashStart:metricHashEnd])
	return mh
}

func (h *Hash) TagHashPtr() (mh *TagHash) {
	return (*TagHash)(unsafe.Pointer(&h[metricHashStart]))
}
