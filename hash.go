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
		uint64(le.Uint32(h[16:20])) +
		0
}

func (h Hash) AppendTo(w *rwutils.W)  { w.Bytes20(h) }
func (h *Hash) ReadFrom(r *rwutils.R) { *h = r.Bytes20() }

func (h *Hash) SetTagKeyHash(th TagKeyHash) {
	copy(h[TagHashStart:TagHashEnd], th[0:TagKeyHashSize])
}

func (h Hash) TagKeyHash() (th TagKeyHash) {
	copy(th[0:TagKeyHashSize], h[TagHashStart:TagHashEnd])
	return th
}

func (h *Hash) TagKeyHashPtr() (th *TagKeyHash) {
	return (*TagKeyHash)(unsafe.Pointer(&h[TagHashStart]))
}

func (h *Hash) SetTagHash(mh TagHash) {
	copy(h[MetricHashStart:MetricHashEnd], mh[0:TagHashSize])
}

func (h Hash) TagHash() (mh TagHash) {
	copy(mh[0:TagHashSize], h[MetricHashStart:MetricHashEnd])
	return mh
}

func (h *Hash) TagHashPtr() (mh *TagHash) {
	return (*TagHash)(unsafe.Pointer(&h[MetricHashStart]))
}
