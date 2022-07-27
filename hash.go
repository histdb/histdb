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

func (h Hash) AppendTo(w *rwutils.W)  { w.Bytes(h[:]) }
func (h *Hash) ReadFrom(r *rwutils.R) { copy(h[:], r.Bytes(len(h))) }

func (h *Hash) SetTagHash(th [TagHashSize]byte) {
	copy(h[tagHashStart:tagHashEnd], th[0:TagHashSize])
}

func (h Hash) TagHash() (th [TagHashSize]byte) {
	copy(th[0:TagHashSize], h[tagHashStart:tagHashEnd])
	return th
}

func (h *Hash) TagHashPtr() (th *[TagHashSize]byte) {
	return (*[TagHashSize]byte)(unsafe.Pointer(&h[tagHashStart]))
}

func (h *Hash) SetMetricHash(mh [MetricHashSize]byte) {
	copy(h[metricHashStart:metricHashEnd], mh[0:MetricHashSize])
}

func (h Hash) MetricHash() (mh [MetricHashSize]byte) {
	copy(mh[0:MetricHashSize], h[metricHashStart:metricHashEnd])
	return mh
}

func (h *Hash) MetricHashPtr() (mh *[MetricHashSize]byte) {
	return (*[MetricHashSize]byte)(unsafe.Pointer(&h[metricHashStart]))
}
