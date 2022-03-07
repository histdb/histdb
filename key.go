package histdb

import (
	"encoding/binary"
	"fmt"
	"unsafe"
)

type Key [KeySize]byte

type Hash [HashSize]byte

const (
	TagHashSize    = 8
	MetricHashSize = 8
	HashSize       = TagHashSize + MetricHashSize
	TimestampSize  = 4

	KeySize = HashSize + TimestampSize

	tagHashStart = 0
	tagHashEnd   = tagHashStart + TagHashSize

	metricHashStart = tagHashEnd
	metricHashEnd   = metricHashStart + MetricHashSize

	hashStart = tagHashStart
	hashEnd   = metricHashEnd

	timestampStart = hashEnd
	timestampEnd   = timestampStart + TimestampSize
)

func (k Key) Zero() bool { return k == Key{} }

func (k Key) String() string {
	return fmt.Sprintf("(key %x %x %08x)", k.TagHash(), k.MetricHash(), k.Timestamp())
}

func (k Key) Hash() (h [HashSize]byte) {
	copy(h[0:HashSize], k[hashStart:hashEnd])
	return h
}

func (k *Key) HashPtr() *[HashSize]byte {
	return (*[HashSize]byte)(unsafe.Pointer(&k[hashStart]))
}

func (k *Key) SetTagHash(th [TagHashSize]byte) {
	copy(k[tagHashStart:tagHashEnd], th[0:TagHashSize])
}

func (k Key) TagHash() (th [TagHashSize]byte) {
	copy(th[0:TagHashSize], k[tagHashStart:tagHashEnd])
	return th
}

func (k *Key) TagHashPtr() (th *[TagHashSize]byte) {
	return (*[TagHashSize]byte)(unsafe.Pointer(&k[tagHashStart]))
}

func (k *Key) SetMetricHash(mh [MetricHashSize]byte) {
	copy(k[metricHashStart:metricHashEnd], mh[0:MetricHashSize])
}

func (k Key) MetricHash() (mh [MetricHashSize]byte) {
	copy(mh[0:MetricHashSize], k[metricHashStart:metricHashEnd])
	return mh
}

func (k *Key) MetricHashPtr() (mh *[MetricHashSize]byte) {
	return (*[MetricHashSize]byte)(unsafe.Pointer(&k[metricHashStart]))
}

func (k *Key) SetTimestamp(ts uint32) {
	binary.BigEndian.PutUint32(k[timestampStart:timestampEnd], ts)
}

func (k Key) Timestamp() uint32 {
	return binary.BigEndian.Uint32(k[timestampStart:timestampEnd])
}

func (k *Key) TimestampPtr() (ts *[TimestampSize]byte) {
	return (*[TimestampSize]byte)(unsafe.Pointer(&k[timestampStart]))
}
