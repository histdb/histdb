package histdb

import (
	"encoding/binary"
	"fmt"
	"unsafe"

	"github.com/histdb/histdb/rwutils"
)

type Key [KeySize]byte

func (k Key) Digest() uint64 {
	return 0 +
		le.Uint64(k[0:8]) +
		le.Uint64(k[8:16]) +
		uint64(le.Uint32(k[16:20])) +
		0
}

func (k Key) AppendTo(w *rwutils.W)  { w.Bytes(k[:]) }
func (k *Key) ReadFrom(r *rwutils.R) { copy(k[:], r.Bytes(len(k))) }

func (k Key) Zero() bool { return k == Key{} }

func (k Key) String() string {
	return fmt.Sprintf("(key %08x %024x %08x)", k.TagKeyHash(), k.TagHash(), k.Timestamp())
}

func (k Key) Hash() (h Hash) {
	copy(h[0:HashSize], k[hashStart:hashEnd])
	return h
}

func (k *Key) HashPtr() *Hash {
	return (*Hash)(unsafe.Pointer(&k[hashStart]))
}

func (k *Key) SetTagKeyHash(th TagKeyHash) {
	copy(k[tagHashStart:tagHashEnd], th[0:TagKeyHashSize])
}

func (k Key) TagKeyHash() (th TagKeyHash) {
	copy(th[0:TagKeyHashSize], k[tagHashStart:tagHashEnd])
	return th
}

func (k *Key) TagKeyHashPtr() (th *TagKeyHash) {
	return (*TagKeyHash)(unsafe.Pointer(&k[tagHashStart]))
}

func (k *Key) SetTagHash(mh TagHash) {
	copy(k[metricHashStart:metricHashEnd], mh[0:TagHashSize])
}

func (k Key) TagHash() (mh TagHash) {
	copy(mh[0:TagHashSize], k[metricHashStart:metricHashEnd])
	return mh
}

func (k *Key) TagHashPtr() (mh *TagHash) {
	return (*TagHash)(unsafe.Pointer(&k[metricHashStart]))
}

func (k *Key) SetTimestamp(ts uint32) {
	binary.BigEndian.PutUint32(k[timestampStart:timestampEnd], ts)
}

func (k Key) Timestamp() uint32 {
	return binary.BigEndian.Uint32(k[timestampStart:timestampEnd])
}

func (k *Key) TimestampPtr() (ts *uint32) {
	return (*uint32)(unsafe.Pointer(&k[timestampStart]))
}
