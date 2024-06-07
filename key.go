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
		le.Uint64(k[16:24]) +
		0
}

func (k Key) AppendTo(w *rwutils.W)  { w.Bytes(k[:]) }
func (k *Key) ReadFrom(r *rwutils.R) { copy(k[:], r.Bytes(len(k))) }

func (k Key) Zero() bool { return k == Key{} }

func (k Key) String() string {
	return fmt.Sprintf("(key %s %08x)", k.Hash(), k.Timestamp())
}

func (k Key) Hash() (h Hash) {
	copy(h[0:HashSize], k[HashStart:HashEnd])
	return h
}

func (k *Key) HashPtr() *Hash {
	return (*Hash)(unsafe.Pointer(&k[HashStart]))
}

func (k *Key) TagKeyHashPtr() (th *TagKeyHash) {
	return (*TagKeyHash)(unsafe.Pointer(&k[TagKeyHashStart]))
}

func (k *Key) TagHashPtr() (mh *TagHash) {
	return (*TagHash)(unsafe.Pointer(&k[TagHashStart]))
}

func (k *Key) SetTimestamp(ts uint32) {
	binary.BigEndian.PutUint32(k[TimestampStart:TimestampEnd], ts)
}

func (k Key) Timestamp() uint32 {
	return binary.BigEndian.Uint32(k[TimestampStart:TimestampEnd])
}

func (k *Key) SetDuration(dur uint16) {
	binary.BigEndian.PutUint16(k[DurationStart:DurationEnd], dur)
}

func (k Key) Duration() uint16 {
	return binary.BigEndian.Uint16(k[DurationStart:DurationEnd])
}
