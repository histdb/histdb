package histdb

import (
	"encoding/binary"
	"fmt"

	"github.com/histdb/histdb/rwutils"
)

type Key [KeySize]byte

func (k Key) AppendTo(w *rwutils.W)  { w.Bytes(k[:]) }
func (k *Key) ReadFrom(r *rwutils.R) { copy(k[:], r.Bytes(len(k))) }

func (k Key) Zero() bool { return k == Key{} }

func (k Key) String() string {
	return fmt.Sprintf("(key %s %08x %08x)", k.Hash(), k.Timestamp(), k.Duration())
}

func (k Key) Hash() (h Hash) {
	copy(h[0:HashSize], k[HashStart:HashEnd])
	return h
}

func (k *Key) HashPtr() *Hash {
	return (*Hash)(k[HashStart:HashEnd])
}

func (k *Key) TagKeyHashPtr() (th *TagKeyHash) {
	return (*TagKeyHash)(k[TagKeyHashStart:TagKeyHashEnd])
}

func (k *Key) TagHashPtr() (mh *TagHash) {
	return (*TagHash)(k[TagHashStart:TagHashEnd])
}

func (k *Key) SetTimestamp(ts uint32) {
	binary.BigEndian.PutUint32(k[TimestampStart:TimestampEnd], ts)
}

func (k Key) Timestamp() uint32 {
	return binary.BigEndian.Uint32(k[TimestampStart:TimestampEnd])
}

func (k *Key) SetDuration(dur uint32) {
	binary.BigEndian.PutUint32(k[DurationStart:DurationEnd], dur)
}

func (k Key) Duration() uint32 {
	return binary.BigEndian.Uint32(k[DurationStart:DurationEnd])
}
