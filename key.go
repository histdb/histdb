package lsm

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type Key [20]byte

func (k Key) String() string {
	return fmt.Sprintf("(key %x %x %08x)", k.TagHash(), k.MetricHash(), k.Timestamp())
}

func (k Key) Hash() (h [16]byte) {
	copy(h[0:16], k[0:16])
	return h
}

func (k *Key) SetTagHash(th [8]byte) { copy(k[0:8], th[0:8]) }
func (k Key) TagHash() (th [8]byte) {
	copy(th[0:8], k[0:8])
	return th
}

func (k *Key) SetMetricHash(mh [8]byte) { copy(k[8:16], mh[0:8]) }
func (k Key) MetricHash() (mh [8]byte) {
	copy(mh[0:8], k[8:16])
	return mh
}

func (k *Key) SetTimestamp(ts uint32) { binary.BigEndian.PutUint32(k[16:20], ts) }
func (k Key) Timestamp() uint32 {
	return binary.BigEndian.Uint32(k[16:20])
}

type keyCmp struct{}

var KeyCmp keyCmp

func (keyCmp) Compare(a, b Key) int   { return bytes.Compare(a[:], b[:]) }
func (keyCmp) Less(a, b Key) bool     { return string(a[:]) < string(b[:]) }
func (keyCmp) LessPtr(a, b *Key) bool { return string(a[:]) < string(b[:]) }
