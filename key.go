package lsm

import (
	"bytes"
	"encoding/hex"
)

type Key [16]byte

func (k Key) String() string {
	return hex.EncodeToString(k[:])
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

type keyCmp struct{}

var KeyCmp keyCmp

func (keyCmp) Compare(a, b Key) int   { return bytes.Compare(a[:], b[:]) }
func (keyCmp) Less(a, b Key) bool     { return string(a[:]) < string(b[:]) }
func (keyCmp) LessPtr(a, b *Key) bool { return string(a[:]) < string(b[:]) }
