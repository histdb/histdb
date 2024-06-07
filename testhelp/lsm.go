package testhelp

import (
	"encoding/binary"

	"github.com/zeebo/mwc"

	"github.com/histdb/histdb"
)

func Key() (key histdb.Key) {
	for i := range key {
		key[i] = byte(mwc.Uint64())
	}
	key[len(key)-1] = 0x80
	return key
}

func KeyFrom(tkh uint32, th uint64, ts uint32, dur uint16) (k histdb.Key) {
	binary.BigEndian.PutUint32(k.TagKeyHashPtr()[:], tkh)
	binary.BigEndian.PutUint64(k.TagHashPtr()[:], th)
	k.SetTimestamp(ts)
	k.SetDuration(dur)
	return k
}

func Timestamp() uint32 { return mwc.Uint32() }

func Name(n int) []byte {
	if n < 0 {
		n = mwc.Intn(20)
	}
	v := make([]byte, n)
	for i := range v {
		v[i] = 'a' + byte(mwc.Uint64n(26))
	}
	return v
}

func Value(n int) []byte {
	if n < 0 {
		n = mwc.Intn(20)
	}
	v := make([]byte, n)
	for i := range v {
		v[i] = byte(mwc.Uint64())
	}
	return v
}
