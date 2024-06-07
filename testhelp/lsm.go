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

func KeyFrom(tkh uint64, th uint64, ts uint32, dur uint32) (k histdb.Key) {
	binary.BigEndian.PutUint64(k.TagKeyHashPtr()[:], tkh)
	binary.BigEndian.PutUint64(k.TagHashPtr()[:], th)
	k.SetTimestamp(ts)
	k.SetDuration(dur)
	return k
}

func Timestamp() uint32 { return mwc.Uint32() }

func Metric(n int) (v []byte) {
	if n <= 0 {
		n = mwc.Intn(10) + 1
	}
	v = make([]byte, 0, n*5-1)
	for i := range n {
		if i > 0 {
			v = append(v, ',')
		}
		for range 2 {
			v = append(v, 'a'+byte(mwc.Uint64n(26)))
		}
		v = append(v, '=')
		v = append(v, '0'+byte(mwc.Uint64n(10)))
	}
	return v
}

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
