package testhelp

import (
	"github.com/zeebo/mwc"

	"github.com/histdb/histdb"
)

var (
	keyRng  = mwc.Rand()
	tsRng   = mwc.Rand()
	nameRng = mwc.Rand()
	valRng  = mwc.Rand()
)

func Key() (key histdb.Key) {
	for i := range key {
		key[i] = byte(keyRng.Uint64())
	}
	key[len(key)-1] = 0x80
	return key
}

func KeyFrom(th uint64, mh uint64, ts uint32) histdb.Key {
	return histdb.Key{
		byte(th >> 0x38), byte(th >> 0x30), byte(th >> 0x28), byte(th >> 0x20),
		byte(th >> 0x18), byte(th >> 0x10), byte(th >> 0x08), byte(th),
		byte(mh >> 0x38), byte(mh >> 0x30), byte(mh >> 0x28), byte(mh >> 0x20),
		byte(mh >> 0x18), byte(mh >> 0x10), byte(mh >> 0x08), byte(mh),
		byte(ts >> 0x18), byte(ts >> 0x10), byte(ts >> 0x08), byte(ts),
	}
}

func Timestamp() uint32 {
	return tsRng.Uint32()
}

func Name(n int) []byte {
	v := make([]byte, n)
	for i := range v {
		v[i] = byte(nameRng.Uint64())
	}
	return v
}

func Value(n int) []byte {
	v := make([]byte, n)
	for i := range v {
		v[i] = byte(valRng.Uint64())
	}
	return v
}
