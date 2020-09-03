package testhelp

import (
	"github.com/zeebo/lsm"
	"github.com/zeebo/pcg"
)

func Key() (key lsm.Key) {
	for i := range key {
		key[i] = byte(pcg.Uint32n(256))
	}
	key[len(key)-1] = 0x80
	return key
}

func Timestamp() uint32 {
	return pcg.Uint32()
}

func Name(n int) []byte {
	v := make([]byte, n)
	for i := range v {
		v[i] = byte(pcg.Uint32n(256))
	}
	return v
}

func Value(n int) []byte {
	v := make([]byte, n)
	for i := range v {
		v[i] = byte(pcg.Uint32n(256))
	}
	return v
}
