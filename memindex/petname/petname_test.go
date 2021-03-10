package petname

import (
	"testing"

	"github.com/zeebo/assert"
)

func TestUint32s(t *testing.T) {
	x := NewUint32s()

	for i := 0; i < 256; i++ {
		_, ok := x.Put(Hash{uint64(i), 0}, []uint32{uint32(i)})
		assert.That(t, !ok)
	}

	for i := 0; i < 256; i++ {
		t.Log(x.Get(uint32(i), nil))
	}
}
