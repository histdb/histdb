package petname

import (
	"testing"

	"github.com/zeebo/assert"
)

func TestUint32s(t *testing.T) {
	x := NewUint32s()
	for i := 0; i < 1<<15; i++ {
		_, ok := x.Put(Hash{0, uint64(i)}, []uint32{uint32(i)})
		assert.That(t, !ok)
	}
	for i := 0; i < 1<<15; i++ {
		_, ok := x.Put(Hash{0, uint64(i)}, []uint32{uint32(i)})
		assert.That(t, ok)
	}
	x.Fix()

	for i := 0; i < 1<<15; i++ {
		assert.DeepEqual(t, x.Get(uint32(i), nil), []uint32{uint32(i)})
	}
}
