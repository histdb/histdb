package flathist

import (
	"runtime"
	"testing"

	"github.com/aclements/go-perfevent/perfbench"
	"github.com/zeebo/assert"
)

func BenchmarkBitmask(b *testing.B) {
	const set = 1 << 31

	a := &[16]uint32{
		set, 0, 0, set,
		0, 0, set, set,
		set, set, 0, set,
		0, 0, 0, set,
	}

	assert.Equal(b, bitmask(a) /*   */, 0b1000101111001001)
	assert.Equal(b, bitmaskFallback(a), 0b1000101111001001)

	b.Run("Native", func(b *testing.B) {
		perfbench.Open(b)

		var count uint32
		for i := 0; i < b.N; i++ {
			count += bitmask(a)
		}
		runtime.KeepAlive(count)
	})

	b.Run("Fallback", func(b *testing.B) {
		perfbench.Open(b)

		var count uint32
		for i := 0; i < b.N; i++ {
			count += bitmaskFallback(a)
		}
		runtime.KeepAlive(count)
	})
}
