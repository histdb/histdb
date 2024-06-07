package flathist

import (
	"runtime"
	"testing"

	"github.com/aclements/go-perfevent/perfbench"
	"github.com/zeebo/assert"
)

func BenchmarkBitmask(b *testing.B) {
	const set = 1 << 31

	a := &[32]uint32{
		set, 0, 0, set,
		0, 0, set, set,
		set, set, 0, set,
		0, 0, 0, set,
		set, 0, 0, 0,
		0, set, set, 0,
		0, 0, 0, 0,
		set, set, set, set,
	}

	assert.Equal(b, bitmaskFallback(a), 0b1111_0000_0110_0001_1000_1011_1100_1001)
	assert.Equal(b, bitmask(a) /*   */, 0b1111_0000_0110_0001_1000_1011_1100_1001)

	b.Run("Native", func(b *testing.B) {
		perfbench.Open(b)

		var count uint32
		for range b.N {
			count += bitmask(a)
		}
		runtime.KeepAlive(count)
	})

	b.Run("Fallback", func(b *testing.B) {
		perfbench.Open(b)

		var count uint32
		for range b.N {
			count += bitmaskFallback(a)
		}
		runtime.KeepAlive(count)
	})
}
