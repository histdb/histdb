package bitmap

import (
	"math"
	"runtime"
	"testing"
)

func TestBitmap(t *testing.T) {
	var b T

	for i := uint(0); i < 64; i++ {
		b.AtomicSetIdx(i)

		got := b.Lowest()
		b.Next()
		if !b.Empty() || got != i {
			t.Fatal(i)
		}
		if b != (T{}) {
			t.Fatal(b)
		}
	}
}

func BenchmarkBitmap64(b *testing.B) {
	b.Run("Next", func(b *testing.B) {
		var bm T
		idx := uint(0)
		for i := 0; i < b.N; i++ {
			idx = bm.Lowest()
			bm.Next()
		}
		runtime.KeepAlive(idx)
		runtime.KeepAlive(b)
	})

	b.Run("NextAll", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			b := T{math.MaxUint64}
			for !b.Empty() {
				b.Next()
			}
		}
	})
}
