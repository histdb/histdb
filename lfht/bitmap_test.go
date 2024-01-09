package lfht

import (
	"math"
	"runtime"
	"testing"
)

func TestBitmap(t *testing.T) {
	var bm bmap

	for i := uint(0); i < 64; i++ {
		bm.AtomicSetIdx(i)

		got := bm.Lowest()
		bm.ClearLowest()
		if !bm.Empty() || got != i {
			t.Fatal(i)
		}
		if bm != (bmap{}) {
			t.Fatal(bm)
		}
	}
}

func BenchmarkBitmap64(b *testing.B) {
	b.Run("Next", func(b *testing.B) {
		var bm bmap
		idx := uint(0)
		for i := 0; i < b.N; i++ {
			idx = bm.Lowest()
			bm.ClearLowest()
		}
		runtime.KeepAlive(idx)
		runtime.KeepAlive(b)
	})

	b.Run("NextAll", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			b := bmap{math.MaxUint64}
			for !b.Empty() {
				b.ClearLowest()
			}
		}
	})
}
