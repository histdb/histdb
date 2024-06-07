package bitmap

import (
	"math"
	"runtime"
	"testing"
)

func TestBitmap64(t *testing.T) {
	var bm T64

	for i := uint(0); i < 64; i++ {
		bm.AtomicAddIdx(i)

		got := bm.Lowest()
		bm.ClearLowest()
		if !bm.Empty() || got != i {
			t.Fatal(i)
		}
		if bm != (T64{}) {
			t.Fatal(bm)
		}
	}
}

func TestBitmap32(t *testing.T) {
	var bm T32

	for i := uint(0); i < 32; i++ {
		bm.AtomicAddIdx(i)

		got := bm.Lowest()
		bm.ClearLowest()
		if !bm.Empty() || got != i {
			t.Fatal(i)
		}
		if bm != (T32{}) {
			t.Fatal(bm)
		}
	}
}

func BenchmarkBitmap64(b *testing.B) {
	b.Run("Next", func(b *testing.B) {
		var bm T64
		idx := uint(0)
		for range b.N {
			idx = bm.Lowest()
			bm.ClearLowest()
		}
		runtime.KeepAlive(idx)
		runtime.KeepAlive(b)
	})

	b.Run("NextAll", func(b *testing.B) {
		for range b.N {
			b := T64{math.MaxUint64}
			for !b.Empty() {
				b.ClearLowest()
			}
		}
	})
}

func BenchmarkBitmap32(b *testing.B) {
	b.Run("Next", func(b *testing.B) {
		var bm T32
		idx := uint(0)
		for range b.N {
			idx = bm.Lowest()
			bm.ClearLowest()
		}
		runtime.KeepAlive(idx)
		runtime.KeepAlive(b)
	})

	b.Run("NextAll", func(b *testing.B) {
		for range b.N {
			b := T32{math.MaxUint32}
			for !b.Empty() {
				b.ClearLowest()
			}
		}
	})
}
