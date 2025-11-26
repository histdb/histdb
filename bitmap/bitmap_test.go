package bitmap

import (
	"fmt"
	"math"
	"runtime"
	"testing"

	"github.com/zeebo/assert"
)

func TestBitmap64(t *testing.T) {
	for i := range uint(64) {
		var bm T64

		assert.That(t, bm.Empty())
		assert.That(t, !bm.AtomicHas(i))

		bm.AtomicAddIdx(i)

		assert.That(t, !bm.Empty())
		assert.That(t, bm.AtomicHas(i))
		assert.Equal(t, bm.String(), fmt.Sprintf("%064b", bm.b))

		assert.Equal(t, New64(bm.b), bm)
		assert.Equal(t, bm.AtomicClone(), bm)

		low := bm.Lowest()
		high := bm.Highest()

		bm.ClearLowest()

		assert.That(t, bm.Empty())
		assert.Equal(t, low, high)
		assert.Equal(t, low, i)
		assert.Equal(t, bm, T64{})
	}
}

func TestBitmap32(t *testing.T) {
	for i := range uint(32) {
		var bm T32

		assert.That(t, bm.Empty())
		assert.That(t, !bm.AtomicHas(i))

		bm.AtomicAddIdx(i)

		assert.That(t, !bm.Empty())
		assert.That(t, bm.AtomicHas(i))
		assert.Equal(t, bm.String(), fmt.Sprintf("%032b", bm.b))

		assert.Equal(t, New32(bm.b), bm)
		assert.Equal(t, bm.AtomicClone(), bm)

		low := bm.Lowest()
		high := bm.Highest()

		bm.ClearLowest()

		assert.That(t, bm.Empty())
		assert.Equal(t, low, high)
		assert.Equal(t, low, i)
		assert.Equal(t, bm, T32{})
	}
}

func BenchmarkBitmap64(b *testing.B) {
	b.Run("Next", func(b *testing.B) {
		var bm T64
		idx := uint(0)
		for b.Loop() {
			idx = bm.Lowest()
			bm.ClearLowest()
		}
		runtime.KeepAlive(idx)
		runtime.KeepAlive(b)
	})

	b.Run("NextAll", func(b *testing.B) {
		for b.Loop() {
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
		for b.Loop() {
			idx = bm.Lowest()
			bm.ClearLowest()
		}
		runtime.KeepAlive(idx)
		runtime.KeepAlive(b)
	})

	b.Run("NextAll", func(b *testing.B) {
		for b.Loop() {
			b := T32{math.MaxUint32}
			for !b.Empty() {
				b.ClearLowest()
			}
		}
	})
}
