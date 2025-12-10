package flathist

import (
	"math"
	"sync/atomic"
	"testing"

	"github.com/aclements/go-perfevent/perfbench"
	"github.com/zeebo/assert"
	"github.com/zeebo/mwc"
)

func TestSimple(t *testing.T) {
	t.Run("MinMax", func(t *testing.T) {
		h := NewHistogram()

		for i := float32(0); i < 1000; i++ {
			h.Observe(i)
		}

		assert.Equal(t, h.Min(), 0.)
		assert.Equal(t, h.Max(), 998.)
	})

	t.Run("Total", func(t *testing.T) {
		h := NewHistogram()

		for i := float32(0); i < 1000; i++ {
			h.Observe(i)
		}

		assert.Equal(t, h.Total(), 1000)
	})

	t.Run("Quantile", func(t *testing.T) {
		h := NewHistogram()

		for i := float32(0); i < 1000; i++ {
			h.Observe(i)
		}

		assert.Equal(t, h.Quantile(0), 0.)
		assert.Equal(t, h.Quantile(.25), 250.)
		assert.Equal(t, h.Quantile(.5), 500.)
		assert.Equal(t, h.Quantile(1), 998.)
		assert.Equal(t, h.Quantile(2), 998.)
	})
}

func BenchmarkSimple(b *testing.B) {
	b.Run("Observe", func(b *testing.B) {
		h := NewHistogram()

		perfbench.Open(b)
		b.ReportAllocs()
		b.ResetTimer()

		for b.Loop() {
			h.Observe(1)
		}
	})

	b.Run("Observe_Parallel", func(b *testing.B) {
		h := NewHistogram()

		b.ReportAllocs()
		b.ResetTimer()

		n := int64(0)
		b.RunParallel(func(pb *testing.PB) {
			i := float32(uint64(1024) << uint64(atomic.AddInt64(&n, 1)))
			for pb.Next() {
				h.Observe(i)
			}
		})
	})

	b.Run("Min", func(b *testing.B) {
		rng := mwc.Rand()

		h := NewHistogram()

		for range 1000000 {
			h.Observe(math.Float32frombits(rng.Uint32() &^ ((1<<10 - 1) << 22)))
		}

		perfbench.Open(b)
		b.ReportAllocs()
		b.ResetTimer()

		for b.Loop() {
			h.Min()
		}
	})

	b.Run("Max", func(b *testing.B) {
		rng := mwc.Rand()

		h := NewHistogram()

		for range 1000000 {
			h.Observe(math.Float32frombits(rng.Uint32() &^ ((1<<10 - 1) << 22)))
		}

		perfbench.Open(b)
		b.ReportAllocs()
		b.ResetTimer()

		for b.Loop() {
			h.Max()
		}
	})

	b.Run("Total", func(b *testing.B) {
		rng := mwc.Rand()

		h := NewHistogram()

		for range 1000000 {
			h.Observe(rng.Float32())
		}

		perfbench.Open(b)
		b.ReportAllocs()
		b.ResetTimer()

		for b.Loop() {
			h.Total()
		}
	})

	b.Run("Total_Easy", func(b *testing.B) {
		rng := mwc.Rand()

		h := NewHistogram()

		for range 1000000 {
			h.Observe(math.Float32frombits(rng.Uint32() &^ ((1<<10 - 1) << 22)))
		}
		assert.Equal(b, h.Total(), 1000000)

		perfbench.Open(b)
		b.ReportAllocs()
		b.ResetTimer()

		for b.Loop() {
			h.Total()
		}
	})

	b.Run("Quantile", func(b *testing.B) {
		rng := mwc.Rand()

		h := NewHistogram()

		for range 1000000 {
			h.Observe(rng.Float32())
		}
		assert.Equal(b, h.Total(), 1000000)

		perfbench.Open(b)
		b.ReportAllocs()
		b.ResetTimer()

		for b.Loop() {
			h.Quantile(.95)
		}
	})

	b.Run("Quantile_Easy", func(b *testing.B) {
		rng := mwc.Rand()

		h := NewHistogram()

		for range 1000000 {
			h.Observe(math.Float32frombits(rng.Uint32() &^ ((1<<10 - 1) << 22)))
		}
		assert.Equal(b, h.Total(), 1000000)

		perfbench.Open(b)
		b.ReportAllocs()
		b.ResetTimer()

		for b.Loop() {
			h.Quantile(rng.Float64())
		}
	})

	b.Run("CDF", func(b *testing.B) {
		rng := mwc.Rand()

		h := NewHistogram()

		for range 1000000 {
			h.Observe(rng.Float32())
		}
		assert.Equal(b, h.Total(), 1000000)

		perfbench.Open(b)
		b.ReportAllocs()
		b.ResetTimer()

		for b.Loop() {
			h.CDF(rng.Float32())
		}
	})

	b.Run("CDF_Easy", func(b *testing.B) {
		rng := mwc.Rand()

		h := NewHistogram()

		for range 1000000 {
			h.Observe(math.Float32frombits(rng.Uint32() &^ ((1<<10 - 1) << 22)))
		}
		assert.Equal(b, h.Total(), 1000000)

		perfbench.Open(b)
		b.ReportAllocs()
		b.ResetTimer()

		for b.Loop() {
			h.CDF(rng.Float32())
		}
	})

	b.Run("Summary", func(b *testing.B) {
		rng := mwc.Rand()

		h := NewHistogram()

		for range 1000 {
			h.Observe(rng.Float32())
		}
		assert.Equal(b, h.Total(), 1000)

		perfbench.Open(b)
		b.ReportAllocs()
		b.ResetTimer()

		for b.Loop() {
			_, _, _, _ = h.Summary()
		}
	})

	b.Run("Alloc_One", func(b *testing.B) {
		perfbench.Open(b)
		b.ReportAllocs()
		b.ResetTimer()

		for b.Loop() {
			NewHistogram().Observe(1)
		}
	})

	b.Run("Alloc_Many", func(b *testing.B) {
		perfbench.Open(b)
		b.ReportAllocs()
		b.ResetTimer()

		for b.Loop() {
			for range 1000 {
				NewHistogram().Observe(1)
			}
		}
	})

}
