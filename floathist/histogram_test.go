package floathist

import (
	"math"
	"sync/atomic"
	"testing"

	"github.com/aclements/go-perfevent/perfbench"
	"github.com/zeebo/assert"
	"github.com/zeebo/mwc"
)

func TestHistogram(t *testing.T) {
	t.Run("Reset", func(t *testing.T) {
		var h T
		for i := float32(0); i < 1000; i++ {
			h.Observe(i)
		}

		assert.Equal(t, h.Total(), 1000)
		h.Reset()
		assert.Equal(t, h.Total(), 0)
		h.Observe(1)
		assert.Equal(t, h.Total(), 1)
	})

	t.Run("MinMax", func(t *testing.T) {
		var h T
		for i := float32(0); i < 1000; i++ {
			h.Observe(i)
		}

		assert.Equal(t, h.Min(), 0.)
		assert.Equal(t, h.Max(), 1000.)
	})

	t.Run("Total", func(t *testing.T) {
		var h T
		for i := float32(0); i < 1000; i++ {
			h.Observe(i)
		}

		assert.Equal(t, h.Total(), 1000)
	})

	t.Run("Quantile", func(t *testing.T) {
		var h T
		for i := float32(0); i < 1000; i++ {
			h.Observe(i)
		}

		assert.Equal(t, h.Quantile(0), 0.)
		assert.Equal(t, h.Quantile(.25), 248.)
		assert.Equal(t, h.Quantile(.5), 496.)
		assert.Equal(t, h.Quantile(1), 1000.)
		assert.Equal(t, h.Quantile(2), 1000.)
	})

	t.Run("CDF", func(t *testing.T) {
		var h T
		for i := float32(0); i < 1000; i++ {
			h.Observe(i)
		}

		assert.Equal(t, h.CDF(0), 0.)
		assert.Equal(t, h.CDF(250), 0.25)
		assert.Equal(t, h.CDF(500), 0.5)
		assert.Equal(t, h.CDF(1000), 0.996)
		assert.Equal(t, h.CDF(1008), 1.0)
	})

	t.Run("Summary", func(t *testing.T) {
		var h T
		for i := float32(0); i < 1000; i++ {
			h.Observe(i)
		}

		total, sum, avg, vari := h.Summary()

		assert.Equal(t, total, 1000.)
		assert.Equal(t, sum, 500021.328125)      // 499500
		assert.Equal(t, avg, 500.021328125)      // 499.5
		assert.Equal(t, vari, 83447.18984652992) // 83416.667
	})

	t.Run("Merge", func(t *testing.T) {
		var h T
		for i := float32(0); i < 1000; i++ {
			h.Observe(i)
		}

		const doublings = 54

		for i := 0; i < doublings; i++ {
			assert.NoError(t, h.Merge(&h))
		}

		total, _, avg, _ := h.Summary()

		assert.Equal(t, h.Total(), uint64(1000*(1<<doublings)))
		assert.Equal(t, total, 1000.*(1<<doublings))
		assert.Equal(t, avg, 500.021328125) // 499.5
	})
}

func BenchmarkHistogram(b *testing.B) {
	b.Run("Observe", func(b *testing.B) {
		var h T

		perfbench.Open(b)
		b.ReportAllocs()
		b.ResetTimer()

		for range b.N {
			h.Observe(1)
		}
	})

	b.Run("Observe_Parallel", func(b *testing.B) {
		var h T

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

		his := new(T)
		for i := 0; i < 1000000; i++ {
			his.Observe(math.Float32frombits(rng.Uint32() &^ ((1<<10 - 1) << 22)))
		}

		perfbench.Open(b)
		b.ReportAllocs()
		b.ResetTimer()

		for range b.N {
			his.Min()
		}
	})

	b.Run("Max", func(b *testing.B) {
		rng := mwc.Rand()

		his := new(T)
		for i := 0; i < 1000000; i++ {
			his.Observe(math.Float32frombits(rng.Uint32() &^ ((1<<10 - 1) << 22)))
		}

		perfbench.Open(b)
		b.ReportAllocs()
		b.ResetTimer()

		for range b.N {
			his.Max()
		}
	})

	b.Run("Total", func(b *testing.B) {
		rng := mwc.Rand()

		his := new(T)
		for i := 0; i < 1000000; i++ {
			his.Observe(rng.Float32())
		}

		perfbench.Open(b)
		b.ReportAllocs()
		b.ResetTimer()

		for range b.N {
			his.Total()
		}
	})

	b.Run("Total_Easy", func(b *testing.B) {
		rng := mwc.Rand()

		his := new(T)
		for i := 0; i < 1000000; i++ {
			his.Observe(math.Float32frombits(rng.Uint32() &^ ((1<<10 - 1) << 22)))
		}
		assert.Equal(b, his.Total(), 1000000)

		perfbench.Open(b)
		b.ReportAllocs()
		b.ResetTimer()

		for range b.N {
			his.Total()
		}
	})

	b.Run("Quantile", func(b *testing.B) {
		rng := mwc.Rand()

		his := new(T)
		for i := 0; i < 1000000; i++ {
			his.Observe(rng.Float32())
		}
		assert.Equal(b, his.Total(), 1000000)

		perfbench.Open(b)
		b.ReportAllocs()
		b.ResetTimer()

		for range b.N {
			his.Quantile(.95)
		}
	})

	b.Run("Quantile_Easy", func(b *testing.B) {
		rng := mwc.Rand()

		his := new(T)
		for i := 0; i < 1000000; i++ {
			his.Observe(math.Float32frombits(rng.Uint32() &^ ((1<<10 - 1) << 22)))
		}
		assert.Equal(b, his.Total(), 1000000)

		perfbench.Open(b)
		b.ReportAllocs()
		b.ResetTimer()

		for range b.N {
			his.Quantile(rng.Float64())
		}
	})

	b.Run("CDF", func(b *testing.B) {
		rng := mwc.Rand()

		his := new(T)
		for i := 0; i < 1000000; i++ {
			his.Observe(rng.Float32())
		}
		assert.Equal(b, his.Total(), 1000000)

		perfbench.Open(b)
		b.ReportAllocs()
		b.ResetTimer()

		for range b.N {
			his.CDF(rng.Float32())
		}
	})

	b.Run("CDF_Easy", func(b *testing.B) {
		rng := mwc.Rand()

		his := new(T)
		for i := 0; i < 1000000; i++ {
			his.Observe(math.Float32frombits(rng.Uint32() &^ ((1<<10 - 1) << 22)))
		}
		assert.Equal(b, his.Total(), 1000000)

		perfbench.Open(b)
		b.ReportAllocs()
		b.ResetTimer()

		for range b.N {
			his.CDF(rng.Float32())
		}
	})

	b.Run("Summary", func(b *testing.B) {
		rng := mwc.Rand()

		his := new(T)
		for i := 0; i < 1000; i++ {
			his.Observe(rng.Float32())
		}
		assert.Equal(b, his.Total(), 1000)

		perfbench.Open(b)
		b.ReportAllocs()
		b.ResetTimer()

		for range b.N {
			_, _, _, _ = his.Summary()
		}
	})
}
