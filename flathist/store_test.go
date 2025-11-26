package flathist

import (
	"math"
	"sync/atomic"
	"testing"

	"github.com/aclements/go-perfevent/perfbench"
	"github.com/zeebo/assert"
	"github.com/zeebo/mwc"
)

func TestStore(t *testing.T) {
	t.Run("Merge", func(t *testing.T) {
		var s1 S
		var s2 S

		h1 := s1.New()
		h2 := s2.New()

		// we want to test these cases:
		// 1. merge small into large (easy)
		// 2. merge large into large (easy)
		// 3. merge small into small (with no growth, easy)
		// 4. merge large into small (with no growth, easy)
		// 5. merge small into small (with growth, hard)
		// 6. merge large into small (with growth, hard)
		// 7. merge small into empty
		// 8. merge large into empty

		h1l0 := s1.getL0(h1)
		h1l0.l1[0] = s1.l1.New().Raw() | 1<<31
		h1l1 := s1.getL1(h1l0.l1[0])
		h1l1.l2[0] = s1.l2l.New().Raw() | (l2TagLarge << 29)
		h1l1.l2[1] = s1.l2l.New().Raw() | (l2TagLarge << 29)
		h1l1.l2[2] = s1.l2s.New().Raw() | (l2TagSmall << 29)
		h1l1.l2[3] = s1.l2s.New().Raw() | (l2TagSmall << 29)
		h1l1.l2[4] = s1.l2s.New().Raw() | (l2TagSmall << 29)
		h1l1.l2[5] = s1.l2s.New().Raw() | (l2TagSmall << 29)

		h2l0 := s2.getL0(h2)
		h2l0.l1[0] = s2.l1.New().Raw() | 1<<31
		h2l1 := s2.getL1(h2l0.l1[0])
		h2l1.l2[0] = s2.l2s.New().Raw() | (l2TagSmall << 29)
		h2l1.l2[1] = s2.l2l.New().Raw() | (l2TagLarge << 29)
		h2l1.l2[2] = s2.l2s.New().Raw() | (l2TagSmall << 29)
		h2l1.l2[3] = s2.l2l.New().Raw() | (l2TagLarge << 29)
		h2l1.l2[4] = s2.l2s.New().Raw() | (l2TagSmall << 29)
		h2l1.l2[5] = s2.l2l.New().Raw() | (l2TagLarge << 29)
		h2l1.l2[6] = s2.l2s.New().Raw() | (l2TagSmall << 29)
		h2l1.l2[7] = s2.l2l.New().Raw() | (l2TagLarge << 29)

		// case 1.
		s1.getL2L(h1l1.l2[0]).cs[0] = l2GrowAt + 1
		s1.getL2L(h1l1.l2[0]).cs[1] = l2GrowAt + 1
		s1.getL2L(h1l1.l2[0]).cs[9] = 1
		s2.getL2S(h2l1.l2[0]).cs[1] = 1
		s2.getL2S(h2l1.l2[0]).cs[2] = 1

		// case 2.
		s1.getL2L(h1l1.l2[1]).cs[0] = l2GrowAt + 1
		s1.getL2L(h1l1.l2[1]).cs[1] = l2GrowAt + 1
		s1.getL2L(h1l1.l2[1]).cs[9] = 1
		s2.getL2L(h2l1.l2[1]).cs[1] = l2GrowAt + 1
		s2.getL2L(h2l1.l2[1]).cs[2] = l2GrowAt + 1

		// case 3.
		s1.getL2S(h1l1.l2[2]).cs[0] = 1
		s1.getL2S(h1l1.l2[2]).cs[1] = 1
		s1.getL2S(h1l1.l2[2]).cs[9] = 1
		s2.getL2S(h2l1.l2[2]).cs[1] = 1
		s2.getL2S(h2l1.l2[2]).cs[2] = 1

		// case 4.
		s1.getL2S(h1l1.l2[3]).cs[0] = 1
		s1.getL2S(h1l1.l2[3]).cs[1] = 1
		s1.getL2S(h1l1.l2[3]).cs[9] = 1
		s2.getL2L(h2l1.l2[3]).cs[1] = 1
		s2.getL2L(h2l1.l2[3]).cs[2] = 1

		// case 5.
		s1.getL2S(h1l1.l2[4]).cs[0] = 1
		s1.getL2S(h1l1.l2[4]).cs[1] = l2GrowAt
		s1.getL2S(h1l1.l2[4]).cs[9] = 1
		s2.getL2S(h2l1.l2[4]).cs[1] = 1
		s2.getL2S(h2l1.l2[4]).cs[2] = 1

		// case 6.
		s1.getL2S(h1l1.l2[5]).cs[0] = 1
		s1.getL2S(h1l1.l2[5]).cs[1] = l2GrowAt
		s1.getL2S(h1l1.l2[5]).cs[9] = 1
		s2.getL2L(h2l1.l2[5]).cs[1] = 1
		s2.getL2L(h2l1.l2[5]).cs[2] = 1

		// case 7.
		s2.getL2S(h2l1.l2[6]).cs[1] = 1
		s2.getL2S(h2l1.l2[6]).cs[2] = 1

		// case 8.
		s2.getL2L(h2l1.l2[7]).cs[1] = l2GrowAt + 1
		s2.getL2L(h2l1.l2[7]).cs[2] = l2GrowAt + 1

		Merge(&s1, h1, &s2, h2)

		assert.Equal(t, s1.getL2L(h1l1.l2[0]).cs, [64]uint64{
			0: l2GrowAt + 1, 1: l2GrowAt + 2, 2: 1, 9: 1,
		})
		assert.Equal(t, s1.getL2L(h1l1.l2[1]).cs, [64]uint64{
			0: l2GrowAt + 1, 1: 2*l2GrowAt + 2, 2: l2GrowAt + 1, 9: 1,
		})
		assert.Equal(t, s1.getL2S(h1l1.l2[2]).cs, [64]uint32{
			0: 1, 1: 2, 2: 1, 9: 1,
		})
		assert.Equal(t, s1.getL2S(h1l1.l2[3]).cs, [64]uint32{
			0: 1, 1: 2, 2: 1, 9: 1,
		})
		assert.Equal(t, s1.getL2L(h1l1.l2[4]).cs, [64]uint64{
			0: 1, 1: l2GrowAt + 1, 2: 1, 9: 1,
		})
		assert.Equal(t, s1.getL2L(h1l1.l2[5]).cs, [64]uint64{
			0: 1, 1: l2GrowAt + 1, 2: 1, 9: 1,
		})
		assert.Equal(t, s1.getL2S(h1l1.l2[6]).cs, [64]uint32{
			1: 1, 2: 1,
		})
		assert.Equal(t, s1.getL2L(h1l1.l2[7]).cs, [64]uint64{
			1: l2GrowAt + 1, 2: l2GrowAt + 1,
		})
	})

	t.Run("Iterate", func(t *testing.T) {
		var s S

		s.Observe(s.New(), 1)
		s.Observe(s.New(), 2)
		s.Observe(s.New(), 3)

		assert.Equal(t, s.Count(), 3)

		n := 1.
		s.Iterate(func(h H) bool {
			assert.Equal(t, s.Min(h), n)
			n++
			return true
		})
		assert.Equal(t, n, 4.)
	})

	t.Run("MinMax", func(t *testing.T) {
		var s S

		h := s.New()
		for i := float32(0); i < 1000; i++ {
			s.Observe(h, i)
		}

		assert.Equal(t, s.Min(h), 0.)
		assert.Equal(t, s.Max(h), 998.)
	})

	t.Run("Total", func(t *testing.T) {
		var s S

		h := s.New()
		for i := float32(0); i < 1000; i++ {
			s.Observe(h, i)
		}

		assert.Equal(t, s.Total(h), 1000)
	})

	t.Run("Quantile", func(t *testing.T) {
		var s S

		h := s.New()
		for i := float32(0); i < 1000; i++ {
			s.Observe(h, i)
		}

		assert.Equal(t, s.Quantile(h, 0), 0.)
		assert.Equal(t, s.Quantile(h, .25), 250.)
		assert.Equal(t, s.Quantile(h, .5), 500.)
		assert.Equal(t, s.Quantile(h, 1), 998.)
		assert.Equal(t, s.Quantile(h, 2), 998.)
	})
}

func BenchmarkHistogram(b *testing.B) {
	b.Run("Observe", func(b *testing.B) {
		var s S
		h := s.New()

		perfbench.Open(b)
		b.ReportAllocs()
		b.ResetTimer()

		for b.Loop() {
			s.Observe(h, 1)
		}
	})

	b.Run("Observe_Parallel", func(b *testing.B) {
		var s S
		h := s.New()

		b.ReportAllocs()
		b.ResetTimer()

		n := int64(0)
		b.RunParallel(func(pb *testing.PB) {
			i := float32(uint64(1024) << uint64(atomic.AddInt64(&n, 1)))
			for pb.Next() {
				s.Observe(h, i)
			}
		})
	})

	b.Run("Min", func(b *testing.B) {
		rng := mwc.Rand()

		var s S
		h := s.New()

		for range 1000000 {
			s.Observe(h, math.Float32frombits(rng.Uint32()&^((1<<10-1)<<22)))
		}

		perfbench.Open(b)
		b.ReportAllocs()
		b.ResetTimer()

		for b.Loop() {
			s.Min(h)
		}
	})

	b.Run("Max", func(b *testing.B) {
		rng := mwc.Rand()

		var s S
		h := s.New()

		for range 1000000 {
			s.Observe(h, math.Float32frombits(rng.Uint32()&^((1<<10-1)<<22)))
		}

		perfbench.Open(b)
		b.ReportAllocs()
		b.ResetTimer()

		for b.Loop() {
			s.Max(h)
		}
	})

	b.Run("Total", func(b *testing.B) {
		rng := mwc.Rand()

		var s S
		h := s.New()

		for range 1000000 {
			s.Observe(h, rng.Float32())
		}

		perfbench.Open(b)
		b.ReportAllocs()
		b.ResetTimer()

		for b.Loop() {
			s.Total(h)
		}
	})

	b.Run("Total_Easy", func(b *testing.B) {
		rng := mwc.Rand()

		var s S
		h := s.New()

		for range 1000000 {
			s.Observe(h, math.Float32frombits(rng.Uint32()&^((1<<10-1)<<22)))
		}
		assert.Equal(b, s.Total(h), 1000000)

		perfbench.Open(b)
		b.ReportAllocs()
		b.ResetTimer()

		for b.Loop() {
			s.Total(h)
		}
	})

	b.Run("Quantile", func(b *testing.B) {
		rng := mwc.Rand()

		var s S
		h := s.New()

		for range 1000000 {
			s.Observe(h, rng.Float32())
		}
		assert.Equal(b, s.Total(h), 1000000)

		perfbench.Open(b)
		b.ReportAllocs()
		b.ResetTimer()

		for b.Loop() {
			s.Quantile(h, .95)
		}
	})

	b.Run("Quantile_Easy", func(b *testing.B) {
		rng := mwc.Rand()

		var s S
		h := s.New()

		for range 1000000 {
			s.Observe(h, math.Float32frombits(rng.Uint32()&^((1<<10-1)<<22)))
		}
		assert.Equal(b, s.Total(h), 1000000)

		perfbench.Open(b)
		b.ReportAllocs()
		b.ResetTimer()

		for b.Loop() {
			s.Quantile(h, rng.Float64())
		}
	})

	b.Run("CDF", func(b *testing.B) {
		rng := mwc.Rand()

		var s S
		h := s.New()

		for range 1000000 {
			s.Observe(h, rng.Float32())
		}
		assert.Equal(b, s.Total(h), 1000000)

		perfbench.Open(b)
		b.ReportAllocs()
		b.ResetTimer()

		for b.Loop() {
			s.CDF(h, rng.Float32())
		}
	})

	b.Run("CDF_Easy", func(b *testing.B) {
		rng := mwc.Rand()

		var s S
		h := s.New()

		for range 1000000 {
			s.Observe(h, math.Float32frombits(rng.Uint32()&^((1<<10-1)<<22)))
		}
		assert.Equal(b, s.Total(h), 1000000)

		perfbench.Open(b)
		b.ReportAllocs()
		b.ResetTimer()

		for b.Loop() {
			s.CDF(h, rng.Float32())
		}
	})

	b.Run("Summary", func(b *testing.B) {
		rng := mwc.Rand()

		var s S
		h := s.New()

		for range 1000 {
			s.Observe(h, rng.Float32())
		}
		assert.Equal(b, s.Total(h), 1000)

		perfbench.Open(b)
		b.ReportAllocs()
		b.ResetTimer()

		for b.Loop() {
			_, _, _, _ = s.Summary(h)
		}
	})
}
