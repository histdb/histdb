package floathist

import (
	"testing"

	"github.com/aclements/go-perfevent/perfbench"
	"github.com/zeebo/assert"
	"github.com/zeebo/mwc"
)

func TestSum(t *testing.T) {
	type testCase struct {
		max uint64
		new func() layer2
		sum func(layer2) uint64
	}

	run := func(t *testing.T, tc testCase) {
		{
			l2 := tc.new()
			for k := uint32(0); k < l2S; k++ {
				assert.That(t, layer2_unsafeSetCounter(l2, nil, k, 1))
			}
			assert.Equal(t, 64, int(tc.sum(layer2_truncate(l2))))
		}

		rng := mwc.Rand()

		for i := 0; i < 1000; i++ {
			l2 := tc.new()
			var total uint64
			for k := uint32(0); k < l2S; k++ {
				v := rng.Uint64n(tc.max)
				total += v
				assert.That(t, layer2_unsafeSetCounter(l2, nil, k, v))
			}
			assert.Equal(t, total, tc.sum(layer2_truncate(l2)))
		}
	}

	t.Run("Small", func(t *testing.T) {
		if l2S == 64 && hasAVX2 {
			t.Run("AVX2", func(t *testing.T) {
				run(t, testCase{
					max: 2 * markAt,
					new: newLayer2_marked,
					sum: sumLayer2SmallAVX2,
				})
			})
		}

		t.Run("Generic", func(t *testing.T) {
			run(t, testCase{
				max: 2 * markAt,
				new: newLayer2_marked,
				sum: sumLayer2SmallFallback,
			})
		})
	})

	t.Run("Large", func(t *testing.T) {
		if l2S == 64 && hasAVX2 {
			t.Run("AVX2", func(t *testing.T) {
				run(t, testCase{
					max: 1 << 64 / 64,
					new: newLayer2_large,
					sum: sumLayer2LargeAVX2,
				})
			})
		}

		t.Run("Generic", func(t *testing.T) {
			run(t, testCase{
				max: 1 << 64 / 64,
				new: newLayer2_large,
				sum: sumLayer2LargeFallback,
			})
		})
	})
}

func BenchmarkSum(b *testing.B) {
	type testCase struct {
		max uint64
		new func() layer2
		sum func(layer2) uint64
	}

	run := func(b *testing.B, tc testCase) {
		l2 := tc.new()
		rng := mwc.Rand()

		for k := uint32(0); k < l2S; k++ {
			v := rng.Uint64n(tc.max)
			assert.That(b, layer2_unsafeSetCounter(l2, nil, k, v))
		}

		perfbench.Open(b)
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			tc.sum(layer2_truncate(l2))
		}
	}

	b.Run("Small", func(b *testing.B) {
		if l2S == 64 && hasAVX2 {
			b.Run("AVX2", func(b *testing.B) {
				run(b, testCase{
					max: 2 * markAt,
					new: newLayer2_marked,
					sum: sumLayer2SmallAVX2,
				})
			})
		}
		b.Run("Generic", func(b *testing.B) {
			run(b, testCase{
				max: 2 * markAt,
				new: newLayer2_marked,
				sum: sumLayer2SmallFallback,
			})
		})
	})

	b.Run("Large", func(b *testing.B) {
		if l2S == 64 && hasAVX2 {
			b.Run("AVX2", func(b *testing.B) {
				run(b, testCase{
					max: 1 << 64 / 64,
					new: newLayer2_large,
					sum: sumLayer2LargeAVX2,
				})
			})
		}

		b.Run("Generic", func(b *testing.B) {
			run(b, testCase{
				max: 1 << 64 / 64,
				new: newLayer2_large,
				sum: sumLayer2LargeFallback,
			})
		})
	})
}
