package floathist

import (
	"testing"

	"github.com/zeebo/assert"
	"github.com/zeebo/mwc"
	"golang.org/x/sys/cpu"
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
			for k := uint32(0); k < l2Size; k++ {
				assert.That(t, layer2_unsafeSetCounter(l2, nil, k, 1))
			}
			assert.Equal(t, 64, int(tc.sum(layer2_truncate(l2))))
		}

		rng := mwc.Rand()

		for i := 0; i < 1000; i++ {
			l2 := tc.new()
			var total uint64
			for k := uint32(0); k < l2Size; k++ {
				v := rng.Uint64n(tc.max)
				total += v
				assert.That(t, layer2_unsafeSetCounter(l2, nil, k, v))
			}
			assert.Equal(t, total, tc.sum(layer2_truncate(l2)))
		}
	}

	t.Run("Small", func(t *testing.T) {
		if l2Size == 64 && cpu.X86.HasAVX2 {
			t.Run("AVX2_32", func(t *testing.T) {
				run(t, testCase{
					max: markAt,
					new: newLayer2_small,
					sum: sumLayer2SmallAVX2_32,
				})
			})

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
				sum: sumLayer2SmallSlow,
			})
		})
	})

	t.Run("Large", func(t *testing.T) {
		if l2Size == 64 && cpu.X86.HasAVX2 {
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
				sum: sumLayer2LargeSlow,
			})
		})
	})
}
