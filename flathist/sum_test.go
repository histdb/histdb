package flathist

import (
	"testing"

	"github.com/aclements/go-perfevent/perfbench"
	"github.com/zeebo/assert"
	"github.com/zeebo/mwc"
)

func TestSum(t *testing.T) {
	t.Run("Small", func(t *testing.T) {
		t.Run("AVX2", func(t *testing.T) { runTestSum(t, layer2SmallAVX2) })
		t.Run("Fallback", func(t *testing.T) { runTestSum(t, layer2SmallFallback) })
	})
	t.Run("Large", func(t *testing.T) {
		t.Run("AVX2", func(t *testing.T) { runTestSum(t, layer2LargeAVX2) })
		t.Run("Fallback", func(t *testing.T) { runTestSum(t, layer2LargeFallback) })
	})
}

func BenchmarkSum(b *testing.B) {
	b.Run("Small", func(b *testing.B) {
		b.Run("AVX2", func(b *testing.B) { runBenchSum(b, layer2SmallAVX2) })
		b.Run("Fallback", func(b *testing.B) { runBenchSum(b, layer2SmallFallback) })
	})
	b.Run("Large", func(b *testing.B) {
		b.Run("AVX2", func(b *testing.B) { runBenchSum(b, layer2LargeAVX2) })
		b.Run("Fallback", func(b *testing.B) { runBenchSum(b, layer2LargeFallback) })
	})
}

type testSumCase[T any] struct {
	new func() *T
	sum func(*T) uint64
	set func(*T, int, uint64)
}

func runTestSum[T any](t *testing.T, tc testSumCase[T]) {
	{
		l2 := tc.new()
		for k := range l2Size {
			tc.set(l2, k, 1)
		}
		assert.Equal(t, 64, int(tc.sum(l2)))
	}

	rng := mwc.Rand()

	for range 1000 {
		l2 := tc.new()
		var total uint64
		for k := range l2Size {
			v := rng.Uint64n(2 * l2GrowAt)
			total += v
			tc.set(l2, k, v)
		}
		assert.Equal(t, total, tc.sum(l2))
	}
}

func runBenchSum[T any](b *testing.B, tc testSumCase[T]) {
	l2 := tc.new()
	rng := mwc.Rand()

	for k := range l2Size {
		tc.set(l2, k, rng.Uint64n(2*l2GrowAt))
	}

	perfbench.Open(b)
	b.ReportAllocs()

	for b.Loop() {
		tc.sum(l2)
	}
}

func newLayer2Small() *layer2Small                    { return new(layer2Small) }
func setLayer2Small(l2 *layer2Small, i int, v uint64) { l2.cs[i] = uint32(v) }

func newLayer2Large() *layer2Large                    { return new(layer2Large) }
func setLayer2Large(l2 *layer2Large, i int, v uint64) { l2.cs[i] = v }

var (
	layer2SmallAVX2 = testSumCase[layer2Small]{
		new: newLayer2Small,
		sum: sumLayer2SmallAVX2,
		set: setLayer2Small,
	}

	layer2SmallFallback = testSumCase[layer2Small]{
		new: newLayer2Small,
		sum: sumLayer2SmallFallback,
		set: setLayer2Small,
	}

	layer2LargeAVX2 = testSumCase[layer2Large]{
		new: newLayer2Large,
		sum: sumLayer2LargeAVX2,
		set: setLayer2Large,
	}

	layer2LargeFallback = testSumCase[layer2Large]{
		new: newLayer2Large,
		sum: sumLayer2LargeFallback,
		set: setLayer2Large,
	}
)
