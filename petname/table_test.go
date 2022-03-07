package petname

import (
	"testing"
	"time"

	"github.com/zeebo/assert"
	"github.com/zeebo/mwc"
)

func TestTable(t *testing.T) {
	tb := newTable()
	const iters = 1e6

	rng := mwc.New(1, 1)
	for i := 0; i < iters; i++ {
		_, ok := tb.Insert(Hash{Lo: rng.Uint64()}, uint32(i))
		assert.That(t, !ok)
	}

	rng = mwc.New(1, 1)
	for i := 0; i < iters; i++ {
		n, ok := tb.Find(Hash{Lo: rng.Uint64()})
		assert.That(t, ok)
		assert.Equal(t, i, n)
	}

	rng = mwc.New(1, 1)
	for i := 0; i < iters; i++ {
		n, ok := tb.Insert(Hash{Lo: rng.Uint64()}, uint32(i+1))
		assert.That(t, ok)
		assert.Equal(t, i, n)
	}

	rng = mwc.New(1, 1)
	for i := 0; i < iters; i++ {
		n, ok := tb.Find(Hash{Lo: rng.Uint64()})
		assert.That(t, ok)
		assert.Equal(t, i, n)
	}
}

func BenchmarkTable(b *testing.B) {
	run := func(b *testing.B, n int) {
		now := time.Now()
		rng := mwc.Rand()

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			tb := newTable()

			for j := 0; j < n; j++ {
				tb.Insert(Hash{Lo: rng.Uint64()}, uint32(j))
			}
		}

		b.ReportMetric(float64(time.Since(now))/float64(n)/float64(b.N), "ns/key")
		b.ReportMetric(float64(n)*float64(b.N)/time.Since(now).Seconds(), "keys/sec")
	}

	b.Run("1e2", func(b *testing.B) { run(b, 1e2) })
	b.Run("1e3", func(b *testing.B) { run(b, 1e3) })
	b.Run("1e4", func(b *testing.B) { run(b, 1e4) })
	b.Run("1e5", func(b *testing.B) { run(b, 1e5) })
	b.Run("1e6", func(b *testing.B) { run(b, 1e6) })
	b.Run("1e7", func(b *testing.B) { run(b, 1e7) })
}

func BenchmarkStdlib(b *testing.B) {
	run := func(b *testing.B, n int) {
		now := time.Now()
		rng := mwc.Rand()

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			tb := make(map[Hash]uint32)

			for j := 0; j < n; j++ {
				tb[Hash{Lo: rng.Uint64()}] = uint32(j)
			}
		}

		b.ReportMetric(float64(time.Since(now))/float64(n)/float64(b.N), "ns/key")
		b.ReportMetric(float64(n)*float64(b.N)/time.Since(now).Seconds(), "keys/sec")
	}

	b.Run("1e2", func(b *testing.B) { run(b, 1e2) })
	b.Run("1e3", func(b *testing.B) { run(b, 1e3) })
	b.Run("1e4", func(b *testing.B) { run(b, 1e4) })
	b.Run("1e5", func(b *testing.B) { run(b, 1e5) })
	b.Run("1e6", func(b *testing.B) { run(b, 1e6) })
	b.Run("1e7", func(b *testing.B) { run(b, 1e7) })
}
