package petname

import (
	"testing"
	"time"

	"github.com/zeebo/pcg"
)

func TestTable(t *testing.T) {
	tb := newTable()

	t.Log(tb.insert(Hash{1, 2}, 3))
	t.Log(tb.insert(Hash{1, 2}, 4))
	t.Log(tb.insert(Hash{4, 5}, 6))
	t.Log(tb.insert(Hash{0, 2}, 7))

	t.Log(tb.find(Hash{1, 2}))
	t.Log(tb.find(Hash{2, 2}))
	t.Log(tb.find(Hash{4, 5}))
}

func BenchmarkTable(b *testing.B) {
	run := func(b *testing.B, n int) {
		now := time.Now()
		var rng pcg.T

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			tb := newTable()

			for j := 0; j < n; j++ {
				tb.insert(Hash{Lo: rng.Uint64()}, uint32(j))
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
		var rng pcg.T

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
