package hashtbl

import (
	"bytes"
	"testing"
	"time"

	"github.com/zeebo/assert"
	"github.com/zeebo/mwc"

	"github.com/histdb/histdb/rwutils"
)

func TestTable(t *testing.T) {
	var tb T[U64, *U64]
	const iters = 1e6

	rng := mwc.New(1, 1)
	for i := 0; i < iters; i++ {
		_, ok := tb.Insert(U64(rng.Uint64()), uint32(i))
		assert.That(t, !ok)
	}

	rng = mwc.New(1, 1)
	for i := 0; i < iters; i++ {
		n, ok := tb.Find(U64(rng.Uint64()))
		assert.That(t, ok)
		assert.Equal(t, i, n)
	}

	rng = mwc.New(1, 1)
	for i := 0; i < iters; i++ {
		n, ok := tb.Insert(U64(rng.Uint64()), uint32(i+1))
		assert.That(t, ok)
		assert.Equal(t, i, n)
	}

	rng = mwc.New(1, 1)
	for i := 0; i < iters; i++ {
		n, ok := tb.Find(U64(rng.Uint64()))
		assert.That(t, ok)
		assert.Equal(t, i, n)
	}
}

func TestTableSerialize(t *testing.T) {
	var tb T[U64, *U64]

	for i := uint64(0); i < 1000; i++ {
		val, ok := tb.Insert(U64(i), uint32(i))
		assert.That(t, !ok)
		assert.Equal(t, val, i)
	}

	var buf bytes.Buffer
	var w rwutils.W
	w.Init(&buf, make([]byte, 4096))
	tb.AppendTo(&w)
	assert.NoError(t, w.Done())

	data := append(buf.Bytes(), 1, 2, 3)

	var tb2 T[U64, *U64]
	var r rwutils.R
	r.Init(data)
	tb2.ReadFrom(&r)
	rem, err := r.Done()
	assert.NoError(t, err)
	assert.Equal(t, rem, []byte{1, 2, 3})

	for i := uint64(0); i < 1000; i++ {
		val, ok := tb2.Insert(U64(i), ^uint32(0))
		assert.That(t, ok)
		assert.Equal(t, val, i)
	}
}

func BenchmarkTable(b *testing.B) {
	run := func(b *testing.B, n int) {
		now := time.Now()
		rng := mwc.Rand()

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			var tb T[U64, *U64]

			for j := 0; j < n; j++ {
				tb.Insert(U64(rng.Uint64()), uint32(j))
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
			tb := make(map[U64]uint32)

			for j := 0; j < n; j++ {
				tb[U64(rng.Uint64())] = uint32(j)
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

func BenchmarkTableSerialize(b *testing.B) {
	mk := func(n int) *T[U64, *U64] {
		var tb T[U64, *U64]
		for i := 0; i < n; i++ {
			tb.Insert(U64(i), uint32(i))
		}
		return &tb
	}

	b.Run("AppendTo", func(b *testing.B) {
		run := func(b *testing.B, n int) {
			buf := new(bytes.Buffer)
			tmp := make([]byte, 0, 4096)
			tb := mk(n)

			var w rwutils.W
			w.Init(buf, tmp)
			tb.AppendTo(&w)

			now := time.Now()
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				buf.Reset()
				w.Init(buf, tmp)
				tb.AppendTo(&w)
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
	})

	b.Run("ReadFrom", func(b *testing.B) {
		run := func(b *testing.B, n int) {
			buf := new(bytes.Buffer)
			tmp := make([]byte, 0, 4096)
			tb := mk(n)

			var w rwutils.W
			var r rwutils.R
			w.Init(buf, tmp)
			tb.AppendTo(&w)

			now := time.Now()
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				r.Init(buf.Bytes())
				tb.ReadFrom(&r)
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
	})
}