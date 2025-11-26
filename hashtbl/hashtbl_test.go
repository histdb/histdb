package hashtbl

import (
	"testing"
	"time"

	"github.com/zeebo/assert"
	"github.com/zeebo/mwc"

	"github.com/histdb/histdb/buffer"
	"github.com/histdb/histdb/num"
	"github.com/histdb/histdb/rwutils"
)

func TestTable(t *testing.T) {
	var tb T[num.U64, num.U32]
	const iters = 1e5

	_, ok := tb.Find(0)
	assert.That(t, !ok)

	rng := mwc.New(1, 1)
	for i := range int(iters) {
		_, ok := tb.Insert(num.U64(rng.Uint64()), num.U32(i))
		assert.That(t, ok)
	}

	rng = mwc.New(1, 1)
	for i := range int(iters) {
		n, ok := tb.Find(num.U64(rng.Uint64()))
		assert.That(t, ok)
		assert.Equal(t, i, n)
	}

	rng = mwc.New(1, 1)
	for i := range int(iters) {
		n, ok := tb.Insert(num.U64(rng.Uint64()), num.U32(i+1))
		assert.That(t, !ok)
		assert.Equal(t, i, n)
	}

	rng = mwc.New(1, 1)
	for i := range int(iters) {
		n, ok := tb.Find(num.U64(rng.Uint64()))
		assert.That(t, ok)
		assert.Equal(t, i, n)
	}
}

func TestTableIterate(t *testing.T) {
	var tb T[num.U64, num.U32]
	exp := make(map[num.U64]num.U32)
	for i := range uint64(1000) {
		_, ok := tb.Insert(num.U64(i), num.U32(i))
		exp[num.U64(i)] = num.U32(i)
		assert.That(t, ok)
	}

	got := make(map[num.U64]num.U32)
	tb.Iterate(func(k num.U64, v num.U32) bool {
		got[k] = v
		return true
	})

	assert.Equal(t, exp, got)
}

func TestTableSerialize(t *testing.T) {
	var tb T[num.U64, num.U32]

	for i := range uint64(1000) {
		val, ok := tb.Insert(num.U64(i), num.U32(i))
		assert.That(t, ok)
		assert.Equal(t, val, i)
	}

	var w rwutils.W
	AppendTo(&tb, &w)
	w.Uint8(1)
	w.Uint8(2)
	w.Uint8(3)

	var r rwutils.R
	r.Init(w.Done().Trim().Reset())

	var tb2 T[num.U64, num.U32]
	ReadFrom(&tb2, &r)

	rem, err := r.Done()
	assert.NoError(t, err)
	assert.Equal(t, rem.Suffix(), []byte{1, 2, 3})

	assert.Equal(t, tb, tb2)

	for i := range uint64(1000) {
		val, ok := tb2.Insert(num.U64(i), ^num.U32(0))
		assert.That(t, !ok)
		assert.Equal(t, val, i)
	}
}

func BenchmarkTable(b *testing.B) {
	run := func(b *testing.B, n int) {
		now := time.Now()
		rng := mwc.Rand()

		b.ReportAllocs()
		b.ResetTimer()

		var tb T[num.U64, num.U32]
		for b.Loop() {
			tb = T[num.U64, num.U32]{}
			for range n {
				tb.Insert(num.U64(rng.Uint64()), num.U32(0))
			}
		}

		b.ReportMetric(float64(time.Since(now))/float64(n)/float64(b.N), "ns/key")
		b.ReportMetric(float64(n)*float64(b.N)/time.Since(now).Seconds(), "keys/sec")
		b.ReportMetric(float64(tb.Size()), "b/table")
	}

	b.Run("1e1", func(b *testing.B) { run(b, 1e1) })
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

		for b.Loop() {
			tb := make(map[num.U64]uint32)

			for j := range n {
				tb[num.U64(rng.Uint64())] = uint32(j)
			}
		}

		b.ReportMetric(float64(time.Since(now))/float64(n)/float64(b.N), "ns/key")
		b.ReportMetric(float64(n)*float64(b.N)/time.Since(now).Seconds(), "keys/sec")
	}

	b.Run("1e1", func(b *testing.B) { run(b, 1e1) })
	b.Run("1e2", func(b *testing.B) { run(b, 1e2) })
	b.Run("1e3", func(b *testing.B) { run(b, 1e3) })
	b.Run("1e4", func(b *testing.B) { run(b, 1e4) })
	b.Run("1e5", func(b *testing.B) { run(b, 1e5) })
	b.Run("1e6", func(b *testing.B) { run(b, 1e6) })
	b.Run("1e7", func(b *testing.B) { run(b, 1e7) })
}

func BenchmarkTableSerialize(b *testing.B) {
	mk := func(n int) *T[num.U64, num.U32] {
		var tb T[num.U64, num.U32]
		for i := range n {
			tb.Insert(num.U64(i), num.U32(i))
		}
		return &tb
	}

	b.Run("AppendTo", func(b *testing.B) {
		run := func(b *testing.B, n int) {
			tmp := make([]byte, 0, 4096)
			tb := mk(n)

			var w rwutils.W
			w.Init(buffer.OfCap(tmp))
			AppendTo(tb, &w)

			now := time.Now()
			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				w.Init(w.Done())
				AppendTo(tb, &w)
			}

			b.ReportMetric(float64(time.Since(now))/float64(n)/float64(b.N), "ns/key")
			b.ReportMetric(float64(n)*float64(b.N)/time.Since(now).Seconds(), "keys/sec")
		}

		b.Run("1e1", func(b *testing.B) { run(b, 1e1) })
		b.Run("1e2", func(b *testing.B) { run(b, 1e2) })
		b.Run("1e3", func(b *testing.B) { run(b, 1e3) })
		b.Run("1e4", func(b *testing.B) { run(b, 1e4) })
		b.Run("1e5", func(b *testing.B) { run(b, 1e5) })
		b.Run("1e6", func(b *testing.B) { run(b, 1e6) })
		b.Run("1e7", func(b *testing.B) { run(b, 1e7) })
	})

	b.Run("ReadFrom", func(b *testing.B) {
		run := func(b *testing.B, n int) {
			tb := mk(n)

			var r rwutils.R
			var w rwutils.W
			AppendTo(tb, &w)

			now := time.Now()
			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				r.Init(w.Done().Reset())
				ReadFrom(tb, &r)
			}

			b.ReportMetric(float64(time.Since(now))/float64(n)/float64(b.N), "ns/key")
			b.ReportMetric(float64(n)*float64(b.N)/time.Since(now).Seconds(), "keys/sec")
		}

		b.Run("1e1", func(b *testing.B) { run(b, 1e1) })
		b.Run("1e2", func(b *testing.B) { run(b, 1e2) })
		b.Run("1e3", func(b *testing.B) { run(b, 1e3) })
		b.Run("1e4", func(b *testing.B) { run(b, 1e4) })
		b.Run("1e5", func(b *testing.B) { run(b, 1e5) })
		b.Run("1e6", func(b *testing.B) { run(b, 1e6) })
		b.Run("1e7", func(b *testing.B) { run(b, 1e7) })
	})
}
