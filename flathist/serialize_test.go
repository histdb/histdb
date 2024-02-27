package flathist

import (
	"encoding/hex"
	"runtime"
	"testing"

	"github.com/aclements/go-perfevent/perfbench"
	"github.com/zeebo/assert"
	"github.com/zeebo/mwc"

	"github.com/histdb/histdb/rwutils"
)

func TestSerialize(t *testing.T) {
	t.Run("WriteSingle", func(t *testing.T) {
		rng := mwc.Rand()
		var buf [13]byte
		var w rwutils.W
		var s Store

		for i := 0; i < 10000; i++ {
			v := rng.Float32()

			h := s.New()
			s.Observe(h, v)

			WriteSingle(&buf, v)

			w.Init(w.Done().Reset())
			AppendTo(&s, h, &w)

			assert.Equal(t, buf[:], w.Done().Prefix())
		}

		t.Logf("%d\n%s", len(buf), hex.Dump(buf[:]))
	})

	t.Run("Write", func(t *testing.T) {
		rng := mwc.Rand()

		var s Store
		h := s.New()
		for i := int64(0); i < 10000; i++ {
			r := float32(rng.Uint32n(1000) + 500)
			s.Observe(h, r)
		}

		var w rwutils.W
		AppendTo(&s, h, &w)
		data := w.Done().Prefix()
		t.Logf("%d\n%s", len(data), hex.Dump(data))
	})

	t.Run("Load", func(t *testing.T) {
		rng := mwc.Rand()

		var s Store
		h1 := s.New()
		h2 := s.New()

		for i := int64(0); i < 10000; i++ {
			r := float32(rng.Uint32n(1000) + 500)
			s.Observe(h1, r)
		}

		var w rwutils.W
		var r rwutils.R
		AppendTo(&s, h1, &w)

		for i := float64(1); i < 10; i++ {
			r.Init(w.Done().Reset())
			ReadFrom(&s, h2, &r)
			_, err := r.Done()
			assert.NoError(t, err)

			tot1, sum1, avg1, _ := s.Summary(h1)
			tot2, sum2, avg2, _ := s.Summary(h2)
			assert.Equal(t, i*tot1, tot2)
			assert.Equal(t, i*sum1, sum2)
			assert.Equal(t, avg1, avg2)
		}
	})
}

func BenchmarkSerialize(b *testing.B) {
	b.Run("WriteSingle", func(b *testing.B) {
		rng := mwc.Rand()
		var buf [13]byte

		perfbench.Open(b)
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			WriteSingle(&buf, rng.Float32())
		}
		runtime.KeepAlive(&buf)
	})

	b.Run("AppendTo", func(b *testing.B) {
		rng := mwc.Rand()

		var s Store
		h := s.New()
		for i := int64(0); i < 100000; i++ {
			s.Observe(h, rng.Float32())
		}

		var w rwutils.W
		AppendTo(&s, h, &w)

		b.SetBytes(int64(w.Done().Pos()))
		b.ReportMetric(float64(w.Done().Pos()), "bytes")

		perfbench.Open(b)
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			w.Init(w.Done().Reset())
			AppendTo(&s, h, &w)
		}
	})

	b.Run("ReadFrom", func(b *testing.B) {
		rng := mwc.Rand()

		var s Store
		h := s.New()
		for i := int64(0); i < 100000; i++ {
			s.Observe(h, rng.Float32())
		}

		var w rwutils.W
		AppendTo(&s, h, &w)

		b.SetBytes(int64(w.Done().Pos()))
		b.ReportMetric(float64(w.Done().Pos()), "bytes")

		perfbench.Open(b)
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			var r rwutils.R
			r.Init(w.Done().Reset())

			ReadFrom(&s, h, &r)
		}
	})
}
