package floathist

import (
	"encoding/hex"
	"runtime"
	"testing"

	"github.com/zeebo/assert"
	"github.com/zeebo/mwc"

	"github.com/histdb/histdb/rwutils"
)

func TestSerialize(t *testing.T) {
	t.Run("WriteSingle", func(t *testing.T) {
		rng := mwc.Rand()
		var buf [13]byte
		var w rwutils.W

		for i := 0; i < 10000; i++ {
			v := rng.Float32()

			var h T
			h.Observe(v)

			WriteSingle(&buf, v)

			w.Init(w.Done().Reset())
			AppendTo(&h, &w)

			assert.Equal(t, buf[:], w.Done().Prefix())
		}

		t.Logf("%d\n%s", len(buf), hex.Dump(buf[:]))
	})

	t.Run("Write", func(t *testing.T) {
		rng := mwc.Rand()

		var h T
		for i := int64(0); i < 10000; i++ {
			r := float32(rng.Uint32n(1000) + 500)
			h.Observe(r)
		}

		var w rwutils.W
		AppendTo(&h, &w)
		data := w.Done().Prefix()
		t.Logf("%d\n%s", len(data), hex.Dump(data))
	})

	t.Run("Load", func(t *testing.T) {
		rng := mwc.Rand()

		var h1 T
		var h2 T

		for i := int64(0); i < 10000; i++ {
			r := float32(rng.Uint32n(1000) + 500)
			h1.Observe(r)
		}

		var w rwutils.W
		var r rwutils.R
		AppendTo(&h1, &w)

		for i := float64(1); i < 10; i++ {
			r.Init(w.Done().Reset())
			ReadFrom(&h2, &r)
			_, err := r.Done()
			assert.NoError(t, err)

			tot1, sum1, avg1, _ := h1.Summary()
			tot2, sum2, avg2, _ := h2.Summary()
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

		for i := 0; i < b.N; i++ {
			WriteSingle(&buf, rng.Float32())
		}
		runtime.KeepAlive(&buf)
	})

	b.Run("AppendTo", func(b *testing.B) {
		rng := mwc.Rand()

		var h T
		for i := int64(0); i < 100000; i++ {
			h.Observe(rng.Float32())
		}

		var w rwutils.W
		AppendTo(&h, &w)

		b.SetBytes(int64(w.Done().Pos()))
		b.ReportMetric(float64(w.Done().Pos()), "bytes")

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			w.Init(w.Done().Reset())
			AppendTo(&h, &w)
		}
	})

	b.Run("ReadFrom", func(b *testing.B) {
		rng := mwc.Rand()

		var h T
		for i := int64(0); i < 100000; i++ {
			h.Observe(rng.Float32())
		}

		var w rwutils.W
		AppendTo(&h, &w)

		b.SetBytes(int64(w.Done().Pos()))
		b.ReportMetric(float64(w.Done().Pos()), "bytes")

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			var r rwutils.R
			r.Init(w.Done().Reset())

			var h T
			ReadFrom(&h, &r)
		}
	})
}
