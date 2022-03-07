package floathist

import (
	"encoding/hex"
	"testing"

	"github.com/zeebo/assert"
	"github.com/zeebo/mwc"
)

func TestSerialize(t *testing.T) {
	t.Run("Write", func(t *testing.T) {
		rng := mwc.Rand()

		h := new(Histogram)
		for i := int64(0); i < 10000; i++ {
			r := float32(rng.Uint32n(1000) + 500)
			h.Observe(r)
		}

		data := h.Serialize(nil)
		t.Logf("%d\n%s", len(data), hex.Dump(data))
	})

	t.Run("Load", func(t *testing.T) {
		rng := mwc.Rand()

		h1 := new(Histogram)
		h2 := new(Histogram)

		for i := int64(0); i < 10000; i++ {
			r := float32(rng.Uint32n(1000) + 500)
			h1.Observe(r)
		}
		buf := h1.Serialize(nil)

		for i := float64(1); i < 10; i++ {
			assert.NoError(t, h2.Load(buf))

			tot1, sum1, avg1, _ := h1.Summary()
			tot2, sum2, avg2, _ := h2.Summary()
			assert.Equal(t, i*tot1, tot2)
			assert.Equal(t, i*sum1, sum2)
			assert.Equal(t, avg1, avg2)
		}
	})
}

func BenchmarkSerialize(b *testing.B) {
	b.Run("Write", func(b *testing.B) {
		rng := mwc.Rand()

		h := new(Histogram)
		for i := int64(0); i < 100000; i++ {
			h.Observe(rng.Float32())
		}
		buf := h.Serialize(nil)

		b.SetBytes(int64(len(buf)))
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			h.Serialize(buf[:0])
		}

		b.ReportMetric(float64(len(buf)), "bytes")
	})

	b.Run("Load", func(b *testing.B) {
		rng := mwc.Rand()

		h := new(Histogram)
		for i := int64(0); i < 100000; i++ {
			h.Observe(rng.Float32())
		}
		buf := h.Serialize(nil)

		b.SetBytes(int64(len(buf)))
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			var h Histogram
			_ = h.Load(buf)
		}

		b.ReportMetric(float64(len(buf)), "bytes")
	})
}
