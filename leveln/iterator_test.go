package leveln

import (
	"testing"
	"time"

	"github.com/zeebo/assert"
	"github.com/zeebo/mwc"

	"github.com/histdb/histdb"
	"github.com/histdb/histdb/testhelp"
)

func TestLevelNWriter(t *testing.T) {
	fs, cleanup := testhelp.FS(t)
	defer cleanup()

	keys, cleanup := testhelp.Tempfile(t, fs)
	defer cleanup()

	values, cleanup := testhelp.Tempfile(t, fs)
	defer cleanup()

	metrics := createMetrics(100)

	var ln Writer
	ln.Init(keys, values)

	for _, metric := range metrics {
		var key histdb.Key
		*key.HashPtr() = metric.hash

		for ts := range uint32(100) {
			key.SetTimestamp(ts)
			assert.NoError(t, ln.Append(key, testhelp.Value(mwc.Intn(1000))))
		}
	}
	assert.NoError(t, ln.Finish())
}

func BenchmarkIterator(b *testing.B) {
	run := func(b *testing.B, n uint64) {

		fs, cleanup := testhelp.FS(b)
		defer cleanup()

		keys, cleanup := testhelp.Tempfile(b, fs)
		defer cleanup()

		values, cleanup := testhelp.Tempfile(b, fs)
		defer cleanup()

		start := time.Now()

		metrics := createMetrics(n)

		var lnw Writer
		lnw.Init(keys, values)

		for _, metric := range metrics {
			var key histdb.Key
			*key.HashPtr() = metric.hash
			assert.NoError(b, lnw.Append(key, nil))
		}
		assert.NoError(b, lnw.Finish())

		setup := time.Since(start).Seconds()

		var it Iterator
		it.Init(keys, values)

		vsize, _ := values.Size()
		ksize, _ := keys.Size()

		b.ReportAllocs()
		b.ResetTimer()

		for range b.N {
			key := testhelp.KeyFrom(mwc.Uint64(), 0, 0, 0)
			it.Seek(key)
		}

		b.ReportMetric(float64(it.stats.valueReads)/float64(b.N), "vreads/op")
		b.ReportMetric(float64(it.kr.stats.reads)/float64(b.N), "kreads/op")
		b.ReportMetric(setup, "sec/setup")
		b.ReportMetric(float64(vsize)/1024/1024, "mb/values")
		b.ReportMetric(float64(ksize)/1024/1024, "mb/keys")
	}

	b.Run("1e2", func(b *testing.B) { run(b, 1e2) })
	b.Run("1e3", func(b *testing.B) { run(b, 1e3) })
	b.Run("1e4", func(b *testing.B) { run(b, 1e4) })
	b.Run("1e5", func(b *testing.B) { run(b, 1e5) })
	b.Run("1e6", func(b *testing.B) { run(b, 1e6) })
}
