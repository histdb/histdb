package level0

import (
	"io"
	"io/ioutil"
	"testing"
	"time"

	"github.com/zeebo/assert"
	"github.com/zeebo/lsm/filesystem"
)

func TestLevel0(t *testing.T) {
	t.Run("Append", func(t *testing.T) {
		l0, _, cleanup := Level0(t, filesystem.Temp, 8, 8)
		defer cleanup()

		_, err := l0.fh.Seek(0, io.SeekStart)
		assert.NoError(t, err)

		data, err := ioutil.ReadAll(l0.fh)
		assert.NoError(t, err)

		assert.Equal(t, len(data), l0DataSize+l0IndexSize)

		// TODO: some better checks
	})
}

func BenchmarkLevel0(b *testing.B) {
	run := func(b *testing.B, nlen, vlen int) {
		l0, entries, cleanup := Level0(b, filesystem.Temp, nlen, vlen)
		defer cleanup()

		now := time.Now()
		b.SetBytes(l0DataSize + l0IndexSize)
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			assert.NoError(b, l0.Init(l0.fh))
			for _, ent := range entries {
				_, err := l0.Append(ent.Key, ent.Name, ent.Value)
				assert.NoError(b, err)
			}
			ok, err := l0.Append(entries[0].Key, entries[0].Name, entries[0].Value)
			assert.NoError(b, err)
			assert.That(b, !ok)
		}

		b.StopTimer()
		b.ReportMetric(float64(len(entries)*b.N)/time.Since(now).Seconds(), "keys/sec")
		b.ReportMetric(float64(time.Since(now).Nanoseconds())/float64(len(entries)*b.N), "ns/key")
	}

	b.Run("AppendAll", func(b *testing.B) {
		b.Run("Small", func(b *testing.B) { run(b, 0, 0) })
		b.Run("Large", func(b *testing.B) { run(b, 32, 512) })
	})
}
