package level0

import (
	"testing"
	"time"

	"github.com/zeebo/assert"

	"github.com/histdb/histdb/testhelp"
)

func TestLevel0(t *testing.T) {
	t.Run("Append", func(t *testing.T) {
		fs, cleanup := testhelp.FS(t)
		defer cleanup()

		l0, _, cleanup := Level0(t, fs, 0, 0)
		defer cleanup()

		size, err := l0.fh.Size()
		assert.NoError(t, err)

		assert.Equal(t, size, L0Size)

		// TODO: some better checks
	})

	t.Run("Reopen", func(t *testing.T) {
		fs, cleanup := testhelp.FS(t)
		defer cleanup()

		fh, cleanup := testhelp.Tempfile(t, fs)
		defer cleanup()

		var l0 T
		l0.InitNew(fh)

		const maxEntries = l0DataSize / l0EntryAlignment

		for i := 0; i < maxEntries/2; i++ {
			ok, err := l0.Append(testhelp.KeyFrom(0, 0, uint32(i+1)), nil, nil)
			assert.NoError(t, err)
			assert.That(t, ok)
		}

		l0.InitCurrent(fh)

		for i := maxEntries / 2; i < maxEntries; i++ {
			ok, err := l0.Append(testhelp.KeyFrom(0, 0, uint32(i+1)), nil, nil)
			assert.NoError(t, err)
			assert.That(t, ok)
		}

		ok, err := l0.Append(testhelp.KeyFrom(0, 0, 1), nil, nil)
		assert.NoError(t, err)
		assert.That(t, !ok)
	})
}

func BenchmarkLevel0(b *testing.B) {
	run := func(b *testing.B, nlen, vlen int) {
		b.Run("AppendAll", func(b *testing.B) {
			fs, cleanup := testhelp.FS(b)
			defer cleanup()

			l0, entries, cleanup := Level0(b, fs, nlen, vlen)
			defer cleanup()

			now := time.Now()
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				assert.NoError(b, l0.InitNew(l0.fh))
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
			size, err := l0.fh.Size()
			assert.NoError(b, err)
			b.SetBytes(size)
		})

		b.Run("InitCurrent", func(b *testing.B) {
			fs, cleanup := testhelp.FS(b)
			defer cleanup()

			l0, entries, cleanup := Level0(b, fs, nlen, vlen)
			defer cleanup()

			now := time.Now()
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				assert.NoError(b, l0.InitCurrent(l0.fh))
			}

			b.StopTimer()
			b.ReportMetric(float64(len(entries)*b.N)/time.Since(now).Seconds(), "keys/sec")
			b.ReportMetric(float64(time.Since(now).Nanoseconds())/float64(len(entries)*b.N), "ns/key")
			size, err := l0.fh.Size()
			assert.NoError(b, err)
			b.SetBytes(size)
		})
	}

	b.Run("Small", func(b *testing.B) { run(b, 0, 0) })
	b.Run("Large", func(b *testing.B) { run(b, 32, 512) })
}
