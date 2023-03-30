package level0

import (
	"math/rand"
	"testing"
	"time"

	"github.com/zeebo/assert"

	"github.com/histdb/histdb"
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
		assert.NoError(t, l0.Init(fh, nil))

		const maxEntries = L0DataSize / l0EntryAlignment

		for i := uint32(0); i < maxEntries/2; i++ {
			ok, err := l0.Append(testhelp.KeyFrom(0, 0, i+1), nil, nil)
			assert.NoError(t, err)
			assert.That(t, ok)
		}

		var ts uint32
		assert.NoError(t, l0.Init(fh, func(key histdb.Key, name, value []byte) {
			ts = key.Timestamp()
		}))

		for i := ts; i < maxEntries-1; i++ {
			ok, err := l0.Append(testhelp.KeyFrom(0, 0, i+1), nil, nil)
			assert.NoError(t, err)
			assert.That(t, ok)
		}

		ok, err := l0.Append(testhelp.KeyFrom(0, 0, 1), nil, nil)
		assert.NoError(t, err)
		assert.That(t, !ok)

		var it Iterator
		l0.InitIterator(&it)
		for i := uint32(0); it.Next(); i++ {
			assert.Equal(t, it.Key().Timestamp(), i+1)
		}
		assert.NoError(t, it.Err())
	})
}

func BenchmarkLevel0(b *testing.B) {
	run := func(b *testing.B, nlen, vlen int) {
		b.Run("AppendAll", func(b *testing.B) {
			fs, cleanup := testhelp.FS(b)
			defer cleanup()

			l0, entries, cleanup := Level0(b, fs, nlen, vlen)
			defer cleanup()
			rand.Shuffle(len(entries), func(i, j int) {
				entries[i], entries[j] = entries[j], entries[i]
			})

			now := time.Now()
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				assert.NoError(b, l0.fh.Truncate(0))
				assert.NoError(b, l0.Init(l0.fh, func(key histdb.Key, name, value []byte) {
					b.Fatal("got a key")
				}))
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

		b.Run("Init", func(b *testing.B) {
			fs, cleanup := testhelp.FS(b)
			defer cleanup()

			l0, _, cleanup := Level0(b, fs, nlen, vlen)
			defer cleanup()

			keys := 0
			now := time.Now()
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				assert.NoError(b, l0.Init(l0.fh, func(key histdb.Key, name, value []byte) {
					keys++
				}))
			}

			b.StopTimer()
			b.ReportMetric(float64(keys)/time.Since(now).Seconds(), "keys/sec")
			b.ReportMetric(float64(time.Since(now).Nanoseconds())/float64(keys), "ns/key")
			size, err := l0.fh.Size()
			assert.NoError(b, err)
			b.SetBytes(size)
		})
	}

	b.Run("Small", func(b *testing.B) { run(b, 0, 0) })
	b.Run("Large", func(b *testing.B) { run(b, 32, 512) })
}
