package level0

import (
	"testing"
	"time"

	"github.com/zeebo/assert"
	"github.com/zeebo/mwc"

	"github.com/histdb/histdb/testhelp"
)

func TestIterator(t *testing.T) {
	t.Run("Next", func(t *testing.T) {
		fs, cleanup := testhelp.FS(t)
		defer cleanup()

		l0, entries, cleanup := Level0(t, fs, 4, 4)
		defer cleanup()

		var it Iterator
		assert.NoError(t, l0.InitIterator(&it))

		for it.Next() {
			assert.Equal(t, entries[0].Key.String(), it.Key().String())
			assert.Equal(t, string(entries[0].Value), string(it.Value()))
			entries = entries[1:]
		}

		assert.NoError(t, it.Err())
		assert.Equal(t, len(entries), 0)
	})

	t.Run("Seek", func(t *testing.T) {
		fs, cleanup := testhelp.FS(t)
		defer cleanup()

		l0, entries, cleanup := Level0(t, fs, 0, 0)
		defer cleanup()

		var it Iterator
		assert.NoError(t, l0.InitIterator(&it))

		rng := mwc.Rand()

		for j := 0; j < 1000; j++ {
			i := int(rng.Uint32()) % len(entries)
			ent := entries[i]

			lt, gt := ent.Key, ent.Key
			lt[len(lt)-1]--
			gt[len(gt)-1]++

			assert.That(t, it.Seek(ent.Key))
			assert.Equal(t, it.Key(), ent.Key)
			assert.Equal(t, string(it.Value()), string(ent.Value))

			assert.That(t, it.Seek(lt))
			assert.Equal(t, it.Key(), ent.Key)
			assert.Equal(t, string(it.Value()), string(ent.Value))

			if i+1 < len(entries) {
				assert.That(t, it.Seek(gt))
				assert.Equal(t, it.Key(), entries[i+1].Key)
				assert.Equal(t, string(it.Value()), string(entries[i+1].Value))
			} else {
				assert.That(t, !it.Seek(gt))
			}
		}

		assert.NoError(t, it.Err())
	})

	count := func(it *Iterator) (n int) {
		for it.Next() {
			n++
		}
		return n
	}

	t.Run("Long", func(t *testing.T) {
		fs, cleanup := testhelp.FS(t)
		defer cleanup()

		l0, _, cleanup := Level0(t, fs, 0, 0)
		defer cleanup()

		var it Iterator
		assert.NoError(t, l0.InitIterator(&it))
		assert.Equal(t, count(&it), 65535)
	})

	t.Run("Short", func(t *testing.T) {
		fs, cleanup := testhelp.FS(t)
		defer cleanup()

		l0, _, cleanup := Level0(t, fs, 256, 256)
		defer cleanup()

		var it Iterator
		assert.NoError(t, l0.InitIterator(&it))
		assert.Equal(t, count(&it), 3855)
	})
}

func BenchmarkIterator(b *testing.B) {
	run := func(b *testing.B, nlen, vlen int) {
		b.Run("Next", func(b *testing.B) {
			fs, cleanup := testhelp.FS(b)
			defer cleanup()

			l0, _, cleanup := Level0(b, fs, nlen, vlen)
			defer cleanup()

			var it Iterator
			it.Init(l0.fh)

			now := time.Now()
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if !it.Next() {
					it.SeekFirst()
				}
				_, _, _ = it.Key(), it.Name(), it.Value()
				assert.NoError(b, it.Err())
			}

			b.StopTimer()
			b.ReportMetric(float64(b.N)/time.Since(now).Seconds(), "keys/sec")
		})

		b.Run("Seek", func(b *testing.B) {
			fs, cleanup := testhelp.FS(b)
			defer cleanup()

			l0, _, cleanup := Level0(b, fs, nlen, vlen)
			defer cleanup()

			var it Iterator
			it.Init(l0.fh)

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				it.Seek(testhelp.Key())
			}
		})
	}

	b.Run("Small", func(b *testing.B) { run(b, 0, 0) })
	b.Run("Large", func(b *testing.B) { run(b, 32, 512) })
}
