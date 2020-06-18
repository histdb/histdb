package level0

import (
	"testing"
	"time"

	"github.com/zeebo/assert"
	"github.com/zeebo/lsm/filesystem"
)

func TestIterator(t *testing.T) {
	t.Run("Next", func(t *testing.T) {
		l0, entries, cleanup := newLevel0(t, new(filesystem.T))
		defer cleanup()

		it, err := l0.Iterator()
		assert.NoError(t, err)

		for it.Next() {
			assert.Equal(t, entries[0].key, it.Key())
			assert.Equal(t, string(entries[0].value), string(it.Value()))
			entries = entries[1:]
		}

		assert.NoError(t, it.Err())
		assert.Equal(t, len(entries), 0)
	})

	t.Run("Seek", func(t *testing.T) {
		l0, entries, cleanup := newLevel0(t, new(filesystem.T))
		defer cleanup()

		it, err := l0.Iterator()
		assert.NoError(t, err)

		for i, ent := range entries {
			lt, gt := ent.key, ent.key
			lt[len(lt)-1]--
			gt[len(gt)-1]++

			it.Seek(ent.key)
			assert.That(t, it.Next())
			assert.Equal(t, it.Key(), ent.key)
			assert.Equal(t, string(it.Value()), string(ent.value))

			it.Seek(lt)
			assert.That(t, it.Next())
			assert.Equal(t, it.Key(), ent.key)
			assert.Equal(t, string(it.Value()), string(ent.value))

			if i+1 < len(entries) {
				it.Seek(gt)
				assert.That(t, it.Next())
				assert.Equal(t, it.Key(), entries[i+1].key)
				assert.Equal(t, string(it.Value()), string(entries[i+1].value))
			}
		}

		assert.NoError(t, it.Err())
	})
}

func BenchmarkIterator(b *testing.B) {
	b.Run("Next", func(b *testing.B) {
		l0, entries, cleanup := newLevel0(b, new(filesystem.T))
		defer cleanup()
		var it Iterator

		now := time.Now()
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			it.Init(l0.fh)
			for it.Next() {
				_, _ = it.Key(), it.Value()
			}
			assert.NoError(b, it.Err())
		}

		b.StopTimer()
		b.ReportMetric(float64(b.N*len(entries))/time.Since(now).Seconds(), "keys/sec")
		b.ReportMetric(float64(time.Since(now).Nanoseconds())/float64(len(entries)*b.N), "ns/key")
	})

	b.Run("Seek", func(b *testing.B) {
		l0, _, cleanup := newLevel0(b, new(filesystem.T))
		defer cleanup()

		var it Iterator
		it.Init(l0.fh)

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			it.Seek(newKey(b))
		}

		b.StopTimer()
		b.ReportMetric(float64(it.perf.read)/float64(b.N), "reads/op")
	})
}
