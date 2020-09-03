package level0

import (
	"testing"
	"time"

	"github.com/zeebo/assert"
	"github.com/zeebo/lsm/filesystem"
	"github.com/zeebo/lsm/testhelp"
	"github.com/zeebo/pcg"
)

func TestIterator(t *testing.T) {
	t.Run("Next", func(t *testing.T) {
		l0, entries, cleanup := Level0(t, new(filesystem.T))
		defer cleanup()

		it, err := l0.Iterator()
		assert.NoError(t, err)

		for it.Next() {
			assert.Equal(t, entries[0].Key, it.Key())
			assert.Equal(t, string(entries[0].Value), string(it.Value()))
			entries = entries[1:]
		}

		assert.NoError(t, it.Err())
		assert.Equal(t, len(entries), 0)
	})

	t.Run("Seek", func(t *testing.T) {
		l0, entries, cleanup := Level0(t, new(filesystem.T))
		defer cleanup()

		it, err := l0.Iterator()
		assert.NoError(t, err)

		for j := 0; j < 1000; j++ {
			i := int(pcg.Uint32()) % len(entries)
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
}

func BenchmarkIterator(b *testing.B) {
	b.Run("Next", func(b *testing.B) {
		l0, entries, cleanup := Level0(b, new(filesystem.T))
		defer cleanup()
		var it Iterator

		now := time.Now()
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			it.Init(l0.fh)
			for it.Next() {
				_, _, _ = it.Key(), it.Name(), it.Value()
			}
			assert.NoError(b, it.Err())
		}

		b.StopTimer()
		b.ReportMetric(float64(b.N*len(entries))/time.Since(now).Seconds(), "keys/sec")
		b.ReportMetric(float64(time.Since(now).Nanoseconds())/float64(len(entries)*b.N), "ns/key")
	})

	b.Run("Seek", func(b *testing.B) {
		l0, _, cleanup := Level0(b, new(filesystem.T))
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
