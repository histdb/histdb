package btree

import (
	"testing"
	"time"

	"github.com/zeebo/assert"
	"github.com/zeebo/lsm"
)

func TestAppend(t *testing.T) {
	const size = 1000000

	var bt T
	var key lsm.Key
	for i := 0; i < size; i++ {
		key.SetTimestamp(uint32(i))
		bt.Append(key, uint32(i)+1)
	}

	var prev lsm.Key
	count := size
	iter := bt.Iterator()
	for iter.Next() {
		assert.That(t, !lsm.KeyCmp.Less(iter.Key(), prev))
		assert.Equal(t, iter.Key().Timestamp()+1, iter.Value())
		prev = iter.Key()
		count--
	}
	assert.Equal(t, count, 0)
}

func TestDuplicates(t *testing.T) {
	var bt T
	var key lsm.Key

	bt.Append(key, 0)
	bt.Append(key, 1)
	bt.Append(key, 2)

	iter := bt.Iterator()
	for i := 0; iter.Next(); i++ {
		assert.Equal(t, key, iter.Key())
		assert.Equal(t, i, iter.Value())
	}
}

func BenchmarkAppend(b *testing.B) {
	run := func(b *testing.B, n int) {
		for i := 0; i < b.N; i++ {
			var bt T
			var key lsm.Key
			now := time.Now()
			for j := 0; j < n; j++ {
				key.SetTimestamp(uint32(j))
				bt.Append(key, 0)
			}
			b.ReportMetric(float64(time.Since(now))/float64(n), "ns/key")
			b.ReportMetric(float64(n)/time.Since(now).Seconds(), "keys/sec")
		}
	}

	b.Run("1e2", func(b *testing.B) { run(b, 1e2) })
	b.Run("1e3", func(b *testing.B) { run(b, 1e3) })
	b.Run("1e4", func(b *testing.B) { run(b, 1e4) })
	b.Run("1e5", func(b *testing.B) { run(b, 1e5) })
	b.Run("1e6", func(b *testing.B) { run(b, 1e6) })
}
