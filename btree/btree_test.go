package btree

import (
	"testing"
	"time"

	"github.com/zeebo/assert"
	"github.com/zeebo/lsm"
	"github.com/zeebo/lsm/testhelp"
)

func assertSorted(t testing.TB, count int, iter Iterator) {
	var prev lsm.Key
	for iter.Next() {
		assert.That(t, !lsm.KeyCmp.Less(iter.Key(), prev))
		assert.Equal(t, iter.Key().Timestamp(), iter.Value())
		prev = iter.Key()
		count--
	}
	assert.Equal(t, count, 0)
}

func TestInsert(t *testing.T) {
	var bt T

	for i := 0; i < 100000; i++ {
		key := testhelp.Key()
		bt.Insert(key, key.Timestamp())
	}

	assertSorted(t, 100000, bt.Iterator())
}

func TestAppend(t *testing.T) {
	var bt1, bt2 T
	for i := 0; i < 100000; i++ {
		key := testhelp.Key()
		bt1.Insert(key, key.Timestamp())
	}

	iter := bt1.Iterator()
	for iter.Next() {
		bt2.Append(iter.Key(), iter.Value())
	}

	assertSorted(t, 100000, bt2.Iterator())
}

func TestDuplicates(t *testing.T) {
	var bt T
	key := testhelp.Key()

	bt.Insert(key, 0)
	bt.Insert(key, 1)
	bt.Insert(key, 2)

	iter := bt.Iterator()
	for i := 0; iter.Next(); i++ {
		assert.Equal(t, key, iter.Key())
		assert.Equal(t, i, iter.Value())
	}
}

var benchmarkKeys []lsm.Key

func init() {
	benchmarkKeys = make([]lsm.Key, 1e6)
	for i := range benchmarkKeys {
		benchmarkKeys[i] = testhelp.Key()
	}
}

func BenchmarkInsert(b *testing.B) {
	run := func(b *testing.B, n int) {
		for i := 0; i < b.N; i++ {
			var bt T
			now := time.Now()
			for j := 0; j < n; j++ {
				bt.Insert(benchmarkKeys[j], 0)
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

func BenchmarkAppend(b *testing.B) {
	run := func(b *testing.B, n int) {
		for i := 0; i < b.N; i++ {
			var bt T
			now := time.Now()
			for j := 0; j < n; j++ {
				// invalid append usage, but sorted property isn't important for speed
				bt.Append(benchmarkKeys[j], 0)
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
