package store

import (
	"testing"

	"github.com/aclements/go-perfevent/perfbench"
	"github.com/zeebo/assert"
	"github.com/zeebo/mwc"

	"github.com/histdb/histdb"
	"github.com/histdb/histdb/flathist"
	"github.com/histdb/histdb/query"
	"github.com/histdb/histdb/testhelp"
)

func TestStore(t *testing.T) {
	const (
		numMetrics      = 1000
		numObservations = 10
	)

	fs, cleanup := testhelp.FS(t)
	defer cleanup()

	var st T
	var q query.Q
	var m []byte

	assert.NoError(t, st.Init(fs, Config{}))
	defer st.Close()

	for range numMetrics {
		m = testhelp.Metric(5)
		for range numObservations {
			st.Observe(m, mwc.Float32())
		}
	}

	assert.NoError(t, query.Parse(m, &q))
	assert.NoError(t, st.WriteLevel(1000, 1))

	called := false
	ok, err := st.QueryData(&q, 0, func(key histdb.Key, name []byte, st *flathist.S, h flathist.H) bool {
		assert.Equal(t, int(st.Total(h)), numObservations)
		called = true
		return true
	})
	assert.NoError(t, err)
	assert.That(t, ok)
	assert.That(t, called)
}

func TestStore_Compact(t *testing.T) {
	const (
		numMetrics      = 1000
		numLevels       = 10
		numObservations = 10
	)

	fs, cleanup := testhelp.FS(t)
	defer cleanup()

	var st T
	var m []byte
	var q query.Q

	assert.NoError(t, st.Init(fs, Config{}))
	defer st.Close()

	for i := range numMetrics {
		m = append(testhelp.Metric(5), ",zzz=1"...)
		for range numObservations {
			st.Observe(m, mwc.Float32())
		}
		if i%(numMetrics/numLevels) == numMetrics/numLevels-1 {
			assert.NoError(t, st.WriteLevel(uint32(i+1), 1))
			assert.NoError(t, st.CompactSuffix())
		}
	}

	assert.NoError(t, query.Parse([]byte("{zzz|}"), &q))

	called := 0
	ok, err := st.QueryData(&q, 0, func(key histdb.Key, name []byte, st *flathist.S, h flathist.H) bool {
		assert.Equal(t, int(st.Total(h)), numObservations)
		called++
		return true
	})
	assert.NoError(t, err)
	assert.That(t, ok)
	assert.Equal(t, called, numMetrics)
}

func BenchmarkStore_Query(b *testing.B) {
	const (
		numMetrics = 10000
		numLevels  = 10
	)

	fs, cleanup := testhelp.FS(b)
	defer cleanup()

	var st T
	var m []byte
	var qAll query.Q
	var qOne query.Q

	assert.NoError(b, st.Init(fs, Config{}))
	defer st.Close()

	for i := range numMetrics {
		m = append(testhelp.Metric(5), ",zzz=1"...)
		for range 10 {
			st.Observe(m, mwc.Float32())
		}
		if i%(numMetrics/numLevels) == numMetrics/numLevels-1 {
			assert.NoError(b, st.WriteLevel(uint32(i+1), 1))
		}
	}

	assert.NoError(b, query.Parse([]byte("{zzz|}"), &qAll))
	assert.NoError(b, query.Parse(m, &qOne))
	b.ResetTimer()

	b.Run("One", func(b *testing.B) {
		perfbench.Open(b)
		b.ReportAllocs()
		b.ResetTimer()

		for range b.N {
			calls := 0
			st.QueryData(&qOne, 0, func(key histdb.Key, name []byte, st *flathist.S, h flathist.H) bool {
				calls++
				return true
			})
			assert.Equal(b, calls, 1)
		}
	})

	b.Run("All", func(b *testing.B) {
		perfbench.Open(b)
		b.ReportAllocs()
		b.ResetTimer()

		for range b.N {
			calls := 0
			st.QueryData(&qAll, 0, func(key histdb.Key, name []byte, st *flathist.S, h flathist.H) bool {
				calls++
				return true
			})
			assert.Equal(b, calls, numMetrics)
		}
	})
}
