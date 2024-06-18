package query

import (
	"testing"
	"time"

	"github.com/zeebo/assert"

	"github.com/histdb/histdb/memindex"
)

func TestQuery(t *testing.T) {
	var now time.Time
	var idx memindex.T

	idx.Add([]byte("foo=bar,bif=bar"), nil, nil)
	idx.Add([]byte("foo=a,bif=a"), nil, nil)
	idx.Add([]byte("foo=b,bif=c"), nil, nil)

	// data, _ := os.ReadFile("../memindex/metrics.idx")
	// var r rwutils.R
	// r.Init(buffer.OfLen(data))

	// now = time.Now()
	// memindex.ReadFrom(&idx, &r)
	// t.Log("metrics loaded in", time.Since(now))
	// _, err := r.Done()
	// assert.NoError(t, err)

	q := new(Q)
	err := Parse(b(`inst !* 12z & name='(*Dir).Commit' & field=successes`), q)
	assert.NoError(t, err)

	t.Log("prog:", q.prog)
	t.Logf("strs: %q", q.strs.list)
	t.Logf("mchs: %q", q.mchs)

	now = time.Now()
	bm := q.Eval(&idx)
	dur := time.Since(now)
	t.Log("query ran in", dur)
	assert.NoError(t, err)

	t.Log(bm.GetCardinality(), "matching metrics")
	t.Log(float64(bm.GetCardinality())/dur.Seconds(), "metrics/sec")

	// var name []byte
	// idx.MetricHashes(bm, func(u memindex.Id, h histdb.Hash) bool {
	// 	name, _ = idx.AppendMetricName(u, name[:0])
	// 	t.Logf("%-10d %x %s", u, h, name)
	// 	return true
	// })
}

func BenchmarkQuery(b *testing.B) {
	var idx memindex.T

	// data, _ := os.ReadFile("../memindex/metrics.idx")
	// var r rwutils.R
	// r.Init(buffer.OfLen(data))

	// memindex.ReadFrom(&idx, &r)
	// _, err := r.Done()
	// assert.NoError(b, err)

	q := new(Q)
	err := Parse([]byte(`inst !* 12z & name='(*Dir).Commit' & field=successes`), q)
	assert.NoError(b, err)

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		_ = q.Eval(&idx)
	}
}
