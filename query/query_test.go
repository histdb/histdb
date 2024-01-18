package query

import (
	"testing"
	"time"

	"github.com/zeebo/assert"

	"github.com/histdb/histdb"
	"github.com/histdb/histdb/memindex"
)

func TestQuery(t *testing.T) {
	var now time.Time
	var idx memindex.T

	idx.Add([]byte("foo=bar,bif=bar"))
	idx.Add([]byte("foo=a,bif=a"))
	idx.Add([]byte("foo=b,bif=c"))

	// data, _ := os.ReadFile("../memindex/metrics.idx")
	// var r rwutils.R
	// r.Init(buffer.OfLen(data))

	// now = time.Now()
	// memindex.ReadFrom(&idx, &r)
	// t.Log("metrics loaded in", time.Since(now))
	// _, err := r.Done()
	// assert.NoError(t, err)

	q, err := Parse(b(`inst !* 12z & name='(*Dir).Commit' & field=successes`))
	assert.NoError(t, err)

	t.Log("prog:", q.prog)
	t.Logf("strs: %q", q.strs)
	t.Log("vals:", q.vals)
	t.Logf("mchs: %q", q.mchs)

	now = time.Now()
	bm, err := q.Eval(&idx)
	dur := time.Since(now)
	t.Log("query ran in", dur)
	assert.NoError(t, err)

	t.Log(bm.GetCardinality(), "matching metrics")
	t.Log(float64(bm.GetCardinality())/dur.Seconds(), "metrics/sec")

	var name []byte
	idx.MetricHashes(bm, func(u uint64, h histdb.Hash) bool {
		name, _ = idx.AppendMetricName(u, name[:0])
		t.Logf("%-10d %x %s", u, h, name)
		return true
	})
}

func BenchmarkQuery(b *testing.B) {
	var idx memindex.T

	// data, _ := os.ReadFile("../memindex/metrics.idx")
	// var r rwutils.R
	// r.Init(buffer.OfLen(data))

	// memindex.ReadFrom(&idx, &r)
	// _, err := r.Done()
	// assert.NoError(b, err)

	q, err := Parse([]byte(`inst !* 12z & name='(*Dir).Commit' & field=successes`))
	assert.NoError(b, err)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = q.Eval(&idx)
	}
}
