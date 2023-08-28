package query

import (
	"fmt"
	"os"
	"testing"

	"github.com/zeebo/assert"

	"github.com/histdb/histdb/buffer"
	"github.com/histdb/histdb/memindex"
	"github.com/histdb/histdb/rwutils"
)

func TestQuery(t *testing.T) {
	var idx memindex.T

	// idx.Add([]byte("foo=bar,bif=bar"))
	// idx.Add([]byte("foo=a,bif=a"))
	// idx.Add([]byte("foo=b,bif=c"))

	data, _ := os.ReadFile("../memindex/metrics.idx")
	var r rwutils.R
	r.Init(buffer.OfLen(data))

	memindex.ReadFrom(&idx, &r)
	_, err := r.Done()
	assert.NoError(t, err)

	// idx.TagValues([]byte(`inst=12XzWDW7Nb496enKo4epRmpQamMe3cw7G3TUuhPrkoqoLb76rHK,field=successes`), []byte(`name`), func(tag []byte) bool {
	// 	fmt.Println(string(tag))
	// 	return true
	// })

	q, err := Parse(b(`inst !~ 12X & name='(*Dir).Commit' & field=successes`))
	assert.NoError(t, err)

	fmt.Println(q.prog)
	fmt.Printf("%q\n", q.strs)
	fmt.Println(q.vals)

	bm, err := q.Eval(&idx)
	assert.NoError(t, err)

	// idx.MetricHashes(bm, func(u uint32, h histdb.Hash) bool {
	// 	t.Logf("%-10d %x %s", u, h, idx.SlowReverseMetricName(u))
	// 	return true
	// })
	t.Log(bm.GetCardinality())
}

func BenchmarkQuery(b *testing.B) {
	var idx memindex.T
	data, _ := os.ReadFile("../memindex/metrics.idx")
	var r rwutils.R
	r.Init(buffer.OfLen(data))
	memindex.ReadFrom(&idx, &r)
	_, err := r.Done()
	assert.NoError(b, err)

	q, err := Parse([]byte(`inst !~ 12X & name='(*Dir).Commit' & field=successes`))
	assert.NoError(b, err)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = q.Eval(&idx)
	}
}
