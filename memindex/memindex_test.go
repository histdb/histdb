package memindex

import (
	"fmt"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/zeebo/assert"

	"github.com/histdb/histdb"
	"github.com/histdb/histdb/buffer"
	"github.com/histdb/histdb/metrics"
	"github.com/histdb/histdb/rwutils"
	"github.com/histdb/histdb/testhelp"
)

func bs(s string) []byte { return []byte(s) }

func TestMemindex(t *testing.T) {
	h := func(x histdb.Hash, _ bool) histdb.Hash { return x }
	b := func(_ histdb.Hash, x bool) bool { return x }

	t.Run("Add", func(t *testing.T) {
		var idx T

		for i := 0; i < 1e5; i++ {
			idx.Add(testhelp.Name(3))
		}
	})

	t.Run("Duplicate Tags", func(t *testing.T) {
		var idx T

		assert.That(t, b(idx.Add(bs("foo=bar"))))
		assert.That(t, !b(idx.Add(bs("foo=bar"))))
		assert.That(t, !b(idx.Add(bs("foo=bar,foo=bar"))))
		assert.That(t, !b(idx.Add(bs("foo=bar,foo=baz"))))
	})

	t.Run("Empty Value", func(t *testing.T) {
		var idx T

		assert.That(t, b(idx.Add(bs("foo=bar,baz"))))
		assert.That(t, b(idx.Add(bs("bif"))))
		assert.That(t, b(idx.Add(bs("baz"))))
		assert.That(t, !b(idx.Add(bs("baz="))))
	})

	var vals []string
	collect := func() func(x []byte) bool {
		vals = vals[:0]
		return func(x []byte) bool { vals = append(vals, string(x)); return true }
	}

	t.Run("TagKeys", func(t *testing.T) {
		var idx T

		assert.That(t, b(idx.Add(bs("k0=v0,k1=v1,k2=v2"))))
		assert.That(t, b(idx.Add(bs("k0=v0,foo"))))
		assert.That(t, b(idx.Add(bs("k1=v1,foo,baz"))))
		assert.That(t, b(idx.Add(bs("k0=v1,bar"))))

		idx.TagKeys(bs("k0=v0"), collect())
		assert.DeepEqual(t, vals, []string{"k1", "k2", "foo"})

		idx.TagKeys(bs("k0"), collect())
		assert.DeepEqual(t, vals, []string{"k1", "k2", "foo", "bar"})

		idx.TagKeys(bs("k0="), collect())
		assert.DeepEqual(t, vals, []string{})

		idx.TagKeys(bs("k0=v0,k1=v0"), collect())
		assert.DeepEqual(t, vals, []string{})

		idx.TagKeys(bs("k0=v0,k1=v1"), collect())
		assert.DeepEqual(t, vals, []string{"k2"})
	})

	t.Run("TagValues", func(t *testing.T) {
		var idx T

		assert.That(t, b(idx.Add(bs("k0=v0,k1=va,k2=v2"))))
		assert.That(t, b(idx.Add(bs("k0=v0,k1=vb,k2=v3"))))
		assert.That(t, b(idx.Add(bs("k0=v0,k2=v4"))))
		assert.That(t, b(idx.Add(bs("k1=va,k2=v4"))))
		assert.That(t, b(idx.Add(bs("k1=vb,k2=v4"))))
		assert.That(t, b(idx.Add(bs("k0=v1,k2=v5"))))
		assert.That(t, b(idx.Add(bs("k3=vx,k2=v6"))))

		idx.TagValues(bs("k0=v0"), bs("k2"), collect())
		assert.DeepEqual(t, vals, []string{"v2", "v3", "v4"})

		idx.TagValues(bs("k0"), bs("k2"), collect())
		assert.DeepEqual(t, vals, []string{"v2", "v3", "v4", "v5"})

		idx.TagValues(bs(""), bs("k2"), collect())
		assert.DeepEqual(t, vals, []string{"v2", "v3", "v4", "v5", "v6"})

		idx.TagValues(bs("k0=v0,k1=va"), bs("k2"), collect())
		assert.DeepEqual(t, vals, []string{"v2"})

		idx.TagValues(bs("k0=v0,k1=vb"), bs("k2"), collect())
		assert.DeepEqual(t, vals, []string{"v3"})
	})

	t.Run("Hash", func(t *testing.T) {
		var idx T

		assert.Equal(t, h(idx.Add(bs("k0=v0"))), metrics.Hash(bs("k0=v0")))
		assert.Equal(t, h(idx.Add(bs("k0=v0,k1=v1"))), metrics.Hash(bs("k0=v0,k1=v1")))
		assert.Equal(t, h(idx.Add(bs("k0=v0,k1=v1"))), metrics.Hash(bs("k0=v0,k1=v1")))
		assert.NotEqual(t, h(idx.Add(bs("k0=v0,k1=v1"))), metrics.Hash(bs("k0=v0,k1=v2")))
		assert.Equal(t, h(idx.Add(bs("k0=v0,k0=v1"))), metrics.Hash(bs("k0=v0,k0=v1")))
		assert.Equal(t, h(idx.Add(bs("k0=v0,k0=v1"))), metrics.Hash(bs("k0=v0")))
		assert.Equal(t, h(idx.Add(bs("k0=v0"))), metrics.Hash(bs("k0=v0,k0=v1")))
	})

	t.Run("Metrics", func(t *testing.T) {
		var idx T

		h0, _ := idx.Add(bs("k0=v0a,k1=v1a,k2=v2a"))
		h1, _ := idx.Add(bs("k0=v0b,k1=v1b,k2=v2b"))
		h2, _ := idx.Add(bs("k0=v0b,k1=v1b"))
		h3, _ := idx.Add(bs("k0=v0c,k1=v1c,k2=v2a"))
		h4, _ := idx.Add(bs("k0=v0c,k1=v1c,k2=v2b"))
		h5, _ := idx.Add(bs("k0=v0c,k1=v1c,k2=v2c"))

		exp := []histdb.Hash{h0, h1, h2, h3, h4, h5}
		got := []histdb.Hash{}

		idx.Metrics(bs("k0,k1"), func(metrics *roaring.Bitmap, tags [][]byte) bool {
			t.Log(tags, metrics)
			idx.MetricHashes(metrics, func(h histdb.Hash) bool {
				t.Logf("\t%032x", h)
				got = append(got, h)
				return true
			})
			return true
		})

		sort.Slice(exp, func(i, j int) bool { return string(exp[i][:]) < string(exp[j][:]) })
		sort.Slice(got, func(i, j int) bool { return string(got[i][:]) < string(got[j][:]) })

		assert.DeepEqual(t, got, exp)
	})

	t.Run("Serialize", func(t *testing.T) {
		var idx T
		loadRandom(&idx)

		var w rwutils.W
		idx.AppendTo(&w)

		var r rwutils.R
		r.Init(w.Done().Trim().Reset())

		var idx2 T
		idx2.ReadFrom(&r)
		_, err := r.Done()
		assert.NoError(t, err)

		assert.Equal(t, idx.card, idx2.card)

		assert.Equal(t, idx.metrics, idx2.metrics)
		assert.Equal(t, idx.tag_names, idx2.tag_names)
		assert.Equal(t, idx.tkey_names, idx2.tkey_names)

		equalBitmaps := func(a, b []*roaring.Bitmap) {
			assert.Equal(t, len(a), len(b))
			for i := range a {
				assert.That(t, a[i].Equals(b[i]))
			}
		}

		equalBitmaps(idx.tag_to_metrics, idx2.tag_to_metrics)
		equalBitmaps(idx.tag_to_tkeys, idx2.tag_to_tkeys)
		equalBitmaps(idx.tag_to_tags, idx2.tag_to_tags)
		equalBitmaps(idx.tkey_to_metrics, idx2.tkey_to_metrics)
		equalBitmaps(idx.tkey_to_tkeys, idx2.tkey_to_tkeys)
		equalBitmaps(idx.tkey_to_tags, idx2.tkey_to_tags)
		equalBitmaps(idx.tkey_to_tvals, idx2.tkey_to_tvals)
	})
}

func BenchmarkMemindex(b *testing.B) {
	data, _ := os.ReadFile("metrics.idx")
	var r rwutils.R
	r.Init(buffer.OfLen(data))

	var idx T
	idx.ReadFrom(&r)
	_, err := r.Done()
	assert.NoError(b, err)

	dumpSizeStats(b, &idx)

	var (
		query  = bs("app=storagenode-release,inst=12XzWDW7Nb496enKo4epRmpQamMe3cw7G3TUuhPrkoqoLb76rHK")
		tkey   = bs("name")
		mquery = bs(string(query) + "," + string(tkey))
	)

	b.Run("TagKeys", func(b *testing.B) {
		b.ReportAllocs()
		count := 0
		start := time.Now()
		for i := 0; i < b.N; i++ {
			idx.TagKeys(query, func([]byte) bool { count++; return true })
		}
		b.ReportMetric(float64(count)/time.Since(start).Seconds()/1e6, "Mk/sec")
		b.ReportMetric(float64(count)/float64(b.N), "k/query")
	})

	b.Run("TagValues", func(b *testing.B) {
		b.ReportAllocs()
		count := 0
		start := time.Now()
		for i := 0; i < b.N; i++ {
			idx.TagValues(query, tkey, func([]byte) bool { count++; return true })
		}
		b.ReportMetric(float64(count)/time.Since(start).Seconds()/1e6, "Mv/sec")
		b.ReportMetric(float64(count)/float64(b.N), "v/query")
	})

	b.Run("AddExisting", func(b *testing.B) {
		var m = bs("foo=bar,baz=bif,foo=bar,a=b,c=d,e=f,g=h")

		var idx T
		idx.Add(m)

		start := time.Now()
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			idx.Add(m)
		}

		b.ReportMetric(float64(b.N)/time.Since(start).Seconds()/1e6, "Mm/sec")
	})

	b.Run("Add1KNew", func(b *testing.B) {
		metrics := make([][]byte, 1000)
		for i := range metrics {
			metrics[i] = bs(fmt.Sprintf("foo=%d,bar=fixed", i))
		}

		start := time.Now()
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			var idx T
			for _, m := range metrics {
				idx.Add(m)
			}
		}

		b.ReportMetric(1000*float64(b.N)/time.Since(start).Seconds()/1e6, "Mm/sec")
	})

	b.Run("Metrics", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		start := time.Now()

		var sets uint64
		var count uint64
		for i := 0; i < b.N; i++ {
			idx.Metrics(mquery, func(metrics *roaring.Bitmap, tags [][]byte) bool {
				sets++
				count += metrics.GetCardinality()
				return true
			})
		}

		b.ReportMetric(float64(sets)/float64(b.N), "sets/op")
		b.ReportMetric(float64(count)/float64(b.N), "metrics/op")
		b.ReportMetric(float64(b.N)/time.Since(start).Seconds(), "ops/sec")
	})

	b.Run("AppendTo", func(b *testing.B) {
		var w rwutils.W
		idx.AppendTo(&w)

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			w.Init(w.Done().Reset())
			idx.AppendTo(&w)
		}
	})

	b.Run("ReadFrom", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			var r rwutils.R
			r.Init(buffer.OfLen(data))

			var idx T
			idx.ReadFrom(&r)
		}
	})
}

func dumpSizeStats(t testing.TB, idx *T) {
	ss := func(x []*roaring.Bitmap) (o uint64) {
		for _, bm := range x {
			o += bm.GetSizeInBytes()
		}
		return o + 8*uint64(len(x))
	}

	cs := func(x []*roaring.Bitmap) (o uint64) {
		for _, bm := range x {
			o += bm.GetCardinality()
		}
		return o
	}

	dumpSlice := func(name string, x []*roaring.Bitmap) {
		t.Log(name, "len:", len(x), "\t\tsize:", ss(x), "\t\tcard:", cs(x))
	}

	t.Log("idx:            ", "len:", idx.Cardinality(), "\tsize:", idx.Size(), "\tbpm: ", float64(idx.Size())/float64(idx.Cardinality()))
	t.Log("metric_set:     ", "len:", idx.metrics.Len(), "\tsize:", idx.metrics.Size(), "\tbpm: ", float64(idx.metrics.Size())/float64(idx.metrics.Len()))
	t.Log("tag_names:      ", "len:", idx.tag_names.Len(), "\t\tsize:", idx.tag_names.Size())
	t.Log("tkey_names:     ", "len:", idx.tkey_names.Len(), "\t\tsize:", idx.tkey_names.Size())

	dumpSlice("tag_to_metrics: ", idx.tag_to_metrics)
	dumpSlice("tag_to_tkeys:   ", idx.tag_to_tkeys)
	dumpSlice("tag_to_tags:    ", idx.tag_to_tags)
	dumpSlice("tkey_to_metrics:", idx.tkey_to_metrics)
	dumpSlice("tkey_to_tkeys:  ", idx.tkey_to_tkeys)
	dumpSlice("tkey_to_tags:   ", idx.tkey_to_tags)
	dumpSlice("tkey_to_tvals:  ", idx.tkey_to_tvals)
}
