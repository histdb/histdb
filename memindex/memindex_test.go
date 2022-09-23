package memindex

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/RoaringBitmap/roaring"
	"github.com/zeebo/assert"

	"github.com/histdb/histdb"
	"github.com/histdb/histdb/metrics"
)

func TestMemindex(t *testing.T) {
	h := func(x histdb.Hash, _ bool) histdb.Hash { return x }
	b := func(_ histdb.Hash, x bool) bool { return x }

	t.Run("Duplicate Tags", func(t *testing.T) {
		var idx T

		assert.That(t, b(idx.Add("foo=bar")))
		assert.That(t, !b(idx.Add("foo=bar")))
		assert.That(t, !b(idx.Add("foo=bar,foo=bar")))
		assert.That(t, !b(idx.Add("foo=bar,foo=baz")))
	})

	t.Run("Empty Value", func(t *testing.T) {
		var idx T

		assert.That(t, b(idx.Add("foo=bar,baz")))
		assert.That(t, b(idx.Add("bif")))
		assert.That(t, b(idx.Add("baz")))
		assert.That(t, !b(idx.Add("baz=")))

		// assert.Equal(t, idx.Count("baz"), 2)
		// assert.Equal(t, idx.Count("bif"), 1)
		// assert.Equal(t, idx.Count(""), 3)
	})

	var strings []string
	collectStrings := func() func(x string) bool {
		strings = strings[:0]
		return func(x string) bool { strings = append(strings, x); return true }
	}

	t.Run("TagKeys", func(t *testing.T) {
		var idx T

		assert.That(t, b(idx.Add("k0=v0,k1=v1,k2=v2")))
		assert.That(t, b(idx.Add("k0=v0,foo")))
		assert.That(t, b(idx.Add("k1=v1,foo,baz")))
		assert.That(t, b(idx.Add("k0=v1,bar")))

		idx.TagKeys("k0=v0", collectStrings())
		assert.DeepEqual(t, strings, []string{"k1", "k2", "foo"})

		idx.TagKeys("k0", collectStrings())
		assert.DeepEqual(t, strings, []string{"k1", "k2", "foo", "bar"})

		idx.TagKeys("k0=", collectStrings())
		assert.DeepEqual(t, strings, []string{})

		idx.TagKeys("k0=v0,k1=v0", collectStrings())
		assert.DeepEqual(t, strings, []string{})

		idx.TagKeys("k0=v0,k1=v1", collectStrings())
		assert.DeepEqual(t, strings, []string{"k2"})
	})

	t.Run("TagValues", func(t *testing.T) {
		var idx T

		assert.That(t, b(idx.Add("k0=v0,k1=va,k2=v2")))
		assert.That(t, b(idx.Add("k0=v0,k1=vb,k2=v3")))
		assert.That(t, b(idx.Add("k0=v0,k2=v4")))
		assert.That(t, b(idx.Add("k1=va,k2=v4")))
		assert.That(t, b(idx.Add("k1=vb,k2=v4")))
		assert.That(t, b(idx.Add("k0=v1,k2=v5")))
		assert.That(t, b(idx.Add("k3=vx,k2=v6")))

		idx.TagValues("k0=v0", "k2", collectStrings())
		assert.DeepEqual(t, strings, []string{"v2", "v3", "v4"})

		idx.TagValues("k0", "k2", collectStrings())
		assert.DeepEqual(t, strings, []string{"v2", "v3", "v4", "v5"})

		idx.TagValues("", "k2", collectStrings())
		assert.DeepEqual(t, strings, []string{"v2", "v3", "v4", "v5", "v6"})

		idx.TagValues("k0=v0,k1=va", "k2", collectStrings())
		assert.DeepEqual(t, strings, []string{"v2"})

		idx.TagValues("k0=v0,k1=vb", "k2", collectStrings())
		assert.DeepEqual(t, strings, []string{"v3"})
	})

	t.Run("Hash", func(t *testing.T) {
		var idx T

		assert.Equal(t, h(idx.Add("k0=v0")), metrics.Hash("k0=v0"))
		assert.Equal(t, h(idx.Add("k0=v0,k1=v1")), metrics.Hash("k0=v0,k1=v1"))
		assert.Equal(t, h(idx.Add("k0=v0,k1=v1")), metrics.Hash("k0=v0,k1=v1"))
		assert.NotEqual(t, h(idx.Add("k0=v0,k1=v1")), metrics.Hash("k0=v0,k1=v2"))
		assert.Equal(t, h(idx.Add("k0=v0,k0=v1")), metrics.Hash("k0=v0,k0=v1"))
		assert.Equal(t, h(idx.Add("k0=v0,k0=v1")), metrics.Hash("k0=v0"))
		assert.Equal(t, h(idx.Add("k0=v0")), metrics.Hash("k0=v0,k0=v1"))
	})

	t.Run("Metrics", func(t *testing.T) {
		var idx T

		h0, _ := idx.Add("k0=v0a,k1=v1a,k2=v2a")
		h1, _ := idx.Add("k0=v0b,k1=v1b,k2=v2b")
		h2, _ := idx.Add("k0=v0c,k1=v1c")
		h3, _ := idx.Add("k0=v0c,k1=v1c,k2=v2a")
		h4, _ := idx.Add("k0=v0c,k1=v1c,k2=v2b")
		h5, _ := idx.Add("k0=v0c,k1=v1c,k2=v2c")

		exp := []histdb.Hash{h0, h1, h2, h3, h4, h5}
		got := []histdb.Hash{}

		idx.Metrics("k0,k1", func(metrics *roaring.Bitmap, tags []string) bool {
			idx.MetricHashes(metrics, func(h histdb.Hash) bool {
				got = append(got, h)
				return true
			})
			return true
		})

		sort.Slice(exp, func(i, j int) bool { return string(exp[i][:]) < string(exp[j][:]) })
		sort.Slice(got, func(i, j int) bool { return string(got[i][:]) < string(got[j][:]) })

		assert.DeepEqual(t, got, exp)
	})
}

func BenchmarkMemindex(b *testing.B) {
	var idx T
	loadLarge(&idx)
	dumpSizeStats(b, &idx)

	// query := "k0=v0"
	// tkey := "k9"

	const (
		query  = "app=storagenode-release,inst=12XzWDW7Nb496enKo4epRmpQamMe3cw7G3TUuhPrkoqoLb76rHK"
		tkey   = "name"
		mquery = query + "," + tkey
	)

	b.Run("TagKeys", func(b *testing.B) {
		b.ReportAllocs()
		count := 0
		start := time.Now()
		for i := 0; i < b.N; i++ {
			idx.TagKeys(query, func(string) bool { count++; return true })
		}
		b.ReportMetric(float64(count)/time.Since(start).Seconds()/1e6, "Mk/sec")
		b.ReportMetric(float64(count)/float64(b.N), "k/query")
	})

	b.Run("TagValues", func(b *testing.B) {
		b.ReportAllocs()
		count := 0
		start := time.Now()
		for i := 0; i < b.N; i++ {
			idx.TagValues(query, tkey, func(string) bool { count++; return true })
		}
		b.ReportMetric(float64(count)/time.Since(start).Seconds()/1e6, "Mv/sec")
		b.ReportMetric(float64(count)/float64(b.N), "v/query")
	})

	b.Run("AddExisting", func(b *testing.B) {
		const m = "foo=bar,baz=bif,foo=bar,a=b,c=d,e=f,g=h"

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

	b.Run("Metrics", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		start := time.Now()

		var sets uint64
		var count uint64
		for i := 0; i < b.N; i++ {
			idx.Metrics(mquery, func(metrics *roaring.Bitmap, tags []string) bool {
				sets++
				count += metrics.GetCardinality()
				return true
			})
		}

		b.ReportMetric(float64(sets)/float64(b.N), "sets/op")
		b.ReportMetric(float64(count)/float64(b.N), "metrics/op")
		b.ReportMetric(float64(b.N)/time.Since(start).Seconds(), "ops/sec")
	})
}

func dumpSizeStats(t testing.TB, idx *T) {
	const query = "k0=v0"
	const tkey = "k9"

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
		t.Log(name+":", "len:", len(x), "size:", ss(x), "card:", cs(x))
	}

	t.Log("metric_set:", "size:", idx.metric_set.Size(), "len:", idx.metric_set.Len())
	t.Log("tag_names:", "size:", idx.tag_names.Size(), "len:", idx.tag_names.Len())
	t.Log("tkey_names:", "size:", idx.tkey_names.Size(), "len:", idx.tkey_names.Len())

	dumpSlice("tag_to_metrics", idx.tag_to_metrics)
	dumpSlice("tag_to_tkeys", idx.tag_to_tkeys)
	dumpSlice("tag_to_tags", idx.tag_to_tags)
	dumpSlice("tkey_to_metrics", idx.tkey_to_metrics)
	dumpSlice("tkey_to_tkeys", idx.tkey_to_tkeys)
	dumpSlice("tkey_to_tags", idx.tkey_to_tags)
	dumpSlice("tkey_to_tvals", idx.tkey_to_tvals)

	t.Log("idx:", "size:", idx.Size(), "count:", idx.Cardinality(), "bpm:", float64(idx.Size())/float64(idx.Cardinality()))
}

func TestWhatever(t *testing.T) {
	t.SkipNow()

	var idx T
	loadLarge(&idx)
	dumpSizeStats(t, &idx)

	idx.Metrics("app=storagenode-release,inst=12XzWDW7Nb496enKo4epRmpQamMe3cw7G3TUuhPrkoqoLb76rHK,name",
		func(metrics *roaring.Bitmap, tags []string) bool {
			fmt.Println(tags, metrics.GetCardinality())
			return true
		})
}

func loadLarge(idx *T) {
	fh, err := os.Open("/home/jeff/go/src/github.com/zeebo/rothko/index/memindex/metrics.txt")
	if err != nil {
		panic(err)
	}
	defer fh.Close()

	gzfh, err := gzip.NewReader(fh)
	if err != nil {
		panic(err)
	}

	const statEvery = 100000
	start := time.Now()
	count := 0

	lstats := start
	lcard := 0
	lcount := 0

	stats := func() {
		msize := float64(idx.metric_set.Size()) +
			(24 + float64(unsafe.Sizeof(histdb.Hash{}))*float64(len(idx.metric_hashes)))
		size := float64(idx.Size())
		card := idx.Cardinality()

		fmt.Printf("Added (%-8d m) (%-8d um) | total (%0.2f%% unique) (%0.2f m/sec) (%0.2f um/sec) | recently (%0.2f%% unique) (%0.2f m/sec) (%0.2f um/sec) | index size (%0.2f MiB) (%0.2f b/m) | metric size (%0.2f MiB) (%0.2f MiB) (%0.2f b/m)\n",
			count,
			card,

			float64(card)/float64(count)*100,
			float64(count)/time.Since(start).Seconds(),
			float64(card)/time.Since(start).Seconds(),

			float64(card-lcard)/float64(count-lcount)*100,
			float64(count-lcount)/time.Since(lstats).Seconds(),
			float64(card-lcard)/time.Since(lstats).Seconds(),

			size/1024/1024,
			size/float64(card),

			msize/1024/1024,
			(size-msize)/1024/1024,
			(size-msize)/float64(card),
		)

		lstats = time.Now()
		lcard = card
		lcount = count
	}

	scanner := bufio.NewScanner(gzfh)
	for scanner.Scan() {
		idx.Add(strings.TrimSpace(scanner.Text()))
		count++
		if count%statEvery == 0 {
			stats()
			// if idx.Cardinality() >= 1e6 {
			// 	break
			// }
		}
	}

	idx.Fix()
	stats()
}
