package memindex

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/zeebo/assert"
)

func TestPopTags(t *testing.T) {
	check := func(tags string, tkey, tag, rest string) {
		gtkey, gtag, grest := popTag(tags)
		assert.Equal(t, tkey, gtkey)
		assert.Equal(t, tag, gtag)
		assert.Equal(t, rest, grest)
	}

	check("foo=bar,foo=bar", "foo", "foo=bar", "foo=bar")
	check("foo=bar", "foo", "foo=bar", "")
	check("foo=", "foo", "foo", "")
	check("foo", "foo", "foo", "")

	// TODO: check escape sequences
}

func TestMemindex(t *testing.T) {
	t.Run("Duplicate Tags", func(t *testing.T) {
		idx := New()

		assert.That(t, idx.Add("foo=bar"))
		assert.That(t, !idx.Add("foo=bar"))
		assert.That(t, !idx.Add("foo=bar,foo=bar"))
	})

	t.Run("Empty Value", func(t *testing.T) {
		idx := New()

		assert.That(t, idx.Add("foo=bar,baz"))
		assert.That(t, idx.Add("bif"))
		assert.That(t, idx.Add("baz"))
		assert.That(t, !idx.Add("baz="))

		assert.Equal(t, idx.Count("baz"), 2)
		assert.Equal(t, idx.Count("bif"), 1)
		assert.Equal(t, idx.Count(""), 3)
	})

	var strings []string
	collectStrings := func() func(x string) bool {
		strings = strings[:0]
		return func(x string) bool { strings = append(strings, x); return true }
	}
	collectBytes := func() func([]byte) bool {
		strings = strings[:0]
		return func(x []byte) bool { strings = append(strings, string(x)); return true }
	}

	t.Run("Metrics", func(t *testing.T) {
		idx := New()

		assert.That(t, idx.Add("k0=v0,k1=v1,k2=v2"))

		idx.Metrics("k0=v0", nil, collectBytes())
		assert.DeepEqual(t, strings, []string{"k0=v0,k1=v1,k2=v2"})

		idx.Metrics("k0=v0,k1=v0", nil, collectBytes())
		assert.DeepEqual(t, strings, []string{})
	})

	t.Run("TagKeys", func(t *testing.T) {
		idx := New()

		assert.That(t, idx.Add("k0=v0,k1=v1,k2=v2"))
		assert.That(t, idx.Add("k0=v0,foo"))
		assert.That(t, idx.Add("k1=v1,foo"))

		idx.TagKeys("k0=v0", collectStrings())
		assert.DeepEqual(t, strings, []string{"k1", "k2", "foo"})

		idx.TagKeys("k0=v0,k1=v0", collectStrings())
		assert.DeepEqual(t, strings, []string{})

		idx.TagKeys("k0=v0,k1=v1", collectStrings())
		assert.DeepEqual(t, strings, []string{"k2"})
	})

	t.Run("TagValues", func(t *testing.T) {
		idx := New()

		assert.That(t, idx.Add("k0=v0,k1=va,k2=v2"))
		assert.That(t, idx.Add("k0=v0,k1=vb,k2=v3"))
		assert.That(t, idx.Add("k0=v0,k2=v4"))
		assert.That(t, idx.Add("k1=va,k2=v4"))
		assert.That(t, idx.Add("k1=vb,k2=v4"))

		idx.TagValues("k0=v0", "k2", collectStrings())
		assert.DeepEqual(t, strings, []string{"v2", "v3", "v4"})

		idx.TagValues("k0=v0,k1=va", "k2", collectStrings())
		assert.DeepEqual(t, strings, []string{"v2"})

		idx.TagValues("k0=v0,k1=vb", "k2", collectStrings())
		assert.DeepEqual(t, strings, []string{"v3"})
	})
}

func BenchmarkMemindex(b *testing.B) {
	idx := New()
	loadLarge(idx)
	dumpSizeStats(b, idx)

	// query := "k0=v0"
	// tkey := "k9"

	query := "app=storagenode-release,inst=12XzWDW7Nb496enKo4epRmpQamMe3cw7G3TUuhPrkoqoLb76rHK"
	tkey := "name"

	// var query, tkey string
	// idx.Metrics("", nil, func(b []byte) (ok bool) {
	// 	ok = query == ""
	// 	query = string(b)
	// 	tkey, _, _ = popTag(query)
	// 	return ok
	// })

	// b.Log(query)
	// idx.TagKeys(query, func(x string) bool {
	// 	b.Log(x)
	// 	return true
	// })

	b.Run("Count", func(b *testing.B) {
		b.ReportAllocs()
		count := 0
		start := time.Now()
		for i := 0; i < b.N; i++ {
			count += idx.Count(query)
		}
		b.ReportMetric(float64(count)/time.Since(start).Seconds()/1e6, "Mm/sec")
		b.ReportMetric(float64(count)/float64(b.N), "m/query")
	})

	b.Run("Metrics", func(b *testing.B) {
		buf := make([]byte, 0, 64)
		b.ReportAllocs()
		count := 0
		start := time.Now()
		for i := 0; i < b.N; i++ {
			idx.Metrics(query, buf, func([]byte) bool { count++; return true })
		}
		b.ReportMetric(float64(count)/time.Since(start).Seconds()/1e6, "Mm/sec")
		b.ReportMetric(float64(count)/float64(b.N), "m/query")
	})

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
		// fh, _ := os.Create("tv-cpu.pprof")
		// defer fh.Close()
		// _ = pprof.StartCPUProfile(fh)
		// defer pprof.StopCPUProfile()

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

		idx := New()
		idx.Add(m)

		start := time.Now()
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			idx.Add(m)
		}

		b.ReportMetric(float64(b.N)/time.Since(start).Seconds()/1e6, "Mm/sec")
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

	t.Log("metric_names:", "size:", idx.metric_names.Size())
	t.Log("tag_names:", "size:", idx.tag_names.Size())
	t.Log("tkey_names:", "size:", idx.tkey_names.Size())

	t.Log("metrics:", "size:", idx.metrics.GetSizeInBytes(), "card:", idx.metrics.GetCardinality())
	t.Log("tags:", "size:", idx.tags.GetSizeInBytes(), "card:", idx.tags.GetCardinality())
	t.Log("keys:", "size:", idx.tkeys.GetSizeInBytes(), "card:", idx.tkeys.GetCardinality())

	dumpSlice("tag_to_metrics", idx.tag_to_metrics)
	dumpSlice("tag_to_tkeys", idx.tag_to_tkeys)
	dumpSlice("tag_to_tags", idx.tag_to_tags)
	dumpSlice("tkey_to_tags", idx.tkey_to_tags)
	dumpSlice("tkey_to_metrics", idx.tkey_to_metrics)

	t.Log("idx:", "size:", idx.Size(), "count:", idx.Count(""), "bpm:", float64(idx.Size())/float64(idx.Count("")))
}

func TestWhatever(t *testing.T) {
	t.SkipNow()

	idx := New()
	loadLarge(idx)
	dumpSizeStats(t, idx)
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
		size := float64(idx.Size())
		card := idx.Count("")

		fmt.Printf("Added (%-8d m) (%-8d um) | total (%0.2f%% unique) (%0.2f m/sec) (%0.2f um/sec) | recently (%0.2f%% unique) (%0.2f m/sec) (%0.2f um/sec) | (%0.2f MiB) (%0.2f b/m)\n",
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
			// if idx.Count("") >= 10000000 {
			// 	break
			// }
		}
	}

	idx.Fix()
	stats()
}
