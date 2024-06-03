package memindex

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/zeebo/assert"

	"github.com/histdb/histdb/buffer"
	"github.com/histdb/histdb/card"
	"github.com/histdb/histdb/metrics"
	"github.com/histdb/histdb/rwutils"
	"github.com/histdb/histdb/testhelp"
)

func bs(s string) []byte                       { return []byte(s) }
func sl[T any](x ...T) []T                     { return x }
func fst[T, U, V, W any](t T, _ U, _ V, _ W) T { return t }
func snd[T, U, V, W any](_ T, u U, _ V, _ W) U { return u }
func trd[T, U, V, W any](_ T, _ U, v V, _ W) V { return v }
func fth[T, U, V, W any](_ T, _ U, _ V, w W) W { return w }

func TestMemindex(t *testing.T) {
	t.Run("CardFix", func(t *testing.T) {
		var idx T
		var cf card.Fixer

		cf.DropTagKey(bs(`interface`))
		cf.RewriteTag(bs(`error_name`), bs(`Node\ ID:`), bs(`error_name=fixed`))

		_, _, norm, ok := idx.Add(bs(`interface=foo,error_name=Node\ ID: blah,field=error`), []byte{}, &cf)
		assert.That(t, ok)
		assert.Equal(t, string(norm), "error_name=fixed,field=error")
	})

	t.Run("AppendMetricName", func(t *testing.T) {
		var idx T

		idx.Add(bs("a=b,foo="), nil, nil)
		idx.Add(bs("a=b,foo"), nil, nil)
		idx.Add(bs("a=c,foo=a"), nil, nil)

		{
			n, _ := idx.AppendMetricName(0, nil)
			assert.Equal(t, n, bs("a=b,foo"))
		}

		{
			n, _ := idx.AppendMetricName(1, nil)
			assert.Equal(t, n, bs("a=c,foo=a"))
		}
	})

	t.Run("Add", func(t *testing.T) {
		var idx T

		for i := 0; i < 1e5; i++ {
			idx.Add(testhelp.Name(3), nil, nil)
		}
	})

	t.Run("EncodeInto", func(t *testing.T) {
		var idx T

		idx.Add(bs("foo1=bar1,foo2=bar2,foo3=bar3"), nil, nil)

		tagis, ok := idx.EncodeInto(bs("foo1=bar1,foo3=bar3"), nil)
		assert.That(t, ok)
		assert.Equal(t, tagis, []Id{0, 2})

		_, ok = idx.EncodeInto(bs("foo1=bar1,foo4=bar4"), nil)
		assert.That(t, !ok)
	})

	t.Run("DecodeInto", func(t *testing.T) {
		var idx T

		idx.Add(bs("foo1=bar1,foo2=bar2,foo3=bar3"), nil, nil)

		metric := idx.DecodeInto([]Id{0, 2}, nil)
		assert.Equal(t, string(metric), "foo1=bar1,foo3=bar3")

		metric = idx.DecodeInto([]Id{0, 3}, nil)
		assert.Equal(t, string(metric), "foo1=bar1")
	})

	t.Run("Duplicate Tags", func(t *testing.T) {
		var idx T

		assert.That(t, fth(idx.Add(bs("foo=bar"), nil, nil)))
		assert.That(t, !fth(idx.Add(bs("foo=bar"), nil, nil)))
		assert.That(t, !fth(idx.Add(bs("foo=bar,foo=bar"), nil, nil)))
		assert.That(t, !fth(idx.Add(bs("foo=bar,foo=baz"), nil, nil)))
	})

	t.Run("Empty Value", func(t *testing.T) {
		var idx T

		assert.That(t, fth(idx.Add(bs("foo=bar,baz"), nil, nil)))
		assert.That(t, fth(idx.Add(bs("bif"), nil, nil)))
		assert.That(t, fth(idx.Add(bs("baz"), nil, nil)))
		assert.That(t, !fth(idx.Add(bs("baz="), nil, nil)))
	})

	t.Run("Hash", func(t *testing.T) {
		var idx T

		assert.Equal(t, fst(idx.Add(bs("k0=v0"), nil, nil)), metrics.Hash(bs("k0=v0")))
		assert.Equal(t, fst(idx.Add(bs("k0=v0,k1=v1"), nil, nil)), metrics.Hash(bs("k0=v0,k1=v1")))
		assert.Equal(t, fst(idx.Add(bs("k0=v0,k1=v1"), nil, nil)), metrics.Hash(bs("k0=v0,k1=v1")))
		assert.NotEqual(t, fst(idx.Add(bs("k0=v0,k1=v1"), nil, nil)), metrics.Hash(bs("k0=v0,k1=v2")))
		assert.Equal(t, fst(idx.Add(bs("k0=v0,k0=v1"), nil, nil)), metrics.Hash(bs("k0=v0,k0=v1")))
		assert.Equal(t, fst(idx.Add(bs("k0=v0,k0=v1"), nil, nil)), metrics.Hash(bs("k0=v0")))
		assert.Equal(t, fst(idx.Add(bs("k0=v0"), nil, nil)), metrics.Hash(bs("k0=v0,k0=v1")))
	})

	t.Run("QueryFilter", func(t *testing.T) {
		var idx T

		idx.Add(bs("k0=v0"), nil, nil)
		idx.Add(bs("k0=v1"), nil, nil)
		idx.Add(bs("k0=v2"), nil, nil)

		idx.QueryFilter(bs("k0"),
			func(b []byte) bool { t.Logf("%s", b); return string(b) != "k0=v1" },
			func(bm *Bitmap) { t.Logf("%s", bm) },
		)
	})

	t.Run("Serialize", func(t *testing.T) {
		var idx T
		loadRandom(&idx)

		var w rwutils.W
		AppendTo(&idx, &w)

		var r rwutils.R
		r.Init(w.Done().Trim().Reset())

		var idx2 T
		ReadFrom(&idx2, &r)
		_, err := r.Done()
		assert.NoError(t, err)

		assert.Equal(t, idx.metrics, idx2.metrics)
		assert.Equal(t, idx.tag_names, idx2.tag_names)
		assert.Equal(t, idx.tkey_names, idx2.tkey_names)

		equalBitmaps := func(a, b []*Bitmap) {
			assert.Equal(t, len(a), len(b))
			for i := range a {
				assert.That(t, a[i].Equals(b[i]))
			}
		}

		equalBitmaps(idx.tag_to_metrics, idx2.tag_to_metrics)
		equalBitmaps(idx.tkey_to_metrics, idx2.tkey_to_metrics)
		equalBitmaps(idx.tkey_to_tvals, idx2.tkey_to_tvals)
	})
}

func BenchmarkMemindex(b *testing.B) {
	data, _ := os.ReadFile("metrics.idx")
	var r rwutils.R
	r.Init(buffer.OfLen(data))

	var idx T
	ReadFrom(&idx, &r)
	_, err := r.Done()
	assert.NoError(b, err)

	var (
		query = bs("app=storagenode-release,inst=12XzWDW7Nb496enKo4epRmpQamMe3cw7G3TUuhPrkoqoLb76rHK")
		// tkey  = bs("name")
		// mquery = bs(string(query) + "," + string(tkey))
		// mquery = bs(`name,field,app`)
	)

	b.Run("EncodeInto", func(b *testing.B) {
		b.ReportAllocs()

		tagis, ok := idx.EncodeInto(query, nil)
		assert.That(b, ok)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = idx.EncodeInto(query, tagis[:0])
		}
	})

	b.Run("DecodeInto", func(b *testing.B) {
		b.ReportAllocs()

		tagis, ok := idx.EncodeInto(query, nil)
		assert.That(b, ok)
		buf := idx.DecodeInto(tagis, nil)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = idx.DecodeInto(tagis, buf[:0])
		}
	})

	b.Run("AddExisting", func(b *testing.B) {
		var m = bs("foo=bar,baz=bif,foo=bar,a=b,c=d,e=f,g=h")

		var idx T
		idx.Add(m, nil, nil)

		start := time.Now()
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			idx.Add(m, nil, nil)
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
				idx.Add(m, nil, nil)
			}
		}

		b.ReportMetric(1000*float64(b.N)/time.Since(start).Seconds()/1e6, "Mm/sec")
	})

	b.Run("AppendTo", func(b *testing.B) {
		var w rwutils.W
		AppendTo(&idx, &w)

		b.SetBytes(int64(w.Done().Pos()))
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			w.Init(w.Done().Reset())
			AppendTo(&idx, &w)
		}
	})

	b.Run("ReadFrom", func(b *testing.B) {
		b.SetBytes(int64(len(data)))
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			var r rwutils.R
			r.Init(buffer.OfLen(data))

			var idx T
			ReadFrom(&idx, &r)
		}
	})
}
