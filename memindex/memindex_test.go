package memindex

import (
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
	t.Run("GetIdByHash", func(t *testing.T) {
		var idx T

		for range 1000 {
			hash, exp, _, ok := idx.Add(testhelp.Metric(5), nil, nil)
			assert.That(t, ok)
			got, ok := idx.GetIdByHash(hash)
			assert.That(t, ok)
			assert.Equal(t, got, exp)
		}

		_, ok := idx.GetIdByHash(testhelp.Key().Hash())
		assert.That(t, !ok)
	})

	t.Run("AppendNameByHash", func(t *testing.T) {
		var idx T

		for range 1000 {
			hash, _, exp, ok := idx.Add(testhelp.Metric(5), []byte{}, nil)
			assert.That(t, ok)
			got, ok := idx.AppendNameByHash(hash, nil)
			assert.That(t, ok)
			assert.Equal(t, string(got), string(exp))
		}
	})

	t.Run("GetHashById", func(t *testing.T) {
		var idx T

		for range 1000 {
			exp, id, _, ok := idx.Add(testhelp.Metric(5), nil, nil)
			assert.That(t, ok)
			got, ok := idx.GetHashById(id)
			assert.That(t, ok)
			assert.Equal(t, got, exp)
		}
	})

	t.Run("AppendNameById", func(t *testing.T) {
		var idx T

		for range 1000 {
			_, id, exp, ok := idx.Add(testhelp.Metric(5), []byte{}, nil)
			assert.That(t, ok)
			got, ok := idx.AppendNameById(id, nil)
			assert.That(t, ok)
			assert.Equal(t, string(got), string(exp))
		}
	})

	t.Run("CardFix", func(t *testing.T) {
		var idx T
		var cf card.Fixer

		cf.DropTagKey(bs(`interface`))
		cf.RewriteTag(bs(`error_name`), bs(`Node\ ID:`), bs(`error_name=fixed`))

		_, _, norm, ok := idx.Add(bs(`interface=foo,error_name=Node\ ID: blah,field=error`), []byte{}, &cf)
		assert.That(t, ok)
		assert.Equal(t, string(norm), "error_name=fixed,field=error")
	})

	t.Run("Duplicate Tags", func(t *testing.T) {
		var idx T

		assert.That(t, fth(idx.Add(bs("foo=bar"), nil, nil)))
		assert.That(t, !fth(idx.Add(bs("foo=bar"), nil, nil)))
		assert.That(t, !fth(idx.Add(bs("foo=bar,foo=bar"), nil, nil)))
		assert.That(t, fth(idx.Add(bs("foo=bar,foo=baz"), nil, nil)))
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
		assert.Equal(t, fst(idx.Add(bs("k0=v0,k0=v0"), nil, nil)), metrics.Hash(bs("k0=v0")))
		assert.Equal(t, fst(idx.Add(bs("k0=v0"), nil, nil)), metrics.Hash(bs("k0=v0,k0=v0")))
	})

	t.Run("QueryFilter", func(t *testing.T) {
		var idx T

		idx.Add(bs("k0=v0"), nil, nil)
		idx.Add(bs("k0=v1"), nil, nil)
		idx.Add(bs("k0=v2"), nil, nil)

		idx.QueryFilter(bs("k0"),
			func(b []byte) bool { return string(b) != "v1" },
			func(bm *Bitmap) { assert.Equal(t, bm.String(), "{0,2}") },
		)
	})

	t.Run("Serialize", func(t *testing.T) {
		var idx T
		for range 1000 {
			idx.Add(testhelp.Metric(5), nil, nil)
		}

		var w rwutils.W
		AppendTo(&idx, &w)

		var r rwutils.R
		r.Init(w.Done().Trim().Reset())

		var idx2 T
		ReadFrom(&idx2, &r)
		_, err := r.Done()
		assert.NoError(t, err)

		assert.Equal(t, idx.metrics, idx2.metrics)
		assert.Equal(t, idx.metric_names, idx2.metric_names)
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
	data, err := os.ReadFile("metrics.idx")
	if err != nil {
		doReload()
		data, err = os.ReadFile("metrics.idx")
		if err != nil {
			b.Fatal(err)
		}
	}

	var r rwutils.R
	r.Init(buffer.OfLen(data))

	var idx T
	ReadFrom(&idx, &r)
	_, err = r.Done()
	assert.NoError(b, err)

	b.Run("GetIdByHash", func(b *testing.B) {
		hash, ok := idx.GetHashById(10)
		assert.That(b, ok)

		b.ReportAllocs()
		b.ResetTimer()

		for range b.N {
			_, ok := idx.GetIdByHash(hash)
			assert.That(b, ok)
		}
	})

	b.Run("AppendNameByHash", func(b *testing.B) {
		hash, ok := idx.GetHashById(10)
		assert.That(b, ok)

		buf, ok := idx.AppendNameByHash(hash, nil)
		assert.That(b, ok)
		buf = buf[:0]

		b.ReportAllocs()
		b.ResetTimer()

		for range b.N {
			_, ok = idx.AppendNameByHash(hash, buf[:0])
			assert.That(b, ok)
		}
	})

	b.Run("GetHashById", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for range b.N {
			_, ok := idx.GetHashById(10)
			assert.That(b, ok)
		}
	})

	b.Run("AppendNameById", func(b *testing.B) {
		buf, ok := idx.AppendNameById(10, nil)
		assert.That(b, ok)
		buf = buf[:0]

		b.ReportAllocs()
		b.ResetTimer()

		for range b.N {
			_, ok := idx.AppendNameById(10, buf)
			assert.That(b, ok)
		}
	})

	b.Run("AddExisting", func(b *testing.B) {
		buf, ok := idx.AppendNameById(10, nil)
		assert.That(b, ok)

		start := time.Now()
		b.ResetTimer()
		b.ReportAllocs()

		for range b.N {
			idx.Add(buf, nil, nil)
		}

		b.ReportMetric(float64(b.N)/time.Since(start).Seconds()/1e6, "Mm/sec")
	})

	b.Run("AddUnique1K", func(b *testing.B) {
		metrics := make([][]byte, 1000)
		for i := range metrics {
			metrics[i] = testhelp.Metric(0)
		}

		start := time.Now()
		b.ResetTimer()
		b.ReportAllocs()

		for range b.N {
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

		for range b.N {
			w.Init(w.Done().Reset())
			AppendTo(&idx, &w)
		}
	})

	b.Run("ReadFrom", func(b *testing.B) {
		b.SetBytes(int64(len(data)))
		b.ReportAllocs()
		b.ResetTimer()

		for range b.N {
			var r rwutils.R
			r.Init(buffer.OfLen(data))

			var idx T
			ReadFrom(&idx, &r)
		}
	})
}
