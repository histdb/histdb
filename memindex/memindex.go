package memindex

import (
	"bytes"

	"github.com/histdb/histdb"
	"github.com/histdb/histdb/card"
	"github.com/histdb/histdb/hashtbl"
	"github.com/histdb/histdb/metrics"
	"github.com/histdb/histdb/pdqsort"
	"github.com/histdb/histdb/petname"
)

// TODO: we can have an LRU cache of common bitmaps based on tag hashes. for example we always
// compute the tag_to_metrics intersection bitmap. if we do it smart, we can keep track of the
// "path" along the way. this would make subsequent queries that have the same prefix faster.

type T struct {
	_ [0]func() // no equality

	metrics    hashtbl.T[histdb.Hash, RWId]
	tag_names  petname.T[histdb.TagHash, RWId]
	tkey_names petname.T[histdb.TagKeyHash, RWId]

	tag_to_metrics  []*Bitmap // what metrics include this tag
	tkey_to_metrics []*Bitmap // what metrics include this tag key
	tkey_to_tvals   []*Bitmap // what tags exist for the specific tag key in any metric with tag key
}

func (t *T) Size() uint64 {
	return 0 +
		/* hash_set        */ t.metrics.Size() +
		/* tag_names       */ t.tag_names.Size() +
		/* tkey_names      */ t.tkey_names.Size() +
		/* tag_to_metrics  */ sliceSize(t.tag_to_metrics) +
		/* tkey_to_metrics */ sliceSize(t.tkey_to_metrics) +
		/* tkey_to_tvals   */ sliceSize(t.tkey_to_tvals) +
		0
}

func (t *T) Cardinality() int { return t.metrics.Len() }

func (t *T) Add(metric, normalized []byte, cf *card.Fixer) (histdb.Hash, Id, []byte, bool) {
	if len(metric) == 0 {
		return histdb.Hash{}, 0, metric, false
	}

	tkeyis := make([]Id, 0, 8)
	tagis := make([]Id, 0, 8)
	var tkeyus map[Id]struct{}
	var hash histdb.Hash

	mhp := hash.TagHashPtr()
	thp := hash.TagKeyHashPtr()

	for rest := metric; len(rest) > 0; {
		var tkey, tag []byte
		tkey, tag, rest = metrics.PopTag(rest)
		if cf != nil {
			tag = cf.Fix(tkey, tag)
		}
		if len(tag) == 0 {
			continue
		}

		tkeyh := histdb.NewTagKeyHash(tkey)
		tkeyi := t.tkey_names.Put(tkeyh, tkey)

		var ok bool
		tkeyis, tkeyus, ok = addSet(tkeyis, tkeyus, Id(tkeyi))

		if ok {
			tagh := histdb.NewTagHash(tag)

			thp.Add(tkeyh)
			mhp.Add(tagh)

			tagi := t.tag_names.Put(tagh, tag)
			tagis = append(tagis, Id(tagi))
		}
	}

	if len(tagis) >= 256 {
		return histdb.Hash{}, 0, metric, false
	}

	// TODO: why was this check here? i'm forget and i don't know what it's for so i'm spooked lol
	// if t.metrics.Len() > 1<<31-1 {
	// 	return hash, false
	// }

	metrici, ok := t.metrics.Insert(hash, RWId(t.metrics.Len()))
	if !ok {
		for i := range tagis {
			tagi := tagis[i]
			tkeyi := tkeyis[i]

			bitmapIndex(&t.tag_to_metrics, tagi).Add(Id(metrici))   // tagis[i] should know about metric
			bitmapIndex(&t.tkey_to_tvals, tkeyi).Add(tagis[i])      // tkeys[i] should know about tagis[i]
			bitmapIndex(&t.tkey_to_metrics, tkeyi).Add(Id(metrici)) // tkeys[i] should know about metric
		}
	}

	if normalized != nil {
		// we have to sort after adding to the bitmaps if necessary because we assume that the values
		// are added in numeric order so we can append a single bitmap to the slice at a time.
		pdqsort.Sort(pdqsort.T{
			Less: func(i, j int) bool {
				tagi := t.tag_names.Get(RWId(tagis[i]))
				tagj := t.tag_names.Get(RWId(tagis[j]))
				return string(tagi) < string(tagj)
			},
			Swap: func(i, j int) {
				tagis[i], tagis[j] = tagis[j], tagis[i]
				tkeyis[i], tkeyis[j] = tkeyis[j], tkeyis[i]
			},
		}, len(tagis))

		normalized = t.DecodeInto(tagis, normalized[:0])
	}

	return hash, Id(metrici), normalized, !ok
}

func (t *T) EncodeInto(metric []byte, out []Id) ([]Id, bool) {
	if len(metric) == 0 {
		return nil, false
	}

	tkeyis := make([]Id, 0, 8)
	var tkeyus map[Id]struct{}

	for rest := metric; len(rest) > 0; {
		var tkey, tag []byte
		tkey, tag, rest = metrics.PopTag(rest)
		if len(tag) == 0 {
			continue
		}

		tkeyh := histdb.NewTagKeyHash(tkey)
		tkeyi, ok := t.tkey_names.Find(tkeyh)
		if !ok {
			return nil, false
		}

		tkeyis, tkeyus, ok = addSet(tkeyis, tkeyus, Id(tkeyi))

		if ok {
			tagh := histdb.NewTagHash(tag)
			tagi, ok := t.tag_names.Find(tagh)
			if !ok {
				return nil, false
			}

			out = append(out, Id(tagi))
		}
	}

	if len(out) >= 256 {
		return nil, false
	}

	return out, true
}

func (t *T) DecodeInto(tagis []Id, buf []byte) []byte {
	for i, tagi := range tagis {
		tag := t.tag_names.Get(RWId(tagi))
		if tag == nil {
			continue
		}
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = append(buf, tag...)
	}
	return buf
}

func (t *T) AppendMetricName(id Id, buf []byte) ([]byte, bool) {
	tagis := make([]Id, 0, 8)

	for tkeyn, tkeybm := range t.tkey_to_metrics {
		if !tkeybm.Contains(id) {
			continue
		}

		Iter(t.tkey_to_tvals[tkeyn], func(tagn Id) bool {
			if t.tag_to_metrics[tagn].Contains(id) {
				tagis = append(tagis, tagn)
			}
			return true
		})
	}

	if len(tagis) == 0 {
		return nil, false
	}

	pdqsort.Less(tagis, func(i, j int) bool {
		tagi := t.tag_names.Get(RWId(tagis[i]))
		tagj := t.tag_names.Get(RWId(tagis[j]))
		return string(tagi) < string(tagj)
	})

	return t.DecodeInto(tagis, buf), true
}

func (t *T) LowCardinalityTags(under int, cb func([]byte) bool) {
	for tagn, tags := range t.tag_to_metrics {
		if tags.GetCardinality() <= uint64(under) {
			if !cb(t.tag_names.Get(RWId(tagn))) {
				return
			}
		}
	}
}

func (t *T) HighCardinalityTagKeys(over int, cb func([]byte) bool) {
	for tkeyn, tkeys := range t.tkey_to_tvals {
		if tkeys.GetCardinality() >= uint64(over) {
			if !cb(t.tkey_names.Get(RWId(tkeyn))) {
				return
			}
		}
	}
}

func (t *T) QueryTrue(tkeys []byte, cb func(*Bitmap)) {
	if bytes.IndexByte(tkeys, ',') == -1 {
		tkeyn, ok := t.tkey_names.Find(histdb.NewTagKeyHash(tkeys))
		if !ok {
			cb(new(Bitmap))
			return
		}
		cb(t.tkey_to_metrics[tkeyn])
		return
	}

	var bms []*Bitmap

	for len(tkeys) > 0 {
		var tkey []byte
		tkey, _, tkeys = metrics.PopTag(tkeys)

		tkeyn, ok := t.tkey_names.Find(histdb.NewTagKeyHash(tkey))
		if !ok {
			cb(new(Bitmap))
			return
		}

		bms = append(bms, t.tkey_to_metrics[tkeyn])
	}

	cb(bitmapOr(bms...))
}

func (t *T) QueryEqual(tag []byte, cb func(*Bitmap)) {
	tagn, ok := t.tag_names.Find(histdb.NewTagHash(tag))
	if !ok {
		cb(new(Bitmap))
		return
	}
	cb(t.tag_to_metrics[tagn])
}

func (t *T) QueryNotEqual(tkey, tag []byte, cb func(*Bitmap)) {
	tkeyn, ok := t.tkey_names.Find(histdb.NewTagKeyHash(tkey))
	if !ok {
		cb(new(Bitmap))
		return
	}

	tagn, ok := t.tag_names.Find(histdb.NewTagHash(tag))
	if !ok {
		cb(new(Bitmap))
		return
	}

	m := bitmapAcquire()
	defer bitmapReplace(m)

	m.Or(t.tkey_to_metrics[tkeyn])
	m.AndNot(t.tag_to_metrics[tagn])

	cb(m)
}

func (t *T) QueryFilter(tkey []byte, fn func([]byte) bool, cb func(*Bitmap)) {
	tkeyn, ok := t.tkey_names.Find(histdb.NewTagKeyHash(tkey))
	if !ok {
		cb(new(Bitmap))
		return
	}

	var bms []*Bitmap

	Iter(t.tkey_to_tvals[tkeyn], func(tagn Id) bool {
		if fn(tagValue(tkey, t.tag_names.Get(RWId(tagn)))) {
			bms = append(bms, t.tag_to_metrics[tagn])
		}
		return true
	})

	cb(bitmapOr(bms...))
}

func (t *T) QueryFilterNot(tkey []byte, fn func([]byte) bool, cb func(*Bitmap)) {
	tkeyn, ok := t.tkey_names.Find(histdb.NewTagKeyHash(tkey))
	if !ok {
		cb(new(Bitmap))
		return
	}

	var bms []*Bitmap

	Iter(t.tkey_to_tvals[tkeyn], func(tagn Id) bool {
		if !fn(tagValue(tkey, t.tag_names.Get(RWId(tagn)))) {
			bms = append(bms, t.tag_to_metrics[tagn])
		}
		return true
	})

	cb(bitmapOr(bms...))
}
