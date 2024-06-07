package memindex

import (
	"bytes"

	"github.com/histdb/histdb"
	"github.com/histdb/histdb/buffer"
	"github.com/histdb/histdb/card"
	"github.com/histdb/histdb/hashtbl"
	"github.com/histdb/histdb/metrics"
	"github.com/histdb/histdb/pdqsort"
	"github.com/histdb/histdb/petname"
	"github.com/histdb/histdb/varint"
)

type T struct {
	_ [0]func() // no equality

	card RWId

	metrics      hashtbl.T[histdb.Hash, RWId] // for dedupe
	metric_names petname.B[RWId]              // to quickly append name
	tag_names    petname.T[histdb.TagHash, RWId]
	tkey_names   petname.T[histdb.TagKeyHash, RWId]

	tag_to_metrics  []*Bitmap // what metrics include this tag
	tkey_to_metrics []*Bitmap // what metrics include this tag key
	tkey_to_tvals   []*Bitmap // what tags exist for the specific tag key in any metric with tag key
}

func (t *T) Size() uint64 {
	return 0 +
		/* metrics         */ t.metrics.Size() +
		/* metric_names    */ t.metric_names.Size() +
		/* tag_names       */ t.tag_names.Size() +
		/* tkey_names      */ t.tkey_names.Size() +
		/* tag_to_metrics  */ sliceSize(t.tag_to_metrics) +
		/* tkey_to_metrics */ sliceSize(t.tkey_to_metrics) +
		/* tkey_to_tvals   */ sliceSize(t.tkey_to_tvals) +
		0
}

func (t *T) Cardinality() uint64 { return uint64(t.card) }

// Add includes the metric in to the index. It returns the hash of the metric,
// the id of the metric, the normalized metric (if the incoming normalized buf
// if not nil), and a boolean indicating if the metric was newly added to the
// index.
func (t *T) Add(metric, normalized []byte, cf *card.Fixer) (histdb.Hash, Id, []byte, bool) {
	if len(metric) == 0 {
		return histdb.Hash{}, 0, metric, false
	}

	tkeyis := make([]Id, 0, 8)
	tagis := make([]Id, 0, 8)
	var tagus map[Id]struct{}
	var hash histdb.Hash

	tagPtr := hash.TagHashPtr()
	tkeyPtr := hash.TagKeyHashPtr()

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

		tagh := histdb.NewTagHash(tag)
		tagi := t.tag_names.Put(tagh, tag)

		var ok bool
		tagis, tagus, ok = addSet(tagis, tagus, Id(tagi))

		if ok {
			tkeyPtr.Add(tkeyh)
			tagPtr.Add(tagh)

			tkeyis = append(tkeyis, Id(tkeyi))
		}
	}

	if len(tagis) >= 256 {
		return histdb.Hash{}, 0, metric, false
	}

	id, ok := t.metrics.Insert(hash, RWId(t.metrics.Len()))
	if ok {
		t.card++

		for i, tagi := range tagis {
			tkeyi := tkeyis[i]
			bitmapIndex(&t.tag_to_metrics, tagi).Add(Id(id))
			bitmapIndex(&t.tkey_to_tvals, tkeyi).Add(tagi)
			bitmapIndex(&t.tkey_to_metrics, tkeyi).Add(Id(id))
		}
	}

	if normalized != nil || ok {
		// we have to sort after adding to the bitmaps if necessary because we
		// assume that the values are added in numeric order so we can append a
		// single bitmap to the slice at a time.
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
	}

	if ok {
		buf := make([]byte, histdb.HashSize, histdb.HashSize+2*len(tagis))
		*(*[histdb.HashSize]byte)(buf) = hash
		for _, tagi := range tagis {
			var tmp [9]byte
			n := varint.Append(&tmp, uint64(tagi))
			buf = append(buf, tmp[:n]...)
		}
		t.metric_names.Append(buf)
	}

	if normalized != nil {
		for i, tagi := range tagis {
			if i > 0 {
				normalized = append(normalized, ',')
			}
			normalized = append(normalized, t.tag_names.Get(RWId(tagi))...)
		}
	}

	return hash, Id(id), normalized, ok
}

func (t *T) GetIdByHash(hash histdb.Hash) (Id, bool) {
	id, ok := t.metrics.Find(hash)
	return Id(id), ok
}

func (t *T) AppendNameByHash(hash histdb.Hash, buf []byte) ([]byte, bool) {
	id, ok := t.metrics.Find(hash)
	if !ok {
		return buf, false
	}
	return t.AppendNameById(Id(id), buf)
}

func (t *T) GetHashById(id Id) (hash histdb.Hash, ok bool) {
	buf := t.metric_names.Get(RWId(id))
	if len(buf) < histdb.HashSize {
		return histdb.Hash{}, false
	}
	return histdb.Hash(buf[0:histdb.HashSize]), true
}

func (t *T) AppendNameById(id Id, buf []byte) ([]byte, bool) {
	tagis := buffer.OfLen(t.metric_names.Get(RWId(id)))
	if tagis.Cap() < histdb.HashSize {
		return buf, false
	}
	tagis = tagis.Advance(histdb.HashSize)

	var (
		tagi uint64
		ok   bool
	)

	for i := 0; tagis.Remaining() > 0; i++ {
		tagi, tagis, ok = varint.Consume(tagis)
		if !ok {
			return buf, false
		}
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = append(buf, t.tag_names.Get(RWId(tagi))...)
	}

	return buf, true
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
