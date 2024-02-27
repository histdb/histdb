package memindex

import (
	"bytes"
	"sort"

	"github.com/histdb/histdb"
	"github.com/histdb/histdb/hashtbl"
	"github.com/histdb/histdb/metrics"
	"github.com/histdb/histdb/petname"
)

// TODO: we can have an LRU cache of common bitmaps based on tag hashes. for example
// we always compute the tag_to_metrics intersection bitmap. if we do it smart, we can
// keep track of the "path" along the way. this would make subsequent queries that
// have the same prefix faster.

type T struct {
	_ [0]func() // no equality

	fixed bool
	card  int

	metrics     hashtbl.T[histdb.Hash, rwId]
	tag_names   petname.T[histdb.TagHash, rwId]
	tkey_names  petname.T[histdb.TagKeyHash, rwId]
	tkeys_names hashtbl.T[tkeySet, rwId]

	tag_to_metrics   []*Bitmap // what metrics include this tag
	tag_to_tkeys     []*Bitmap // what tag keys exist in any metric with tag
	tag_to_tags      []*Bitmap // what tags exist in any metric with tag
	tkeys_to_metrics []*Bitmap // what metrics have a specific tag key set
	tkey_to_metrics  []*Bitmap // what metrics include this tag key
	tkey_to_tkeys    []*Bitmap // what tag keys exist in any metric with tag key
	tkey_to_tags     []*Bitmap // what tags exist in any metric with tag key
	tkey_to_tvals    []*Bitmap // what tags exist for the specific tag key in any metric with tag key
}

func (t *T) Size() uint64 {
	return 0 +
		/* fixed           */ 8 +
		/* card            */ 8 +
		/* hash_set        */ t.metrics.Size() +
		/* tag_names       */ t.tag_names.Size() +
		/* tkey_names      */ t.tkey_names.Size() +
		/* tkeys_names     */ t.tkeys_names.Size() +
		/* tag_to_metrics  */ sliceSize(t.tag_to_metrics) +
		/* tag_to_tkeys    */ sliceSize(t.tag_to_tkeys) +
		/* tag_to_tags     */ sliceSize(t.tag_to_tags) +
		/* tkey_to_metrics */ sliceSize(t.tkeys_to_metrics) +
		/* tkey_to_metrics */ sliceSize(t.tkey_to_metrics) +
		/* tkey_to_tkeys   */ sliceSize(t.tkey_to_tkeys) +
		/* tkey_to_tags    */ sliceSize(t.tkey_to_tags) +
		/* tkey_to_tvals   */ sliceSize(t.tkey_to_tvals) +
		0
}

func (t *T) Fix() {
	fix := func(bms []*Bitmap) {
		for _, bm := range bms {
			bm.RunOptimize()
		}
	}

	fix(t.tag_to_metrics)
	fix(t.tag_to_tkeys)
	fix(t.tag_to_tags)
	fix(t.tkeys_to_metrics)
	fix(t.tkey_to_metrics)
	fix(t.tkey_to_tkeys)
	fix(t.tkey_to_tags)
	fix(t.tkey_to_tvals)

	t.metrics = hashtbl.T[histdb.Hash, rwId]{}

	t.fixed = true
}

func (t *T) Find(metric []byte) (histdb.Hash, Id, bool) {
	if len(metric) == 0 || bytes.Count(metric, []byte{','}) >= 256 {
		return histdb.Hash{}, 0, false
	}

	tkeyis := make([]Id, 0, 8)
	tagis := make(tkeySet, 0, 8)
	var tkeyus map[Id]struct{}
	var hash histdb.Hash

	mhp := hash.TagHashPtr()
	thp := hash.TagKeyHashPtr()

	for rest := metric; len(rest) > 0; {
		var tkey, tag []byte
		tkey, tag, _, rest = metrics.PopTag(rest)
		if len(tag) == 0 {
			continue
		}

		tkeyh := histdb.NewTagKeyHash(tkey)
		tkeyi, ok := t.tkey_names.Find(tkeyh)
		if !ok {
			return histdb.Hash{}, 0, false
		}

		tkeyis, tkeyus, ok = addSet(tkeyis, tkeyus, Id(tkeyi))
		if !ok {
			continue
		}

		tagh := histdb.NewTagHash(tag)
		tagi, ok := t.tag_names.Find(tagh)
		if !ok {
			return histdb.Hash{}, 0, false
		}

		tagis = append(tagis, Id(tagi))

		thp.Add(tkeyh)
		mhp.Add(tagh)
	}

	// so ok, we have a set of tags. intersecting on tag_to_metrics
	// for each tag returns a bitmap that contains all of the metrics
	// with at least those tags. then check if the metric is in
	// tkeys_to_metrics for the tag key set, implying that it has
	// exactly those tags.

	sort.Slice(tkeyis, func(i, j int) bool { return tkeyis[i] < tkeyis[j] })
	tkeyisi, ok := t.tkeys_names.Find(tkeyis)
	if !ok {
		return hash, 0, false
	}

	bm := acquireBitmap()
	defer replaceBitmap(bm)

	bm.Or(t.tkeys_to_metrics[tkeyisi])
	for _, tagi := range tagis {
		bm.And(t.tag_to_metrics[tagi])
	}

	if bm.GetCardinality() == 1 {
		return hash, bm.Minimum(), true
	}

	return histdb.Hash{}, 0, false
}

func (t *T) Add(metric []byte) (histdb.Hash, Id, bool) {
	if len(metric) == 0 || t.fixed || bytes.Count(metric, []byte{','}) >= 256 {
		return histdb.Hash{}, 0, false
	}

	tkeyis := make(tkeySet, 0, 8)
	tagis := make([]Id, 0, 8)
	var tkeyus map[Id]struct{}
	var hash histdb.Hash

	mhp := hash.TagHashPtr()
	thp := hash.TagKeyHashPtr()

	for rest := metric; len(rest) > 0; {
		var tkey, tag []byte
		tkey, tag, _, rest = metrics.PopTag(rest)
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

	// TODO: why was this check here? i'm spooked lol
	// if t.metrics.Len() > 1<<31-1 {
	// 	return hash, false
	// }

	metrici, ok := t.metrics.Insert(hash, rwId(t.metrics.Len()))
	if ok {
		return hash, Id(metrici), false
	}

	tkeyisSorted := append(tkeySet{}, tkeyis...)
	sort.Slice(tkeyisSorted, func(i, j int) bool {
		return tkeyisSorted[i] < tkeyisSorted[j]
	})

	tkeyisi, _ := t.tkeys_names.Insert(tkeyisSorted, rwId(t.tkeys_names.Len()))
	getBitmap(&t.tkeys_to_metrics, Id(tkeyisi)).Add(Id(metrici))

	t.card++

	for i := range tagis {
		tagi := tagis[i]
		tkeyi := tkeyis[i]

		getBitmap(&t.tag_to_metrics, tagi).Add(Id(metrici))   // tagis[i] should know about metric
		getBitmap(&t.tag_to_tkeys, tagi).AddMany(tkeyis)      // tagis[i] should know about every other tkeyis
		getBitmap(&t.tag_to_tags, tagi).AddMany(tagis)        // tagis[i] should know about every other tagis
		getBitmap(&t.tkey_to_tkeys, tkeyi).AddMany(tkeyis)    // tkeys[i] should know about every other tkeyis
		getBitmap(&t.tkey_to_tags, tkeyi).AddMany(tagis)      // tkeys[i] should know about every other tagis[i]
		getBitmap(&t.tkey_to_tvals, tkeyi).Add(tagis[i])      // tkeys[i] should know about tagis[i]
		getBitmap(&t.tkey_to_metrics, tkeyi).Add(Id(metrici)) // tkeys[i] should know about metric
	}

	return hash, Id(metrici), true
}

func (t *T) EncodeInto(metric []byte, out []Id) ([]Id, bool) {
	if len(metric) == 0 {
		return nil, false
	}

	tkeyis := make([]Id, 0, 8)
	var tkeyus map[Id]struct{}

	for rest := metric; len(rest) > 0; {
		var tkey, tag []byte
		tkey, tag, _, rest = metrics.PopTag(rest)
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

func (t *T) DecodeInto(tagis []Id, buf []byte) ([]byte, bool) {
	if len(tagis) == 0 || len(tagis) >= 256 {
		return nil, false
	}

	for i, tagi := range tagis {
		tag := t.tag_names.Get(rwId(tagi))
		if tag == nil {
			return nil, false
		}

		if i > 0 {
			buf = append(buf, ',')
		}
		buf = append(buf, tag...)
	}

	return buf, true
}

func (t *T) Cardinality() int { return t.card }

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

	sort.Slice(tagis, func(i, j int) bool {
		tagi := t.tag_names.Get(rwId(tagis[i]))
		tagj := t.tag_names.Get(rwId(tagis[j]))
		return string(tagi) < string(tagj)
	})

	return t.DecodeInto(tagis, buf)
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
		tkey, _, _, tkeys = metrics.PopTag(tkeys)

		tkeyn, ok := t.tkey_names.Find(histdb.NewTagKeyHash(tkey))
		if !ok {
			cb(new(Bitmap))
			return
		}

		bms = append(bms, t.tkey_to_metrics[tkeyn])
	}

	cb(parOr(bms...))
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

	m := acquireBitmap()
	defer replaceBitmap(m)

	m.Or(t.tkey_to_metrics[tkeyn])
	m.AndNot(t.tag_to_metrics[tagn])

	cb(m)
}

func tagValue(tkey, tag []byte) []byte {
	if len(tag) > len(tkey) {
		return tag[len(tkey)+1:]
	}
	return nil
}

func (t *T) QueryFilter(tkey []byte, fn func([]byte) bool, cb func(*Bitmap)) {
	tkeyn, ok := t.tkey_names.Find(histdb.NewTagKeyHash(tkey))
	if !ok {
		cb(new(Bitmap))
		return
	}

	var bms []*Bitmap

	Iter(t.tkey_to_tvals[tkeyn], func(tagn Id) bool {
		if fn(tagValue(tkey, t.tag_names.Get(rwId(tagn)))) {
			bms = append(bms, t.tag_to_metrics[tagn])
		}
		return true
	})

	cb(parOr(bms...))
}

func (t *T) QueryFilterNot(tkey []byte, fn func([]byte) bool, cb func(*Bitmap)) {
	tkeyn, ok := t.tkey_names.Find(histdb.NewTagKeyHash(tkey))
	if !ok {
		cb(new(Bitmap))
		return
	}

	var bms []*Bitmap

	Iter(t.tkey_to_tvals[tkeyn], func(tagn Id) bool {
		if !fn(tagValue(tkey, t.tag_names.Get(rwId(tagn)))) {
			bms = append(bms, t.tag_to_metrics[tagn])
		}
		return true
	})

	cb(parOr(bms...))
}

func (t *T) TagKeys(input []byte, cb func(result []byte) bool) {
	// TODO: look at using ParOr

	tkbm := acquireBitmap()
	defer replaceBitmap(tkbm)

	mbm := acquireBitmap()
	defer replaceBitmap(mbm)

	for rest := input; len(rest) > 0; {
		var (
			tag, tkey   []byte
			isKey       bool
			ltkbm, lmbm *Bitmap
		)

		tkey, tag, isKey, rest = metrics.PopTag(rest)
		if len(tag) == 0 {
			continue
		}

		tkeyn, ok := t.tkey_names.Find(histdb.NewTagKeyHash(tkey))
		if !ok {
			return
		}

		if isKey {
			ltkbm = t.tkey_to_tkeys[tkeyn]
			lmbm = t.tkey_to_metrics[tkeyn]
		} else {
			name, ok := t.tag_names.Find(histdb.NewTagHash(tag))
			if !ok {
				return
			}
			ltkbm = t.tag_to_tkeys[name]
			lmbm = t.tag_to_metrics[name]
		}

		if mbm.IsEmpty() {
			tkbm.Or(ltkbm)
			mbm.Or(lmbm)
		} else {
			tkbm.And(ltkbm)
			mbm.And(lmbm)
		}

		tkbm.Remove(Id(tkeyn))

		if tkbm.IsEmpty() || mbm.IsEmpty() {
			return
		}
	}

	// the only way it's here and still empty is if the input query was empty
	if mbm.IsEmpty() {
		for i := 0; i < t.tkey_names.Len(); i++ {
			if !cb(t.tkey_names.Get(rwId(i))) {
				return
			}
		}
		return
	}

	Iter(tkbm, func(name Id) bool {
		if mbm != nil && !mbm.Intersects(t.tkey_to_metrics[name]) {
			return true
		}
		return cb(t.tkey_names.Get(rwId(name)))
	})
}

func (t *T) TagValues(input, tkey []byte, cb func(result []byte) bool) {
	// TODO: look at using ParOr

	name, ok := t.tkey_names.Find(histdb.NewTagKeyHash(tkey))
	if !ok {
		return
	}

	tbm := acquireBitmap()
	defer replaceBitmap(tbm)

	mbm := acquireBitmap()
	defer replaceBitmap(mbm)

	for rest := input; len(rest) > 0; {
		var tag, tkey []byte
		var isKey bool
		var ltbm, lmbm *Bitmap

		tkey, tag, isKey, rest = metrics.PopTag(rest)
		if len(tag) == 0 {
			continue
		}

		if isKey {
			name, ok := t.tkey_names.Find(histdb.NewTagKeyHash(tkey))
			if !ok {
				return
			}
			ltbm = t.tkey_to_tags[name]
			lmbm = t.tkey_to_metrics[name]
		} else {
			name, ok := t.tag_names.Find(histdb.NewTagHash(tag))
			if !ok {
				return
			}
			ltbm = t.tag_to_tags[name]
			lmbm = t.tag_to_metrics[name]
		}

		if mbm.IsEmpty() {
			tbm.Or(ltbm)
			mbm.Or(lmbm)
		} else {
			tbm.And(ltbm)
			mbm.And(lmbm)
		}

		if tbm.IsEmpty() || mbm.IsEmpty() {
			return
		}
	}

	// the only way it's here and still empty is if the input query was empty
	if mbm.IsEmpty() {
		tbm = t.tkey_to_tvals[name]
		mbm = nil
	} else {
		tbm.And(t.tkey_to_tvals[name])
	}

	Iter(tbm, func(name Id) bool {
		tag := t.tag_names.Get(rwId(name))
		if len(tag) <= len(tkey) {
			return true
		} else if mbm != nil && !mbm.Intersects(t.tag_to_metrics[name]) {
			return true
		}
		return cb(tagValue(tkey, tag))
	})
}
