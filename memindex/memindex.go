package memindex

import (
	"bytes"
	"sort"
	"strings"

	"github.com/RoaringBitmap/roaring"

	"github.com/histdb/histdb"
	"github.com/histdb/histdb/hashset"
	"github.com/histdb/histdb/metrics"
	"github.com/histdb/histdb/petname"
)

// TODO: we can have an LRU cache of common bitmaps based on tag hashes. for example
// we always compute the tag_to_metrics intersection bitmap. if we do it smart, we can
// keep track of the "path" along the way. this would make subsequent queries that
// have the same prefix faster.

type T struct {
	fixed bool
	card  int

	metrics    hashset.T[histdb.Hash]
	tag_names  petname.T[histdb.TagHash]
	tkey_names petname.T[histdb.TagKeyHash]

	tag_to_metrics  []*Bitmap // what metrics include this tag
	tag_to_tkeys    []*Bitmap // what tag keys exist in any metric with tag
	tag_to_tags     []*Bitmap // what tags exist in any metric with tag
	tkey_to_metrics []*Bitmap // what metrics include this tag key
	tkey_to_tkeys   []*Bitmap // what tag keys exist in any metric with tag key
	tkey_to_tags    []*Bitmap // what tags exist in any metric with tag key
	tkey_to_tvals   []*Bitmap // what tags exist for the specific tag key in any metric with tag key
}

func (t *T) Size() uint64 {
	return 0 +
		/* fixed           */ 8 +
		/* card            */ 8 +
		/* hash_set        */ t.metrics.Size() +
		/* tag_names       */ t.tag_names.Size() +
		/* tkey_names      */ t.tkey_names.Size() +
		/* tag_to_metrics  */ sliceSize(t.tag_to_metrics) +
		/* tag_to_tkeys    */ sliceSize(t.tag_to_tkeys) +
		/* tag_to_tags     */ sliceSize(t.tag_to_tags) +
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
	fix(t.tkey_to_metrics)
	fix(t.tkey_to_tkeys)
	fix(t.tkey_to_tags)
	fix(t.tkey_to_tvals)

	t.metrics.Fix()

	t.fixed = true
}

func (t *T) Add(metric []byte) (histdb.Hash, bool) {
	tkeyis := make([]uint32, 0, 8)
	tagis := make([]uint32, 0, 8)
	var tkeyus map[uint32]struct{}
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
		tkeyis, tkeyus, ok = addSet(tkeyis, tkeyus, tkeyi)

		if ok {
			tagh := histdb.NewTagHash(tag)

			thp.Add(tkeyh)
			mhp.Add(tagh)

			tagi := t.tag_names.Put(tagh, tag)
			tagis = append(tagis, tagi)
		}
	}

	if t.fixed || t.metrics.Len() > 1<<31-1 {
		return hash, false
	}

	metrici, ok := t.metrics.Insert(hash)
	if ok {
		return hash, false
	}

	t.card++

	for i := range tagis {
		tagi := tagis[i]
		tkeyi := tkeyis[i]

		getBitmap(&t.tag_to_metrics, tagi).Add(metrici)    // tagis[i] should know about metric
		getBitmap(&t.tag_to_tkeys, tagi).AddMany(tkeyis)   // tagis[i] should know about every other tkeyis
		getBitmap(&t.tag_to_tags, tagi).AddMany(tagis)     // tagis[i] should know about every other tagis
		getBitmap(&t.tkey_to_tkeys, tkeyi).AddMany(tkeyis) // tkeys[i] should know about every other tkeyis
		getBitmap(&t.tkey_to_tags, tkeyi).AddMany(tagis)   // tkeys[i] should know about every other tagis[i]
		getBitmap(&t.tkey_to_tvals, tkeyi).Add(tagis[i])   // tkeys[i] should know about tagis[i]
		getBitmap(&t.tkey_to_metrics, tkeyi).Add(metrici)  // tkeys[i] should know about metric
	}

	return hash, true
}

func (t *T) Cardinality() int { return t.card }

func (t *T) SlowReverseMetricName(n uint32) string {
	var out []string
	for tkeyn, tkeybm := range t.tkey_to_metrics {
		if !tkeybm.Contains(n) {
			continue
		}
		t.tkey_to_tvals[tkeyn].Iterate(func(tagn uint32) bool {
			if t.tag_to_metrics[tagn].Contains(n) {
				out = append(out, string(t.tag_names.Get(uint32(tagn))))
			}
			return true
		})
	}
	sort.Strings(out)
	return strings.Join(out, ",")
}

func (t *T) MetricHashes(metrics *Bitmap, cb func(uint32, histdb.Hash) bool) {
	metrics.Iterate(func(metricn uint32) bool {
		return cb(metricn, t.metrics.Hash(metricn))
	})
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

	cb(roaring.ParOr(orParallelism, bms...))
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

	t.tkey_to_tvals[tkeyn].Iterate(func(tagn uint32) bool {
		if fn(tagValue(tkey, t.tag_names.Get(tagn))) {
			bms = append(bms, t.tag_to_metrics[tagn])
		}
		return true
	})

	cb(roaring.ParOr(orParallelism, bms...))
}

func (t *T) QueryFilterNot(tkey []byte, fn func([]byte) bool, cb func(*Bitmap)) {
	tkeyn, ok := t.tkey_names.Find(histdb.NewTagKeyHash(tkey))
	if !ok {
		cb(new(Bitmap))
		return
	}

	var bms []*Bitmap

	t.tkey_to_tvals[tkeyn].Iterate(func(tagn uint32) bool {
		if !fn(tagValue(tkey, t.tag_names.Get(tagn))) {
			bms = append(bms, t.tag_to_metrics[tagn])
		}
		return true
	})

	cb(roaring.ParOr(orParallelism, bms...))
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

		tkbm.Remove(tkeyn)

		if tkbm.IsEmpty() || mbm.IsEmpty() {
			return
		}
	}

	// the only way it's here and still empty is if the input query was empty
	if mbm.IsEmpty() {
		for i := 0; i < t.tkey_names.Len(); i++ {
			if !cb(t.tkey_names.Get(uint32(i))) {
				return
			}
		}
		return
	}

	tkbm.Iterate(func(name uint32) bool {
		if mbm != nil && !mbm.Intersects(t.tkey_to_metrics[name]) {
			return true
		}
		return cb(t.tkey_names.Get(name))
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

	tbm.Iterate(func(name uint32) bool {
		tag := t.tag_names.Get(name)
		if len(tag) <= len(tkey) {
			return true
		} else if mbm != nil && !mbm.Intersects(t.tag_to_metrics[name]) {
			return true
		}
		return cb(tagValue(tkey, tag))
	})
}
