package memindex

import (
	"fmt"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/zeebo/xxh3"

	"github.com/histdb/histdb/petname"
)

// TODO: we can have an LRU cache of common bitmaps based on tag hashes. for example
// we always compute the tag_to_metrics intersection bitmap. if we do it smart, we can
// keep track of the "path" along the way. this would make subsequent queries that
// have the same prefix faster.

type T struct {
	fixed bool
	card  int

	metric_names *petname.T
	tag_names    *petname.T
	tkey_names   *petname.T

	tag_to_metrics  []*roaring.Bitmap
	tag_to_tkeys    []*roaring.Bitmap
	tag_to_tags     []*roaring.Bitmap
	tkey_to_metrics []*roaring.Bitmap
	tkey_to_tkeys   []*roaring.Bitmap
	tkey_to_tags    []*roaring.Bitmap // all other tags associated with tkey
	tkey_to_tvals   []*roaring.Bitmap // only tags with tkey as the tag key

	query_pool sync.Pool
}

func New() *T {
	return &T{
		metric_names: petname.New(),
		tag_names:    petname.New(),
		tkey_names:   petname.New(),

		query_pool: sync.Pool{New: func() interface{} { return roaring.New() }},
	}
}

func (t *T) find(v string, names *petname.T) (uint32, bool) {
	return names.Find(petname.Hash(xxh3.HashString128(v)))
}

func (t *T) replaceBitmap(m *roaring.Bitmap) {
	t.query_pool.Put(m)
}

func (t *T) acquireBitmap() *roaring.Bitmap {
	bm := t.query_pool.Get().(*roaring.Bitmap)
	if !bm.IsEmpty() {
		bm.Clear()
	}
	return bm
}

func sliceSize(m []*roaring.Bitmap) (n uint64) {
	for _, bm := range m {
		n += bm.GetSizeInBytes()
	}
	return n + 8*uint64(len(m))
}

func (t *T) Size() uint64 {
	return 0 +
		t.metric_names.Size() +
		t.tag_names.Size() +
		t.tkey_names.Size() +
		sliceSize(t.tag_to_metrics) +
		sliceSize(t.tag_to_tkeys) +
		sliceSize(t.tag_to_tags) +
		sliceSize(t.tkey_to_metrics) +
		sliceSize(t.tkey_to_tkeys) +
		sliceSize(t.tkey_to_tags) +
		sliceSize(t.tkey_to_tvals) +
		0
}

func (t *T) Fix() {
	fix := func(bms []*roaring.Bitmap) {
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

	t.metric_names = nil

	t.fixed = true
}

func (t *T) Hash(metric string) petname.Hash {
	var mhash petname.Hash

	for rest := metric; len(rest) > 0; {
		var tag string
		_, tag, _, rest = popTag(rest)
		if len(tag) == 0 {
			continue
		}

		hash := xxh3.HashString128(tag)
		mhash.Hi += hash.Hi
		mhash.Lo += hash.Lo
	}

	return mhash
}

func getBitmap(bmsp *[]*roaring.Bitmap, n uint32) (bm *roaring.Bitmap) {
	if bms := *bmsp; n < uint32(len(bms)) {
		bm = bms[n]
	} else if n == uint32(len(bms)) {
		bm = roaring.New()
		*bmsp = append(bms, bm)
	} else {
		panic(fmt.Sprintf("petname non-monotonic: req=%d len=%d", n, len(bms)))
	}
	return bm
}

func (t *T) Add(metric string) bool {
	if t.fixed {
		return false
	}

	tkeyis := make([]uint32, 0, 8)
	tagis := make([]uint32, 0, 8)
	var tagus map[uint32]struct{}
	var mhash petname.Hash

	for rest := metric; len(rest) > 0; {
		var tkey, tag string
		tkey, tag, _, rest = popTag(rest)
		if len(tag) == 0 {
			continue
		}

		tagh := petname.Hash(xxh3.HashString128(tag))
		tagi, _ := t.tag_names.Put(tagh, tag)

		var ok bool
		tagis, tagus, ok = addUint32Set(tagis, tagus, tagi)

		if ok {
			tkeyh := petname.Hash(xxh3.HashString128(tkey))
			tkeyi, _ := t.tkey_names.Put(tkeyh, tkey)
			tkeyis = append(tkeyis, tkeyi)

			mhash.Hi += tagh.Hi
			mhash.Lo += tagh.Lo
		}
	}

	//
	// yowzer. now update all the things.
	//

	metrici, ok := t.metric_names.Put(mhash, "")
	if ok {
		return false
	}
	t.card++

	for i := range tagis {
		getBitmap(&t.tag_to_metrics, tagis[i]).Add(metrici)    // tagis[i] should know about metric
		getBitmap(&t.tag_to_tkeys, tagis[i]).AddMany(tkeyis)   // tagis[i] should know about every other tkeyis
		getBitmap(&t.tag_to_tags, tagis[i]).AddMany(tagis)     // tagis[i] should know about every other tagis
		getBitmap(&t.tkey_to_tkeys, tkeyis[i]).AddMany(tkeyis) // tkeys[i] should know about every other tkeyis
		getBitmap(&t.tkey_to_tags, tkeyis[i]).AddMany(tagis)   // tkeys[i] should know about every other tagis[i]
		getBitmap(&t.tkey_to_tvals, tkeyis[i]).Add(tagis[i])   // tkeys[i] should know about tagis[i]
		getBitmap(&t.tkey_to_metrics, tkeyis[i]).Add(metrici)  // tkeys[i] should know about metric
	}

	return true
}

func (t *T) Count(input string) int {
	var metrics *roaring.Bitmap

	for rest := input; len(rest) > 0; {
		var tag, tkey string
		var isKey bool
		var bm *roaring.Bitmap

		tkey, tag, isKey, rest = popTag(rest)
		if len(tag) == 0 {
			continue
		}

		if isKey {
			name, ok := t.find(tkey, t.tkey_names)
			if !ok {
				return 0
			}
			bm = t.tkey_to_metrics[name]
		} else {
			name, ok := t.find(tag, t.tag_names)
			if !ok {
				return 0
			}
			bm = t.tag_to_metrics[name]
		}

		if metrics == nil {
			metrics = t.acquireBitmap()
			defer t.replaceBitmap(metrics)
		}

		if metrics.IsEmpty() {
			metrics.Or(bm)
		} else {
			metrics.And(bm)
		}

		if metrics.IsEmpty() {
			return 0
		}
	}

	// the only way it's here and still empty is if the input query was empty
	if metrics == nil || metrics.IsEmpty() {
		return t.card
	}

	return int(metrics.GetCardinality())
}

func (t *T) TagKeys(input string, cb func(result string) bool) {
	tkeys := t.acquireBitmap()
	defer t.replaceBitmap(tkeys)

	metrics := t.acquireBitmap()
	defer t.replaceBitmap(metrics)

	for rest := input; len(rest) > 0; {
		var (
			tag, tkey string
			isKey     bool
			bmk, bmm  *roaring.Bitmap
		)

		tkey, tag, isKey, rest = popTag(rest)
		if len(tag) == 0 {
			continue
		}

		tkeyn, ok := t.find(tkey, t.tkey_names)
		if !ok {
			return
		}

		if isKey {
			bmk = t.tkey_to_tkeys[tkeyn]
			bmm = t.tkey_to_metrics[tkeyn]
		} else {
			name, ok := t.find(tag, t.tag_names)
			if !ok {
				return
			}
			bmk = t.tag_to_tkeys[name]
			bmm = t.tag_to_metrics[name]
		}

		if metrics.IsEmpty() {
			tkeys.Or(bmk)
			metrics.Or(bmm)
		} else {
			tkeys.And(bmk)
			metrics.And(bmm)
		}

		tkeys.Remove(tkeyn)

		if tkeys.IsEmpty() || metrics.IsEmpty() {
			return
		}
	}

	// the only way it's here and still empty is if the input query was empty
	if metrics.IsEmpty() {
		for i := 0; i < t.tkey_names.Len(); i++ {
			if !cb(t.tkey_names.Get(uint32(i))) {
				return
			}
		}
		return
	}

	tkeys.Iterate(func(name uint32) bool {
		if metrics != nil && !metrics.Intersects(t.tkey_to_metrics[name]) {
			return true
		}
		return cb(t.tkey_names.Get(name))
	})
}

func (t *T) TagValues(input, tkey string, cb func(result string) bool) {
	name, ok := t.find(tkey, t.tkey_names)
	if !ok {
		return
	}

	tags := t.acquireBitmap()
	defer t.replaceBitmap(tags)

	metrics := t.acquireBitmap()
	defer t.replaceBitmap(metrics)

	for rest := input; len(rest) > 0; {
		var tag, tkey string
		var isKey bool
		var bmt, bmm *roaring.Bitmap

		tkey, tag, isKey, rest = popTag(rest)
		if len(tag) == 0 {
			continue
		}

		if isKey {
			name, ok := t.find(tkey, t.tkey_names)
			if !ok {
				return
			}
			bmt = t.tkey_to_tags[name]
			bmm = t.tkey_to_metrics[name]
		} else {
			name, ok := t.find(tag, t.tag_names)
			if !ok {
				return
			}
			bmt = t.tag_to_tags[name]
			bmm = t.tag_to_metrics[name]
		}

		if metrics.IsEmpty() {
			tags.Or(bmt)
			metrics.Or(bmm)
		} else {
			tags.And(bmt)
			metrics.And(bmm)
		}

		if tags.IsEmpty() || metrics.IsEmpty() {
			return
		}
	}

	// the only way it's here and still empty is if the input query was empty
	if metrics.IsEmpty() {
		tags = t.tkey_to_tvals[name]
		metrics = nil
	} else {
		tags.And(t.tkey_to_tvals[name])
	}

	tags.Iterate(func(name uint32) bool {
		tag := t.tag_names.Get(name)
		if len(tag) <= len(tkey) {
			return true
		} else if metrics != nil && !metrics.Intersects(t.tag_to_metrics[name]) {
			return true
		}
		return cb(tag[len(tkey)+1:])
	})
}
