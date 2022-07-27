package memindex

import (
	"encoding/binary"
	"fmt"
	"sync"
	"unsafe"

	"github.com/RoaringBitmap/roaring"
	"github.com/zeebo/xxh3"

	"github.com/histdb/histdb"
	"github.com/histdb/histdb/hashtbl"
	"github.com/histdb/histdb/metrics"
	"github.com/histdb/histdb/petname"
)

// TODO: we can have an LRU cache of common bitmaps based on tag hashes. for example
// we always compute the tag_to_metrics intersection bitmap. if we do it smart, we can
// keep track of the "path" along the way. this would make subsequent queries that
// have the same prefix faster.

var le = binary.LittleEndian

var queryPool = sync.Pool{New: func() interface{} { return roaring.New() }}

func replaceBitmap(m *roaring.Bitmap) {
	queryPool.Put(m)
}

func acquireBitmap() *roaring.Bitmap {
	bm := queryPool.Get().(*roaring.Bitmap)
	if !bm.IsEmpty() {
		bm.Clear()
	}
	return bm
}

func addSet[T comparable](l []T, s map[T]struct{}, v T) ([]T, map[T]struct{}, bool) {
	if s != nil {
		if _, ok := s[v]; ok {
			return l, s, false
		}
		l = append(l, v)
		s[v] = struct{}{}
		return l, s, true
	}

	for _, u := range l {
		if u == v {
			return l, s, false
		}
	}

	l = append(l, v)
	if len(l) == cap(l) {
		s = make(map[T]struct{})
		for _, u := range l {
			s[u] = struct{}{}
		}
	}

	return l, s, true
}

type T struct {
	fixed bool
	card  int

	metric_set    hashtbl.T[histdb.Hash, *histdb.Hash]
	metric_hashes []histdb.Hash

	tag_names  petname.T
	tkey_names petname.T

	tag_to_metrics  []*roaring.Bitmap
	tag_to_tkeys    []*roaring.Bitmap
	tag_to_tags     []*roaring.Bitmap
	tkey_to_metrics []*roaring.Bitmap
	tkey_to_tkeys   []*roaring.Bitmap
	tkey_to_tags    []*roaring.Bitmap // all other tags associated with tkey
	tkey_to_tvals   []*roaring.Bitmap // only tags with tkey as the tag key
}

func (t *T) find(v string, names *petname.T) (uint32, bool) {
	return names.Find(xxh3.HashString(v))
}

func sliceSize(m []*roaring.Bitmap) (n uint64) {
	for _, bm := range m {
		n += bm.GetSizeInBytes()
	}
	return 24 + n + 8*uint64(len(m))
}

func (t *T) Size() uint64 {
	return 0 +
		/* fixed           */ 8 +
		/* card            */ 8 +
		/* metric_set      */ t.metric_set.Size() +
		/* metric_hashes   */ 24 + uint64(unsafe.Sizeof(histdb.Hash{}))*uint64(len(t.metric_hashes)) +
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

	t.metric_set = hashtbl.T[histdb.Hash, *histdb.Hash]{}

	t.fixed = true
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

func (t *T) Add(metric string) (histdb.Hash, bool) {
	tkeyis := make([]uint32, 0, 8)
	tagis := make([]uint32, 0, 8)
	var tkeyus map[uint32]struct{}
	var hash histdb.Hash

	for rest := metric; len(rest) > 0; {
		var tkey, tag string
		tkey, tag, _, rest = metrics.PopTag(rest)
		if len(tag) == 0 {
			continue
		}

		tkeyh := xxh3.HashString(tkey)
		tkeyi := t.tkey_names.Put(tkeyh, tkey)

		var ok bool
		tkeyis, tkeyus, ok = addSet(tkeyis, tkeyus, tkeyi)

		if ok {
			th := le.Uint64(hash.TagHashPtr()[:])
			le.PutUint64(hash.TagHashPtr()[:], th+tkeyh)

			tagh := xxh3.HashString(tag)
			mh := le.Uint64(hash.MetricHashPtr()[:])
			le.PutUint64(hash.MetricHashPtr()[:], mh+tagh)

			tagi := t.tag_names.Put(tagh, tag)
			tagis = append(tagis, tagi)
		}
	}

	if t.fixed || t.metric_set.Len() > 1<<31-1 {
		return hash, false
	}

	metrici, ok := t.metric_set.Insert(hash, uint32(t.metric_set.Len()))
	if ok {
		return hash, false
	}

	t.metric_hashes = append(t.metric_hashes, hash)
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

func (t *T) TagKeys(input string, cb func(result string) bool) {
	tkbm := acquireBitmap()
	defer replaceBitmap(tkbm)

	mbm := acquireBitmap()
	defer replaceBitmap(mbm)

	for rest := input; len(rest) > 0; {
		var (
			tag, tkey   string
			isKey       bool
			ltkbm, lmbm *roaring.Bitmap
		)

		tkey, tag, isKey, rest = metrics.PopTag(rest)
		if len(tag) == 0 {
			continue
		}

		tkeyn, ok := t.find(tkey, &t.tkey_names)
		if !ok {
			return
		}

		if isKey {
			ltkbm = t.tkey_to_tkeys[tkeyn]
			lmbm = t.tkey_to_metrics[tkeyn]
		} else {
			name, ok := t.find(tag, &t.tag_names)
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

func (t *T) TagValues(input, tkey string, cb func(result string) bool) {
	name, ok := t.find(tkey, &t.tkey_names)
	if !ok {
		return
	}

	tbm := acquireBitmap()
	defer replaceBitmap(tbm)

	mbm := acquireBitmap()
	defer replaceBitmap(mbm)

	for rest := input; len(rest) > 0; {
		var tag, tkey string
		var isKey bool
		var ltbm, lmbm *roaring.Bitmap

		tkey, tag, isKey, rest = metrics.PopTag(rest)
		if len(tag) == 0 {
			continue
		}

		if isKey {
			name, ok := t.find(tkey, &t.tkey_names)
			if !ok {
				return
			}
			ltbm = t.tkey_to_tags[name]
			lmbm = t.tkey_to_metrics[name]
		} else {
			name, ok := t.find(tag, &t.tag_names)
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
		return cb(tag[len(tkey)+1:])
	})
}
