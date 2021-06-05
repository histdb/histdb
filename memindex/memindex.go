package memindex

import (
	"fmt"
	"strings"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/zeebo/xxh3"

	"github.com/histdb/histdb/memindex/petname"
)

// TODO: we can have an LRU cache of common bitmaps based on tag hashes. for example
// we always compute the tag_to_metrics intersection bitmap. if we do it smart, we can
// keep track of the "path" along the way.

type T struct {
	fixed bool

	metric_names *petname.Uint32s
	tag_names    *petname.Strings
	tkey_names   *petname.Strings

	metrics *roaring.Bitmap
	tags    *roaring.Bitmap
	tkeys   *roaring.Bitmap

	tag_to_metrics  []*roaring.Bitmap
	tag_to_tkeys    []*roaring.Bitmap
	tag_to_tags     []*roaring.Bitmap
	tkey_to_tags    []*roaring.Bitmap
	tkey_to_metrics []*roaring.Bitmap

	query_pool sync.Pool
}

func New() *T {
	return &T{
		metric_names: petname.NewUint32s(),
		tag_names:    petname.NewStrings(),
		tkey_names:   petname.NewStrings(),

		metrics: roaring.New(),
		tags:    roaring.New(),
		tkeys:   roaring.New(),

		query_pool: sync.Pool{New: func() interface{} { return roaring.New() }},
	}
}

func (t *T) find(v string, names *petname.Strings) (uint32, bool) {
	h := xxh3.HashString128(v)
	return names.Find(petname.Hash{h[0], h[1]})
}

func (t *T) replaceBitmap(m *roaring.Bitmap) {
	m.Clear()
	t.query_pool.Put(m)
}

func (t *T) acquireBitmap() *roaring.Bitmap {
	return t.query_pool.Get().(*roaring.Bitmap)
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
		t.metrics.GetSizeInBytes() +
		t.tags.GetSizeInBytes() +
		t.tkeys.GetSizeInBytes() +
		sliceSize(t.tag_to_metrics) +
		sliceSize(t.tag_to_tkeys) +
		sliceSize(t.tag_to_tags) +
		sliceSize(t.tkey_to_tags) +
		sliceSize(t.tkey_to_metrics) +
		0
}

func (t *T) Fix() {
	fix := func(bm *roaring.Bitmap) {
		bm.RunOptimize()
	}

	fix(t.metrics)
	fix(t.tags)
	fix(t.tkeys)

	for _, bm := range t.tag_to_metrics {
		fix(bm)
	}
	for _, bm := range t.tkey_to_tags {
		fix(bm)
	}
	for _, bm := range t.tag_to_tkeys {
		fix(bm)
	}
	for _, bm := range t.tag_to_tags {
		fix(bm)
	}

	t.metric_names.Fix()

	t.fixed = true
}

func (t *T) Hash(metric string) petname.Hash {
	var mhash petname.Hash

	for rest := metric; len(rest) > 0; {
		var tag string
		_, tag, rest = popTag(rest)
		if len(tag) == 0 {
			continue
		}

		hash := xxh3.HashString128(tag)
		mhash.H += hash[0]
		mhash.L += hash[1]
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
		tkey, tag, rest = popTag(rest)
		if len(tag) == 0 {
			continue
		}

		tagh := xxh3.HashString128(tag)
		tagi := t.tag_names.Put(petname.Hash{tagh[0], tagh[1]}, tag)

		var ok bool
		tagis, tagus, ok = addUint32Set(tagis, tagus, tagi)

		if ok {
			tkeyh := xxh3.HashString128(tkey)
			tkeyi := t.tkey_names.Put(petname.Hash{tkeyh[0], tkeyh[1]}, tkey)
			tkeyis = append(tkeyis, tkeyi)

			mhash.H += tagh[0]
			mhash.L += tagh[1]
		}
	}

	//
	// yowzer. now update all the things.
	//

	metrici, ok := t.metric_names.Put(mhash, tagis)
	if ok {
		return false
	}

	for i := range tagis {
		// tagis[i] should know about metric
		getBitmap(&t.tag_to_metrics, tagis[i]).Add(metrici)

		// tagis[i] should know about every other tkey
		{
			bm := getBitmap(&t.tag_to_tkeys, tagis[i])
			for j := range tkeyis {
				if tkeyis[i] != tkeyis[j] {
					bm.Add(tkeyis[j])
				}
			}
		}

		// tagis[i] should know about every other tagis
		{
			bm := getBitmap(&t.tag_to_tags, tagis[i])
			for j := range tagis {
				if tagis[i] != tagis[j] {
					bm.Add(tagis[j])
				}
			}
		}

		// tkeys[i] should know about tagis[i]
		getBitmap(&t.tkey_to_tags, tkeyis[i]).Add(tagis[i])

		// tkeys[i] should know about metric
		getBitmap(&t.tkey_to_metrics, tkeyis[i]).Add(metrici)
	}

	// record to all of our base bitmaps
	t.metrics.Add(metrici)
	t.tags.AddMany(tagis)
	t.tkeys.AddMany(tkeyis)

	return true
}

func (t *T) Count(input string) int {
	metrics := t.acquireBitmap()
	defer t.replaceBitmap(metrics)

	for rest := input; len(rest) > 0; {
		var tag string
		_, tag, rest = popTag(rest)
		if len(tag) == 0 {
			continue
		}

		name, ok := t.find(tag, t.tag_names)
		if !ok {
			return 0
		}

		ttm := t.tag_to_metrics[name]

		if metrics.IsEmpty() {
			metrics.Or(ttm)
		} else {
			metrics.And(ttm)
		}

		if metrics.IsEmpty() {
			return 0
		}
	}

	// the only way it's here and still empty is if the input query was empty
	if metrics.IsEmpty() {
		metrics = t.metrics
	}

	return int(metrics.GetCardinality())
}

func (t *T) Metrics(input string, buf []byte, cb func(buf []byte) bool) {
	metrics := t.acquireBitmap()
	defer t.replaceBitmap(metrics)

	for rest := input; len(rest) > 0; {
		var tag string
		_, tag, rest = popTag(rest)
		if len(tag) == 0 {
			continue
		}

		name, ok := t.find(tag, t.tag_names)
		if !ok {
			return
		}

		ttm := t.tag_to_metrics[name]

		if metrics.IsEmpty() {
			metrics.Or(ttm)
		} else {
			metrics.And(ttm)
		}

		if metrics.IsEmpty() {
			return
		}
	}

	// the only way it's here and still empty is if the input query was empty
	if metrics.IsEmpty() {
		metrics = t.metrics
	}

	nbuf := make([]uint32, 0, 8)

	metrics.Iterate(func(name uint32) bool {
		nbuf = t.metric_names.Get(name, nbuf[:0])

		buf = buf[:0]
		for i, part := range nbuf {
			if i != 0 {
				buf = append(buf, ',')
			}
			buf = append(buf, t.tag_names.Get(part)...)
		}

		return cb(buf)
	})
}

func (t *T) TagKeys(input string, cb func(result string) bool) {
	tkeys := t.acquireBitmap()
	defer t.replaceBitmap(tkeys)

	metrics := t.acquireBitmap()
	defer t.replaceBitmap(metrics)

	for rest := input; len(rest) > 0; {
		var tag string
		_, tag, rest = popTag(rest)
		if len(tag) == 0 {
			continue
		}

		name, ok := t.find(tag, t.tag_names)
		if !ok {
			return
		}

		ttk := t.tag_to_tkeys[name]
		ttm := t.tag_to_metrics[name]

		if metrics.IsEmpty() {
			tkeys.Or(ttk)
			metrics.Or(ttm)
		} else {
			tkeys.And(ttk)
			metrics.And(ttm)
		}

		if tkeys.IsEmpty() || metrics.IsEmpty() {
			return
		}
	}

	// the only way it's here and still empty is if the input query was empty
	if metrics.IsEmpty() {
		tkeys = t.tkeys
		metrics = nil
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
		var tag string
		_, tag, rest = popTag(rest)
		if len(tag) == 0 {
			continue
		}

		name, ok := t.find(tag, t.tag_names)
		if !ok {
			return
		}

		ttt := t.tag_to_tags[name]
		ttm := t.tag_to_metrics[name]

		if metrics.IsEmpty() {
			tags.Or(ttt)
			metrics.Or(ttm)
		} else {
			tags.And(ttt)
			metrics.And(ttm)
		}

		if tags.IsEmpty() || metrics.IsEmpty() {
			return
		}
	}

	// the only way it's here and still empty is if the input query was empty
	if metrics.IsEmpty() {
		tags = t.tkey_to_tags[name]
		metrics = nil
	} else {
		tags.And(t.tkey_to_tags[name])
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

func popTag(tags string) (tkey, tag string, rest string) {
	// find the first unescaped ','
	for j := uint(0); j < uint(len(tags)); {
		i := strings.IndexByte(tags[j:], ',')
		if i < 0 {
			break
		}
		ui := uint(i)

		if ui > 0 && ui-1 < uint(len(tags)) && tags[ui-1] == '\\' {
			j = ui + 1
			continue
		}

		idx := ui + j
		tags, rest = tags[:idx], tags[idx+1:]
		break
	}

	// if there's no =, then the tag key is the tag
	tkey = tags

	// find the first unescaped '='
	for j := uint(0); j < uint(len(tkey)); {
		i := strings.IndexByte(tkey[j:], '=')
		if i < 0 {
			break
		}
		ui := uint(i)

		if ui > 0 && ui-1 < uint(len(tkey)) && tkey[ui-1] == '\\' {
			j = ui + 1
			continue
		}

		tkey = tkey[:ui+j]
		break
	}

	// if the tag has an empty string value, then drop the trailing =
	if len(tags) == len(tkey)+1 && tags[len(tags)-1] == '=' {
		tags = tags[:len(tags)-1]
	}

	return tkey, tags, rest
}
