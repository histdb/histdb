package memindex

import (
	"fmt"
	"sync"

	"github.com/RoaringBitmap/roaring"

	"github.com/histdb/histdb/sizeof"
)

const orParallelism = 0

type Bitmap = roaring.Bitmap

var queryPool = sync.Pool{New: func() interface{} { return roaring.New() }}

func replaceBitmap(m *Bitmap) {
	queryPool.Put(m)
}

func acquireBitmap() *Bitmap {
	bm := queryPool.Get().(*Bitmap)
	if !bm.IsEmpty() {
		bm.Clear()
	}
	return bm
}

func getBitmap(bmsp *[]*Bitmap, n uint32) (bm *Bitmap) {
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

func sliceSize(m []*Bitmap) (n uint64) {
	for _, bm := range m {
		n += bm.GetSizeInBytes()
	}
	return sizeof.Slice(m) + n
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
