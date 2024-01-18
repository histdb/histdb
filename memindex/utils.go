package memindex

import (
	"fmt"
	"sync"

	"github.com/RoaringBitmap/roaring/roaring64"

	"github.com/histdb/histdb/hashtbl"
	"github.com/histdb/histdb/sizeof"
)

const orParallelism = 0

//
// bitmap parameters
//

type (
	Bitmap = roaring64.Bitmap
	Id     = uint64
	rwId   = hashtbl.U64
)

func newBitmap() *Bitmap           { return roaring64.New() }
func parOr(bms ...*Bitmap) *Bitmap { return roaring64.ParOr(0, bms...) }

//
//
//

var queryPool = sync.Pool{New: func() interface{} { return newBitmap() }}

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

func getBitmap(bmsp *[]*Bitmap, n Id) (bm *Bitmap) {
	if bms := *bmsp; n < Id(len(bms)) {
		bm = bms[n]
	} else if n == Id(len(bms)) {
		bm = newBitmap()
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

func iter(bm *Bitmap, cb func(Id) bool) {
	var buf [64]Id
	it := bm.ManyIterator()
	for {
		n := it.NextMany(buf[:])
		if n == 0 {
			return
		}
		for _, u := range buf[:n] {
			if !cb(u) {
				return
			}
		}
	}
}
