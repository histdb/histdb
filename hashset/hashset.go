package hashset

import (
	"github.com/histdb/histdb/hashtbl"
	"github.com/histdb/histdb/rwutils"
	"github.com/histdb/histdb/sizeof"
)

type Key interface {
	rwutils.Bytes
	hashtbl.Key
}

type Numeric interface{ ~uint32 | ~uint64 }

type T[K Key, V Numeric] struct {
	_ [0]func() // no equality

	set  hashtbl.T[K, V]
	list []K
}

func (t *T[K, V]) Len() int { return len(t.list) }

func (t *T[K, V]) Size() uint64 {
	return 0 +
		/* set  */ t.set.Size() +
		/* list */ sizeof.Slice(t.list) +
		0
}

func (t *T[K, V]) Fix() { t.set = hashtbl.T[K, V]{} }

func (t *T[K, V]) Insert(k K) (V, bool) {
	idx, ok := t.set.Insert(k, V(len(t.list)))
	if ok {
		return idx, ok
	}
	t.list = append(t.list, k)
	return idx, false
}

func (t *T[K, V]) Hash(idx V) K { return t.list[idx] }
