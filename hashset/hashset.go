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

type T[K Key] struct {
	_ [0]func() // no equality

	set  hashtbl.T[K, hashtbl.U32]
	list []K
}

func (t *T[K]) Len() int { return len(t.list) }

func (t *T[K]) Size() uint64 {
	return 0 +
		/* set  */ t.set.Size() +
		/* list */ sizeof.Slice(t.list) +
		0
}

func (t *T[K]) Fix() {
	t.set = hashtbl.T[K, hashtbl.U32]{}
}

func (t *T[K]) Insert(k K) (uint32, bool) {
	idx, ok := t.set.Insert(k, hashtbl.U32(len(t.list)))
	if ok {
		return uint32(idx), ok
	}
	t.list = append(t.list, k)
	return uint32(idx), false
}

func (t *T[K]) Hash(idx uint32) K {
	return t.list[idx]
}
