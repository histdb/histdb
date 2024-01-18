package petname

import (
	"github.com/histdb/histdb/hashtbl"
	"github.com/histdb/histdb/sizeof"
)

type span struct {
	_ [0]func() // no equality

	begin uint32
	end   uint32
}

type Numeric interface{ ~uint32 | ~uint64 }

type T[K hashtbl.Key, V Numeric] struct {
	_ [0]func() // no equality

	buf   []byte
	idxs  hashtbl.T[K, V]
	spans []span
}

func (t *T[K, V]) Buf() []byte { return t.buf }

func (t *T[K, V]) Len() int {
	if t == nil {
		return 0
	}
	return t.idxs.Len()
}
func (t *T[K, V]) Size() uint64 {
	return 0 +
		/* buf   */ sizeof.Slice(t.buf) +
		/* idxs  */ t.idxs.Size() +
		/* spans */ sizeof.Slice(t.spans) +
		0
}

func (t *T[K, V]) Put(h K, v []byte) V {
	n, ok := t.idxs.Insert(h, V(len(t.spans)))
	if !ok && len(v) > 0 {
		t.spans = append(t.spans, span{
			begin: uint32(len(t.buf)),
			end:   uint32(len(t.buf) + len(v)),
		})
		t.buf = append(t.buf, v...)
	}
	return n
}

func (t *T[K, V]) Find(h K) (V, bool) { return t.idxs.Find(h) }

func (t *T[K, V]) Get(n V) []byte {
	if uint64(n) < uint64(len(t.spans)) {
		s := t.spans[n]
		b, e := uint64(s.begin), uint64(s.end)
		if b < uint64(len(t.buf)) && e <= uint64(len(t.buf)) && b <= e {
			return t.buf[b:e]
		}
	}
	return nil
}
