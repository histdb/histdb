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

type T[K hashtbl.Key] struct {
	_ [0]func() // no equality

	buf   []byte
	idxs  hashtbl.T[K, hashtbl.U64]
	spans []span
}

func (t *T[K]) Buf() []byte { return t.buf }

func (t *T[K]) Len() int {
	if t == nil {
		return 0
	}
	return t.idxs.Len()
}
func (t *T[K]) Size() uint64 {
	return 0 +
		/* buf   */ sizeof.Slice(t.buf) +
		/* idxs  */ t.idxs.Size() +
		/* spans */ sizeof.Slice(t.spans) +
		0
}

func (t *T[K]) Put(h K, v []byte) uint64 {
	n, ok := t.idxs.Insert(h, hashtbl.U64(len(t.spans)))
	if !ok && len(v) > 0 {
		t.spans = append(t.spans, span{
			begin: uint32(len(t.buf)),
			end:   uint32(len(t.buf) + len(v)),
		})
		t.buf = append(t.buf, v...)
	}
	return uint64(n)
}

func (t *T[K]) Find(h K) (uint64, bool) {
	v, ok := t.idxs.Find(h)
	return uint64(v), ok
}

func (t *T[K]) Get(n uint64) []byte {
	if uint64(n) < uint64(len(t.spans)) {
		s := t.spans[n]
		b, e := uint64(s.begin), uint64(s.end)
		if b < uint64(len(t.buf)) && e <= uint64(len(t.buf)) && b <= e {
			return t.buf[b:e]
		}
	}
	return nil
}
