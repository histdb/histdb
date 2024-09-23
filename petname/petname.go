package petname

import (
	"github.com/histdb/histdb/hashtbl"
	"github.com/histdb/histdb/num"
	"github.com/histdb/histdb/sizeof"
)

type span struct {
	_ [0]func() // no equality

	begin uint32
	end   uint32
}

type B[V num.T] struct {
	_ [0]func() // no equality

	buf   []byte
	spans []span
}

func (b *B[V]) Size() uint64 {
	return 0 +
		/* buf   */ sizeof.Slice(b.buf) +
		/* spans */ sizeof.Slice(b.spans) +
		0
}

func (b *B[V]) Len() int { return len(b.spans) }

func (b *B[V]) Append(v []byte) {
	b.spans = append(b.spans, span{
		begin: uint32(len(b.buf)),
		end:   uint32(len(b.buf) + len(v)),
	})
	b.buf = append(b.buf, v...)
}

func (b *B[V]) Get(n V) []byte {
	if spans := b.spans; uint64(n) < uint64(len(spans)) {
		s := spans[n]
		be, ed := uint64(s.begin), uint64(s.end)
		if buf := b.buf; be < uint64(len(buf)) && ed <= uint64(len(buf)) && be <= ed {
			return buf[be:ed]
		}
	}
	return nil
}

type T[K comparable, V num.T] struct {
	_ [0]func() // no equality

	idxs hashtbl.T[K, V]
	buf  B[V]
}

func (t *T[K, V]) Buf() []byte { return t.buf.buf }

func (t *T[K, V]) Len() int {
	if t == nil {
		return 0
	}
	return t.idxs.Len()
}

func (t *T[K, V]) Size() uint64 {
	return 0 +
		/* idxs */ t.idxs.Size() +
		/* buf  */ t.buf.Size() +
		0
}

func (t *T[K, V]) Put(h K, v []byte) V {
	n, ok := t.idxs.Insert(h, V(t.buf.Len()))
	if ok {
		t.buf.Append(v)
	}
	return n
}

func (t *T[K, V]) Find(h K) (V, bool) { return t.idxs.Find(h) }

func (t *T[K, V]) Get(n V) []byte { return t.buf.Get(n) }
