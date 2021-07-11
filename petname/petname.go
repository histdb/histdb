package petname

import "unsafe"

type Hash = struct {
	Hi uint64
	Lo uint64
}

type span struct {
	begin uint32
	end   uint32
}

type T struct {
	buf   []byte
	idxs  *table
	spans []span
}

func New() *T {
	return &T{
		idxs: newTable(),
	}
}

func (t *T) Len() int {
	if t == nil {
		return 0
	}
	return t.idxs.Len()
}

func (t *T) Size() uint64 {
	if t == nil {
		return 0
	}
	return 0 +
		1*uint64(len(t.buf)) +
		t.idxs.Size() +
		8*uint64(len(t.spans)) +
		0
}

func (t *T) Put(h Hash, v string) (uint32, bool) {
	n, ok := t.idxs.Insert(h, uint32(t.idxs.Len()))
	if !ok && len(v) > 0 {
		t.spans = append(t.spans, span{
			begin: uint32(len(t.buf)),
			end:   uint32(len(t.buf) + len(v)),
		})
		t.buf = append(t.buf, v...)
	}
	return n, ok
}

func (t *T) Find(h Hash) (uint32, bool) {
	return t.idxs.Find(h)
}

func (t *T) Get(n uint32) string {
	s := t.spans[n]
	b, e := uint64(s.begin), uint64(s.end)
	if b < uint64(len(t.buf)) && e <= uint64(len(t.buf)) && b <= e {
		v := t.buf[b:e]
		return *(*string)(unsafe.Pointer(&v))
	}
	return ""
}
