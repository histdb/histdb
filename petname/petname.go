package petname

import (
	"unsafe"

	"github.com/histdb/histdb/buffer"
	"github.com/histdb/histdb/varint"
)

type Hash = struct {
	Hi uint64
	Lo uint64
}

const hashSize = 16

//
// []byte
//

type Strings struct {
	buf []byte
	// idxs  map[Hash]uint32
	idxs  *table
	spans [][2]uint32
}

func NewStrings() *Strings {
	return &Strings{
		// idxs: make(map[Hash]uint32),
		idxs: newTable(),
	}
}

func (t *Strings) Size() uint64 {
	return 0 +
		1*uint64(len(t.buf)) +
		// (hashSize+4)*uint64(len(t.idxs)) +
		t.idxs.size() +
		8*uint64(len(t.spans)) +
		0
}

func (t *Strings) Put(h Hash, v string) uint32 {
	// n, ok := t.idxs[h]
	n, ok := t.idxs.insert(h, uint32(len(t.spans)))
	if !ok {
		n = uint32(len(t.spans))
		t.spans = append(t.spans, [2]uint32{
			uint32(len(t.buf)),
			uint32(len(t.buf)) + uint32(len(v)),
		})
		t.buf = append(t.buf, v...)
		// t.idxs[h] = n
	}
	return n
}

func (t *Strings) Find(h Hash) (uint32, bool) {
	// n, ok := t.idxs[h]
	// return n, ok
	return t.idxs.find(h)
}

func (t *Strings) Get(n uint32) string {
	s := t.spans[n]
	b, e := uint64(s[0]), uint64(s[1])
	if b < uint64(len(t.buf)) && e <= uint64(len(t.buf)) && b <= e {
		v := t.buf[b:e]
		return *(*string)(unsafe.Pointer(&v))
	}
	return ""
}

//
// []uint32
//

type Uint32s struct {
	buf buffer.T
	// idxs  map[Hash]uint32
	idxs  *table
	spans []span
}

type span struct {
	start uint32
	len   uint16
}

func NewUint32s() *Uint32s {
	return &Uint32s{
		buf: buffer.Of(make([]byte, 16)),
		// idxs: make(map[Hash]uint32),
		idxs: newTable(),
	}
}

func (t *Uint32s) Fix() {
	t.buf = t.buf.Grow().Reset()
	t.idxs = nil
}

func (t *Uint32s) Size() uint64 {
	return 0 +
		1*uint64(t.buf.Cap()) +
		// (hashSize+4)*uint64(len(t.idxs)) +
		t.idxs.size() +
		6*uint64(len(t.spans)) +
		0
}

func (t *Uint32s) Put(h Hash, vs []uint32) (uint32, bool) {
	// n, ok := t.idxs[h]
	n, ok := t.idxs.insert(h, uint32(len(t.spans)))
	if !ok {
		start := uint32(t.buf.Pos())
		t.buf = t.buf.GrowN(9 * uintptr(len(vs)))

		for _, v := range vs {
			t.buf = t.buf.Advance(varint.Append(t.buf.Front9(), uint64(v)))
		}

		nlen := uint16(uint32(t.buf.Pos()) - start)
		t.spans = append(t.spans, span{start: start, len: nlen})
		// t.idxs[h] = n
		return uint32(len(t.spans) - 1), false
	}
	return n, ok
}

func (t *Uint32s) Find(h Hash) (uint32, bool) {
	// n, ok := t.idxs[h]
	// return n, ok
	return t.idxs.find(h)
}

func (t *Uint32s) Get(n uint32, buf []uint32) []uint32 {
	var nb uintptr
	var dec uint64

	s := t.spans[n]
	d := t.buf.Reset().Advance(uintptr(s.start))

	for s.len > 0 && d.Remaining() >= 9 {
		nb, dec = varint.FastConsume(d.Front9())
		buf = append(buf, uint32(dec))
		d = d.Advance(nb)
		s.len -= uint16(nb)
	}

	for s.len > 0 && d.Remaining() > 0 {
		dec, d, _ = varint.SafeConsume(d)
		buf = append(buf, uint32(dec))
		s.len -= uint16(nb)
	}

	return buf
}
