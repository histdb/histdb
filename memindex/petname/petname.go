package petname

import (
	"encoding/binary"
	"math/bits"
	"unsafe"
)

type Hash = struct {
	H uint64
	L uint64
}

const hashSize = 16

//
// []byte
//

type Strings struct {
	buf   []byte
	idxs  map[Hash]uint32
	spans [][2]uint32
}

func NewStrings() *Strings {
	return &Strings{
		idxs: make(map[Hash]uint32),
	}
}

func (t *Strings) Size() uint64 {
	return 0 +
		1*uint64(len(t.buf)) +
		(hashSize+4)*uint64(len(t.idxs)) +
		8*uint64(len(t.spans)) +
		0
}

func (t *Strings) Put(h Hash, v string) uint32 {
	n, ok := t.idxs[h]
	if !ok {
		n = uint32(len(t.spans))
		t.spans = append(t.spans, [2]uint32{
			uint32(len(t.buf)),
			uint32(len(t.buf)) + uint32(len(v)),
		})
		t.buf = append(t.buf, v...)
		t.idxs[h] = n
	}
	return n
}

func (t *Strings) Find(h Hash) (uint32, bool) {
	n, ok := t.idxs[h]
	return n, ok
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
	buf   []byte
	idxs  map[Hash]uint32
	spans []span
}

type span struct {
	b uint32
	e uint16
}

func NewUint32s() *Uint32s {
	return &Uint32s{
		idxs: make(map[Hash]uint32),
	}
}

func (t *Uint32s) Fix() {
	t.buf = append(t.buf, 0, 0, 0, 0, 0, 0, 0, 0)
	t.idxs = nil
}

func (t *Uint32s) Size() uint64 {
	return 0 +
		1*uint64(len(t.buf)) +
		(hashSize+4)*uint64(len(t.idxs)) +
		6*uint64(len(t.spans)) +
		0
}

func (t *Uint32s) Put(h Hash, vs []uint32) (uint32, bool) {
	n, ok := t.idxs[h]
	if !ok {
		le := binary.LittleEndian
		n = uint32(len(t.spans))
		s := uint32(len(t.buf))

		for _, v := range vs {
			if cap(t.buf) < len(t.buf)+8 {
				nbuf := make([]byte, 8+len(t.buf)*2)
				t.buf = nbuf[:copy(nbuf, t.buf)]
			}

			nbytes, enc := varintStats(v)
			le.PutUint64(t.buf[len(t.buf):len(t.buf)+8], enc)
			t.buf = t.buf[:len(t.buf)+int(nbytes)]
		}

		t.spans = append(t.spans, span{b: s, e: uint16(uint32(len(t.buf)) - s)})
		t.idxs[h] = n
	}
	return n, ok
}

func (t *Uint32s) Find(h Hash) (uint32, bool) {
	n, ok := t.idxs[h]
	return n, ok
}

func (t *Uint32s) Get(n uint32, buf []uint32) []uint32 {
	le := binary.LittleEndian
	s := t.spans[n]
	b := uint64(s.b)
	e := uint64(s.b) + uint64(s.e)
	if b < uint64(len(t.buf)) && e <= uint64(len(t.buf)) {
		for b < e {
			nbytes, dec := fastVarintConsume(le.Uint64(t.buf[b : b+8]))
			buf = append(buf, dec)
			b += uint64(nbytes)
		}
	}
	return buf
}

func fastVarintConsume(val uint64) (nbytes uint8, dec uint32) {
	nbytes = uint8(bits.TrailingZeros8(^uint8(val)) + 1)
	val <<= (64 - 8*nbytes) % 64
	val >>= (64 - 7*nbytes) % 64
	return nbytes, uint32(val)
}

func varintStats(val uint32) (nbytes uint8, enc uint64) {
	if val == 0 {
		return 1, 0
	}
	nbytes = (uint8(bits.Len32(val)) - 1) / 7
	return nbytes + 1, (2*uint64(val)+1)<<(nbytes%64) - 1
}
