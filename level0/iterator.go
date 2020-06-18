package level0

import (
	"encoding/binary"

	"github.com/zeebo/lsm"
	"github.com/zeebo/lsm/filesystem"
)

type Iterator struct {
	fh   filesystem.File
	idxb []byte
	idx  []byte
	buf  []byte
	err  error

	// perf counters
	perf struct {
		read int
	}
}

func (it *Iterator) Init(fh filesystem.File) {
	*it = Iterator{
		fh: fh,
	}
}

func (it *Iterator) readIndex() {
	var size int64
	if size, it.err = it.fh.Size(); it.err != nil {
		return
	}
	size -= 4

	var tmp [4]byte
	it.perf.read++
	if _, it.err = it.fh.ReadAt(tmp[:], size); it.err != nil {
		return
	}

	pos := int64(binary.BigEndian.Uint32(tmp[:]))
	it.idxb = make([]byte, size-pos)
	it.perf.read++
	if _, it.err = it.fh.ReadAt(it.idxb, pos); it.err != nil {
		return
	}
	it.idx = it.idxb
}

func (it *Iterator) Next() bool {
	if it.err != nil {
		return false
	} else if it.idx == nil {
		it.readIndex()
	}
	if len(it.idx) < 8 {
		return false
	}

	offset := int64(binary.BigEndian.Uint32(it.idx[0:4]))
	length := uint(binary.BigEndian.Uint32(it.idx[4:8]))
	it.idx = it.idx[8:]

	it.readEntry(offset, length)
	return it.err == nil
}

func (it *Iterator) readEntry(offset int64, length uint) {
	if uint(cap(it.buf)) < length {
		it.buf = make([]byte, length)
	} else {
		it.buf = it.buf[:length]
	}
	it.perf.read++
	_, it.err = it.fh.ReadAt(it.buf, offset)
}

func (it *Iterator) Key() (k lsm.Key) {
	copy(k[:], it.buf[:len(k)])
	return k
}

func (it *Iterator) Value() []byte {
	return it.buf[4+len(lsm.Key{}) : len(it.buf) : len(it.buf)]
}

func (it *Iterator) Err() error {
	return it.err
}

func (it *Iterator) Seek(key lsm.Key) {
	if it.err != nil {
		return
	} else if it.idx == nil {
		it.readIndex()
	}
	if len(it.idxb) < 8 {
		return
	}

	i, j := 0, len(it.idxb)/8
	for i < j {
		h := int(uint(i+j) / 2)

		offset := int64(binary.BigEndian.Uint32(it.idxb[8*h:]))
		length := uint(binary.BigEndian.Uint32(it.idxb[8*h+4:]))
		it.readEntry(offset, length)
		if it.err != nil {
			return
		}

		if lsm.KeyCmp.Less(it.Key(), key) {
			i = h + 1
		} else {
			j = h
		}
	}

	it.idx = it.idxb[8*i:]
}
