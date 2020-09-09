package level0

import (
	"encoding/binary"

	"github.com/zeebo/lsm"
	"github.com/zeebo/lsm/filesystem"
)

type Iterator struct {
	nvbuf []byte
	nbuf  []byte // reference into nvbuf
	vbuf  []byte // reference into nvbuf
	idx   []byte // reference into idxb
	err   error

	fh   filesystem.File
	hbuf [l0EntryHeaderSize]byte
	idxb [l0IndexSize]byte
}

func (it *Iterator) Init(fh filesystem.File) {
	it.fh = fh
	it.err = nil
	it.idx = nil
}

func (it *Iterator) readIndex() {
	var n int
	if n, it.err = it.fh.ReadAt(it.idxb[:], l0DataSize); it.err != nil {
		return
	}
	it.idx = it.idxb[:n]
}

func (it *Iterator) Next() bool {
	if it.err != nil {
		return false
	} else if it.idx == nil {
		it.readIndex()
	}
	if len(it.idx) < 4 {
		return false
	}

	offset := int64(binary.BigEndian.Uint16(it.idx[2:4])) * 32
	if offset == 0 {
		return false
	}

	it.idx = it.idx[4:]

	it.readEntryHeader(offset)
	it.readNameAndValue(offset)

	return it.err == nil
}

func (it *Iterator) readEntryHeader(offset int64) {
	if _, it.err = it.fh.ReadAt(it.hbuf[:], offset); it.err != nil {
		return
	}
}

func (it *Iterator) readNameAndValue(offset int64) {
	nlen := int64(binary.BigEndian.Uint32(it.hbuf[4:8]))
	vlen := int64(binary.BigEndian.Uint32(it.hbuf[8:12]))
	nvlen := nlen + vlen

	if int64(cap(it.nvbuf)) < nvlen {
		it.nvbuf = make([]byte, nvlen)
	}

	it.nbuf = it.nvbuf[:nlen]
	it.vbuf = it.nvbuf[nlen:]

	if _, it.err = it.fh.ReadAt(it.nvbuf[:nvlen], offset+l0EntryHeaderSize); it.err != nil {
		return
	}
}

func (it *Iterator) Key() (k lsm.Key) {
	copy(k[:], it.hbuf[12:12+len(k)])
	return k
}

func (it *Iterator) Name() []byte {
	return it.nbuf
}

func (it *Iterator) Value() []byte {
	return it.vbuf
}

func (it *Iterator) Err() error {
	return it.err
}

func (it *Iterator) Seek(key lsm.Key) bool {
	if it.err != nil {
		return false
	} else if it.idx == nil {
		it.readIndex()
	}
	if len(it.idxb) < 4 {
		return false
	}

	kp := binary.BigEndian.Uint16(key[0:2])
	i, j := 0, len(it.idxb)/4
	for i < j {
		h := int(uint(i+j) / 2)

		// check if we have a value there at all
		offset := int64(binary.BigEndian.Uint16(it.idxb[4*h+2:])) * 32
		if offset == 0 {
			j = h
			continue
		}

		// check if the prefix is definitely larger/smaller
		kph := binary.BigEndian.Uint16(it.idxb[4*h:])
		if kph < kp {
			i = h + 1
			continue
		}
		if kph > kp {
			j = h
			continue
		}

		// have to read the whole entry
		it.readEntryHeader(offset)
		if it.err != nil {
			return false
		}

		if lsm.KeyCmp.Less(it.Key(), key) {
			i = h + 1
		} else {
			j = h
		}
	}

	it.idx = it.idxb[4*i:]
	return it.Next()
}
