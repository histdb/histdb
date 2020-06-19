package level0

import (
	"encoding/binary"

	"github.com/zeebo/errs"
	"github.com/zeebo/lsm"
	"github.com/zeebo/lsm/filesystem"
)

type Iterator struct {
	fh   filesystem.File
	hbuf [24]byte
	vbuf []byte
	idxb []byte

	idx []byte
	err error
}

func (it *Iterator) Init(fh filesystem.File) {
	*it = Iterator{
		fh:   fh,
		idxb: it.idxb,
		vbuf: it.vbuf,
	}
}

func (it *Iterator) readIndex() {
	if it.idxb == nil {
		it.idxb = make([]byte, l0IndexSize)
	}

	if _, it.err = it.fh.ReadAt(it.idxb, l0DataSize); it.err != nil {
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
	if len(it.idx) < 2 {
		return false
	}

	offset := int64(binary.BigEndian.Uint16(it.idx[0:2])) * 32
	if offset == 0 {
		return false
	}

	it.idx = it.idx[2:]

	it.readEntryHeader(offset)
	it.readValue(offset)

	return it.err == nil
}

func (it *Iterator) readEntryHeader(offset int64) {
	if _, it.err = it.fh.ReadAt(it.hbuf[:], offset); it.err != nil {
		return
	}
}

func (it *Iterator) readValue(offset int64) {
	length := int64(binary.BigEndian.Uint32(it.hbuf[0:4])) - l0EntryHeaderSize
	if length < 0 {
		it.err = errs.New("invalid read: length too short")
		return
	}

	if int64(len(it.vbuf)) < length {
		it.vbuf = make([]byte, length)
	} else {
		it.vbuf = it.vbuf[:length]
	}

	if _, it.err = it.fh.ReadAt(it.vbuf, offset+l0EntryHeaderSize); it.err != nil {
		return
	}
}

func (it *Iterator) Key() (k lsm.Key) {
	copy(k[:], it.hbuf[4:20])
	return k
}

func (it *Iterator) Timestamp() uint32 {
	return binary.BigEndian.Uint32(it.hbuf[20:24])
}

func (it *Iterator) Value() []byte {
	return it.vbuf
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
	if len(it.idxb) < 2 {
		return
	}

	i, j := 0, len(it.idxb)/2
	for i < j {
		h := int(uint(i+j) / 2)

		offset := int64(binary.BigEndian.Uint16(it.idxb[2*h:])) * 32
		if offset == 0 {
			j = h
			continue
		}

		it.readEntryHeader(offset)
		if it.err != nil {
			return
		}

		if lsm.KeyCmp.Less(it.Key(), key) {
			i = h + 1
		} else {
			j = h
		}
	}

	it.idx = it.idxb[2*i:]
}
