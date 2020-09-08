package leveln

import (
	"encoding/binary"
	"io"

	"github.com/zeebo/errs"
	"github.com/zeebo/lsm"
	"github.com/zeebo/lsm/filesystem"
)

type Reader struct {
	keys   filesystem.File
	values filesystem.File
}

func (r *Reader) Init(keys, values filesystem.File) {
	*r = Reader{
		keys:   keys,
		values: values,
	}
}

func (r *Reader) Iterator() (it Iterator) {
	it.Init(r.keys, r.values)
	return it
}

type Iterator struct {
	err    error
	kr     keyReader
	values filesystem.File
	offset uint32
	skey   lsm.Key
	size   int
	sbuf   []byte
	span   []byte
	value  []byte

	stats struct {
		valueReads int64
	}
}

func (it *Iterator) Init(keys, values filesystem.File) {
	*it = Iterator{
		values: values,
	}
	it.kr.Init(keys)
}

func (it *Iterator) Done() bool {
	return it.err != nil
}

func (it *Iterator) Next() bool {
	if it.err != nil {
		return false
	} else if len(it.span) < 6 || binary.BigEndian.Uint16(it.span[0:2]) < 6 {
		it.readNextSpan()
		if it.err != nil {
			return false
		}
	}

	vend := binary.BigEndian.Uint16(it.span[0:2])
	copy(it.skey[16:20], it.span[2:6])
	it.value = it.span[6:vend]
	it.span = it.span[vend:]

	return true
}

func (it *Iterator) readNextSpan() {
	if it.err != nil {
		return
	}

	// lazily allocate the span backing byte slice
	if it.sbuf == nil {
		it.sbuf = make([]byte, vwSpanSize)
	}

	// increment offset by the number of alignment blocks necessary
	it.offset += uint32(((it.size - len(it.span)) + vwSpanAlign - 1) / vwSpanAlign)

	// read a span into the buffer
	it.stats.valueReads++
	n, err := it.values.ReadAt(it.sbuf, int64(it.offset)*vwSpanAlign)
	if n >= 16 {
		it.size = n
		it.span = it.sbuf[:n:n]
	} else if err != nil {
		it.err = err
		return
	} else {
		it.err = errs.New("iterator short read")
		return
	}

	copy(it.skey[0:16], it.span[:16])
	it.span = it.span[16:]
}

func (it *Iterator) Key() lsm.Key {
	return it.skey
}

func (it *Iterator) Value() []byte {
	return it.value
}

func (it *Iterator) Err() error {
	if it.err != io.EOF {
		return it.err
	}
	return nil
}

func (it *Iterator) Seek(key lsm.Key) bool {
	if it.err == io.EOF {
		it.err = nil
	} else if it.err != nil {
		return false
	}

	offset, _, _, err := it.kr.Search(key)
	if err != nil {
		it.err = err
		return false
	}

	it.size = 0
	it.span = nil
	it.offset = offset
	it.readNextSpan()

	// no fancy comparisons because the prefix likely matches.
	for it.Next() && lsm.KeyCmp.Less(it.Key(), key) {
	}

	return it.err == nil
}
