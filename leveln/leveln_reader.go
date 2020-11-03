package leveln

import (
	"encoding/binary"
	"errors"
	"io"

	"github.com/zeebo/errs/v2"
	"github.com/zeebo/lsm"
	"github.com/zeebo/lsm/filesystem"
)

type Reader struct {
	keys   filesystem.Handle
	values filesystem.Handle
}

func (r *Reader) Init(keys, values filesystem.Handle) {
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
	values filesystem.Handle
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

func (it *Iterator) Init(keys, values filesystem.Handle) {
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
	} else if len(it.span) < vwEntryHeaderSize || binary.BigEndian.Uint16(it.span[0:2]) < vwEntryHeaderSize {
		it.readNextSpan()
		if it.err != nil {
			return false
		}
	}

	vend := binary.BigEndian.Uint16(it.span[0:2])
	it.skey.SetTimestamp(binary.BigEndian.Uint32(it.span[2:6]))
	it.value = it.span[vwEntryHeaderSize:vend]
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
	it.offset += uint32(((it.size - len(it.span)) + vwSpanMask) / vwSpanAlign)

	// read a span into the buffer
	it.stats.valueReads++
	n, err := it.values.ReadAt(it.sbuf, int64(it.offset)*vwSpanAlign)
	if n >= lsm.HashSize {
		it.size = n
		it.span = it.sbuf[:n:n]
	} else if err != nil {
		it.err = err
		return
	} else {
		it.err = errs.Errorf("iterator short read")
		return
	}

	copy(it.skey.HashPtr()[:], it.span[:lsm.HashSize])
	it.span = it.span[lsm.HashSize:]
}

func (it *Iterator) Key() lsm.Key {
	return it.skey
}

func (it *Iterator) Value() []byte {
	return it.value
}

func (it *Iterator) Err() error {
	if !errors.Is(it.err, io.EOF) {
		return it.err
	}
	return nil
}

func (it *Iterator) Seek(key lsm.Key) bool {
	if errors.Is(it.err, io.EOF) {
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
