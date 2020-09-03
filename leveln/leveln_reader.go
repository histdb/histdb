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
	span   []byte
	value  []byte
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
	} else if cap(it.span) < vwSpanSize {
		it.span = make([]byte, vwSpanSize)
	} else {
		it.span = it.span[:vwSpanSize]
	}

	n, err := it.values.ReadAt(it.span, int64(it.offset)*vwSpanAlign)
	if n >= 16 {
		it.span = it.span[:n]
	} else if err != nil {
		it.err = err
		return
	} else {
		it.err = errs.New("iterator short read")
	}

	copy(it.skey[0:16], it.span[:16])
	it.span = it.span[16:]
	it.offset++
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

	origKey := key
	binary.BigEndian.PutUint32(key[16:20], 0)
	key, offset, ok, err := it.kr.Seek(key)
	if err != nil {
		it.err = err
		return false
	} else if !ok {
		it.err = io.EOF
		return false
	} else {
		it.skey = key
		it.offset = offset
		it.readNextSpan()
		for it.Next() && lsm.KeyCmp.Less(it.Key(), origKey) {
		}
	}
	return it.err == nil
}
