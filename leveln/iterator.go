package leveln

import (
	"errors"
	"io"

	"github.com/zeebo/errs/v2"

	"github.com/histdb/histdb"
	"github.com/histdb/histdb/filesystem"
	"github.com/histdb/histdb/memindex"
)

type Iterator struct {
	_ [0]func() // no equality

	stats struct {
		valueReads int64
	}

	err    error
	kr     keyReader
	values filesystem.Handle
	idx    *memindex.T
	offset uint32
	skey   histdb.Key
	name   []byte
	size   int
	span   []byte
	value  []byte

	sbuf [vwSpanSize]byte
}

func (it *Iterator) Init(keys, values filesystem.Handle, idx *memindex.T) {
	*it = Iterator{
		idx:    idx,
		values: values,
	}
	it.kr.Init(keys)
}

func (it *Iterator) Next() bool {
	if it.err != nil {
		return false
	} else if len(it.span) < vwEntryHeaderSize || be.Uint16(it.span[0:2]) < vwEntryHeaderSize {
		it.readNextSpan()
		if it.err != nil {
			return false
		}
	}

	var vend uint
	span := it.span

	if len(span) < vwEntryHeaderSize {
		goto corrupt
	}

	vend = uint(be.Uint16(span[0:2]))
	it.skey.SetTimestamp(be.Uint32(span[2:6]))
	it.skey.SetDuration(be.Uint32(span[6:10]))

	if vwEntryHeaderSize <= vend && vend < uint(len(span)) {
		it.value = span[vwEntryHeaderSize:vend]
		it.span = span[vend:]

		return true
	}

corrupt:
	it.err = errs.Errorf("iterator data corruption: span too short")
	return false
}

func (it *Iterator) readNextSpan() {
	if it.err != nil {
		return
	}

	// increment offset by the number of alignment blocks necessary
	it.offset += uint32(((it.size - len(it.span)) + vwSpanMask) / vwSpanAlign)

	// read a span into the buffer
	it.stats.valueReads++
	n, err := it.values.ReadAt(it.sbuf[:], int64(it.offset)*vwSpanAlign)
	if n > 0 && n <= cap(it.sbuf) {
		it.size = n
		it.span = it.sbuf[:n:n]
	} else if err != nil {
		it.err = err
		return
	} else {
		it.err = errs.Errorf("iterator short read")
		return
	}

	if len(it.span) < histdb.HashSize {
		it.err = errs.Errorf("iterator data corruption: span too short")
		return
	}

	*it.skey.HashPtr() = (histdb.Hash)(it.span[0:histdb.HashSize])
	it.span = it.span[histdb.HashSize:]

	var ok bool
	it.name, ok = it.idx.AppendNameByHash(it.skey.Hash(), it.name)
	if !ok {
		it.err = errs.Errorf("iterator data corruption: name not found")
		return
	}
}

func (it *Iterator) Key() histdb.Key { return it.skey }
func (it *Iterator) Name() []byte    { return it.name }
func (it *Iterator) Value() []byte   { return it.value }

func (it *Iterator) Err() error {
	if !errors.Is(it.err, io.EOF) {
		return it.err
	}
	return nil
}

func (it *Iterator) Seek(key histdb.Key) bool {
	if errors.Is(it.err, io.EOF) {
		it.err = nil
	} else if it.err != nil {
		return false
	}

	ent, _, err := it.kr.Search(key)
	if err != nil {
		it.err = err
		return false
	}

	it.size = 0
	it.span = nil
	it.offset = ent.ValOffset()

	// no fancy comparisons because the prefix likely matches.
	for it.Next() && string(it.skey[:]) < string(key[:]) {
	}

	return it.err == nil
}
