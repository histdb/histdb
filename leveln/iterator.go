package leveln

import (
	"errors"
	"io"

	"github.com/zeebo/errs/v2"

	"github.com/histdb/histdb"
	"github.com/histdb/histdb/filesystem"
)

type Iterator struct {
	_ [0]func() // no equality

	stats struct {
		valueReads int64
	}

	err    error
	kr     keyReader
	values filesystem.H
	off    uint32

	key   histdb.Key
	value []byte

	coff  uint32 // current span offset
	sboff uint32 // buffered span start offset
	cpos  uint16 // current pos within span
	sblen uint16 // buffered span length

	sbuf [vwSpanSize]byte
}

func (it *Iterator) Init(keys, values filesystem.H) {
	it.stats.valueReads = 0

	it.err = nil
	it.kr.Init(keys)
	it.values = values
	it.off = 0

	it.key = histdb.Key{}
	it.value = nil

	it.coff = 0
	it.sboff = 0
	it.cpos = 0
	it.sblen = 0
}

func (it *Iterator) Key() histdb.Key { return it.key }
func (it *Iterator) Value() []byte   { return it.value }

func (it *Iterator) Err() error {
	if !errors.Is(it.err, io.EOF) {
		return it.err
	}
	return nil
}

func (it *Iterator) Next() bool {
	// if we don't have enough data left to read an entry header, update
	// our offset to point at the next span and ensure it's loaded
	if it.err != nil {
		return false
	}

again:

	// we need to load the entry at {coff, cpos} so first check to see if
	// that exists in the buffer
	sblo := it.sboff * vwSpanAlign
	sbhi := sblo + uint32(it.sblen)

	clo := it.coff*vwSpanAlign + uint32(it.cpos)
	chi := clo + 2

	if sblo > clo || chi > sbhi {
		// we need to load the span into the buffer
		var n int
		it.stats.valueReads++
		n, it.err = it.values.ReadAt(it.sbuf[:], int64(it.coff*vwSpanAlign))
		if errors.Is(it.err, io.EOF) {
			if n == 0 {
				return false
			}
			it.err = nil
		}
		if n&vwSpanMask != 0 {
			it.err = errs.Errorf("values span not aligned")
		}
		if it.err != nil {
			return false
		}

		it.sboff = it.coff
		it.sblen = uint16(n)
		goto again
	}

	// TODO: explicit bounds checking so no static panics and instead have
	// errors.

	prefix := it.sbuf[clo-sblo:]

	// if we're at the start of a span group, we need to read in the hash
	if it.cpos == 0 {
		*it.key.HashPtr() = histdb.Hash(prefix)
		it.cpos += histdb.HashSize
		prefix = prefix[histdb.HashSize:]
	}

	vend := be.Uint16(prefix)
	if vend == 0 {
		// we reached the end of the span. we have to round up to the next
		// offset and try again
		it.coff += uint32(it.cpos+vwSpanAlign+1) / vwSpanAlign
		it.cpos = 0
		goto again
	}

	// if the entry spans the buffer, we want to read starting at the current
	// offset instead. we're guaranteed to get enough data because the value
	// writer cannot write more than our buffer size before flushing.
	if len(prefix) < vwEntryHeaderSize || int(vend) > len(prefix) {
		it.sboff = 0
		it.sblen = 0
		goto again
	}

	it.key.SetTimestamp(be.Uint32(prefix[2:]))
	it.key.SetDuration(be.Uint32(prefix[6:]))
	it.value = prefix[vwEntryHeaderSize:vend]
	it.cpos += vend

	return true
}

func (it *Iterator) Seek(key histdb.Key) {
	if errors.Is(it.err, io.EOF) {
		it.err = nil
	} else if it.err != nil {
		return
	}

	var ent kwEntry
	ent, _, it.err = it.kr.Search(key)
	if it.err != nil {
		return
	}

	it.coff = ent.ValOffset()
	it.cpos = 0

	for it.Next() && string(it.key[:]) < string(key[:]) {
	}
}
