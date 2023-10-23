package level0

import (
	"encoding/binary"
	"io"

	"github.com/zeebo/errs/v2"

	"github.com/histdb/histdb"
	"github.com/histdb/histdb/filesystem"
)

type Iterator struct {
	_ [0]func() // no equality

	eoff int64 // offset of ebuf
	elen int   // valid length of ebuf
	ebuf []byte

	keyb histdb.Key
	nbuf []byte // reference into ebuf
	vbuf []byte // reference into ebuf

	ibuf []byte
	idx  []byte // reference into ibuf
	idxf []byte // reference into ibuf

	err error
	fh  filesystem.Handle
}

func (it *Iterator) Init(fh filesystem.Handle) {
	it.fh = fh
	it.idx = nil
	it.idxf = nil
	it.err = nil
}

func (it *Iterator) readIndex() {
	if it.ibuf == nil {
		it.ibuf = make([]byte, L0IndexSize)
	}

	n, err := it.fh.ReadAt(it.ibuf[:], L0DataSize)
	if err != nil && err != io.EOF {
		it.err = errs.Wrap(err)
		return
	}

	split := uint(n) - 4
	if split >= uint(len(it.ibuf)) {
		it.err = errs.Errorf("invalid checksum: index too short")
		return
	}

	idx, sum := it.ibuf[:split], it.ibuf[split:]
	if exp, got := checksum(idx), binary.BigEndian.Uint32(sum); exp != got {
		it.err = errs.Errorf("invalid checksum: %08x != %08x", exp, got)
		return
	}

	it.idxf = idx
	it.idx = idx
}

func (it *Iterator) SeekFirst() bool {
	it.idx = it.idxf
	return it.Next()
}

func (it *Iterator) Next() bool {
	if it.err != nil {
		return false
	} else if it.idxf == nil {
		it.readIndex()
	}
	if len(it.idx) < 4 {
		return false
	}

	offset := int64(binary.BigEndian.Uint16(it.idx[2:4])) * l0EntryAlignment
	if offset == 0 {
		return false
	}

	it.idx = it.idx[4:]

	it.readEntryHeader(offset)
	it.readNameAndValue(offset)

	return it.err == nil
}

func (it *Iterator) read(offset, length int64) []byte {
	b := uint64(offset - it.eoff)
	e := b + uint64(length)
	if b < e && e < uint64(len(it.ebuf)) && e < uint64(it.elen) {
		return it.ebuf[b:e]
	}

	alloc := (length + 1023) &^ 1023
	if int64(len(it.ebuf)) < alloc {
		if int64(cap(it.ebuf)) >= alloc {
			it.ebuf = it.ebuf[:alloc]
		} else {
			it.ebuf = make([]byte, alloc)
		}
	}

	n, err := it.fh.ReadAt(it.ebuf, offset)
	if err != nil && err != io.EOF {
		it.err = errs.Wrap(err)
		return nil
	}
	it.eoff = offset
	it.elen = n

	if int64(n) < length {
		length = int64(n)
	}

	return it.ebuf[:length]
}

func (it *Iterator) readEntryHeader(offset int64) bool {
	ebuf := it.read(offset, l0EntryHeaderSize)
	if len(ebuf)+2+2 < len(it.keyb) {
		return false
	}
	copy(it.keyb[:], ebuf[2+2:])
	return true
}

func (it *Iterator) readNameAndValue(offset int64) int64 {
	if it.err != nil {
		return 0
	} else if len(it.ebuf) < l0EntryHeaderSize {
		it.err = errs.Errorf("invalid readNameAndValue")
		return 0
	}

	b := uint64(offset - it.eoff)
	e := b + uint64(l0EntryHeaderSize)
	ebuf := it.ebuf[b:e]

	nhead := int64(l0EntryHeaderSize)
	ntail := nhead + int64(binary.BigEndian.Uint16(ebuf[0:2]))
	etail := ntail + int64(binary.BigEndian.Uint16(ebuf[2:4]))
	elen := etail + l0ChecksumSize

	ebuf = it.read(offset, elen)
	if int64(len(ebuf)) != elen {
		it.err = errs.Errorf("unable to read full entry buffer")
		return 0
	}

	exp, got := checksum(ebuf[:etail]), binary.BigEndian.Uint32(ebuf[etail:elen])
	if exp != got {
		// if we have an empty record, ignore the checksum failure
		if it.keyb != (histdb.Key{}) && got != 0 {
			it.err = errs.Errorf("invalid entry checksum: %08x != %08x", exp, got)
		}
		return 0
	}

	it.nbuf = ebuf[nhead:ntail]
	it.vbuf = ebuf[ntail:etail]

	return (elen + l0EntryAlignmentMask) &^ l0EntryAlignmentMask
}

func (it *Iterator) Key() histdb.Key {
	return it.keyb
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

func (it *Iterator) Seek(key histdb.Key) bool {
	if it.err != nil {
		return false
	} else if it.idxf == nil {
		it.readIndex()
	}
	if len(it.idxf) < 4 {
		return false
	}

	kp := binary.BigEndian.Uint16(key[0:2])
	i, j := 0, len(it.idxf)/4
	for i < j {
		h := int(uint(i+j) / 2)

		// check if we have a value there at all
		offset := int64(binary.BigEndian.Uint16(it.idxf[4*h+2:])) * 32
		if offset == 0 {
			j = h
			continue
		}

		// check if the prefix is definitely larger/smaller
		kph := binary.BigEndian.Uint16(it.idxf[4*h:])
		if kph < kp {
			i = h + 1
			continue
		}
		if kph > kp {
			j = h
			continue
		}

		// have to read the whole entry
		if !it.readEntryHeader(offset) {
			return false
		} else if it.err != nil {
			return false
		}

		if string(it.keyb[:]) < string(key[:]) {
			i = h + 1
		} else {
			j = h
		}
	}

	it.idx = it.idxf[4*i:]

	return it.Next()
}
