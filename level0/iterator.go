package level0

import (
	"encoding/binary"
	"io"

	"github.com/zeebo/errs/v2"

	"github.com/histdb/histdb"
	"github.com/histdb/histdb/filesystem"
)

type Iterator struct {
	ebuf []byte
	nbuf []byte // reference into ebuf
	vbuf []byte // reference into ebuf
	idx  []byte // reference into idxb
	idxf []byte // reference into idxb
	err  error

	fh   filesystem.Handle
	keyb histdb.Key
	idxb [l0IndexSize]byte
}

func (it *Iterator) Init(fh filesystem.Handle) {
	it.fh = fh
	it.idx = nil
	it.idxf = nil
	it.err = nil
}

func (it *Iterator) readIndex() {
	n, err := it.fh.ReadAt(it.idxb[:], l0DataSize)
	if err != nil && err != io.EOF {
		it.err = errs.Wrap(err)
		return
	}

	split := uint(n) - 4
	if split >= uint(len(it.idxb)) {
		it.err = errs.Errorf("invalid checksum: index too short")
		return
	}

	idx, sum := it.idxb[:split], it.idxb[split:]
	if exp, got := checksum(idx), binary.BigEndian.Uint32(sum); exp != got {
		it.err = errs.Errorf("invalid checksum: %08x != %08x", exp, got)
		return
	}

	it.idxf = idx
	it.idx = idx
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

func (it *Iterator) readEntryHeader(offset int64) {
	if len(it.ebuf) < l0EntryHeaderSize {
		it.ebuf = make([]byte, 256)
	}

	if _, err := it.fh.ReadAt(it.ebuf, offset); err != nil && err != io.EOF {
		it.err = errs.Wrap(err)
		return
	}

	copy(it.keyb[:], it.ebuf[8:])
}

func (it *Iterator) readNameAndValue(offset int64) int64 {
	if it.err != nil {
		return 0
	} else if len(it.ebuf) < l0EntryHeaderSize {
		it.err = errs.Errorf("invalid readNameAndValue")
		return 0
	}

	nhead := int64(l0EntryHeaderSize)
	ntail := nhead + int64(binary.BigEndian.Uint32(it.ebuf[0:4]))
	etail := ntail + int64(binary.BigEndian.Uint32(it.ebuf[4:8]))
	elen := etail + l0ChecksumSize

	if int64(len(it.ebuf)) < elen {
		it.ebuf = make([]byte, elen)

		if _, it.err = it.fh.ReadAt(it.ebuf, offset); it.err != nil {
			it.err = errs.Wrap(it.err)
			return 0
		}
	}

	exp, got := checksum(it.ebuf[:etail]), binary.BigEndian.Uint32(it.ebuf[etail:elen])
	if exp != got {
		it.err = errs.Errorf("invalid entry checksum: %08x != %08x", exp, got)
		return 0
	}

	it.nbuf = it.ebuf[nhead:ntail]
	it.vbuf = it.ebuf[ntail:etail]

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
		it.readEntryHeader(offset)
		if it.err != nil {
			return false
		}

		if histdb.KeyCmp.Less(it.Key(), key) {
			i = h + 1
		} else {
			j = h
		}
	}

	it.idx = it.idxf[4*i:]

	return it.Next()
}
