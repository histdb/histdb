package filesystem

import (
	"encoding/binary"

	"github.com/histdb/histdb/hexx"
)

var be = binary.BigEndian

type File struct {
	Low  uint32
	High uint32
	Kind byte
}

func (f File) WithKind(kind byte) File {
	f.Kind = kind
	return f
}

func (f File) String() string {
	var buf [22]byte
	writeFile(&buf, f)
	return string(buf[:])
}

const (
	KindIndx = 1
	KindKeys = 2
	KindVals = 3
)

var (
	rkinds = [256]byte{'i': 1, 'k': 2, 'v': 3}
	kinds  = [4][4]byte{
		{'x', 'x', 'x', 'x'},
		{'i', 'n', 'd', 'x'},
		{'k', 'e', 'y', 's'},
		{'v', 'a', 'l', 's'},
	}
)

func writeFile(buf *[22]byte, f File) {
	*buf = [...]byte{
		/**/ 'X', 'X', 'X', 'X', 'X', 'X', 'X', 'X',
		'-', 'X', 'X', 'X', 'X', 'X', 'X', 'X', 'X',
		'.', 'X', 'X', 'X', 'X',
	}

	be.PutUint64(buf[0:8], hexx.Put32(f.Low))
	be.PutUint64(buf[9:17], hexx.Put32(f.High))
	*(*[4]byte)(buf[18:22]) = kinds[f.Kind%4]
}

func ParseFile(name string) (f File, ok bool) {
	if len(name) != 22 {
		return
	}
	return File{
		Low:  hexx.Get32(readUint64(name[0:8])),
		High: hexx.Get32(readUint64(name[9:17])),
		Kind: rkinds[name[18]],
	}, name[8] == '-' && name[17] == '.'
}

//
// binary.BigEndian for strings
//

func readUint64(b string) uint64 {
	if len(b) < 8 {
		return 0
	}
	return 0 |
		uint64(b[7])<<0x00 | uint64(b[6])<<0x08 |
		uint64(b[5])<<0x10 | uint64(b[4])<<0x18 |
		uint64(b[3])<<0x20 | uint64(b[2])<<0x28 |
		uint64(b[1])<<0x30 | uint64(b[0])<<0x38
}

func readUint32(b string) uint32 {
	if len(b) < 4 {
		return 0
	}
	return 0 |
		uint32(b[3])<<0x00 | uint32(b[2])<<0x08 |
		uint32(b[1])<<0x10 | uint32(b[0])<<0x18
}

func readUint16(b string) uint16 {
	if len(b) < 2 {
		return 0
	}
	return 0 |
		uint16(b[1])<<0x00 | uint16(b[0])<<0x08
}
