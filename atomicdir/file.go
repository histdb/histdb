package atomicdir

import (
	"encoding/binary"

	"github.com/histdb/histdb/hexx"
)

var be = binary.BigEndian

const (
	Kind_Level0 uint8 = iota
	Kind_LevelN_Keys
	Kind_LevelN_Values
	Kind_LevelN_Memidx
)

type File struct {
	GenLow  uint32
	GenHigh uint32
	Level   uint8
	Kind    uint8
}

func (f File) String() string {
	var buf [24]byte
	writeFile(&buf, f)
	return string(buf[:])
}

func writeFile(buf *[24]byte, f File) {
	*buf = [...]byte{
		'L', 'X', 'X',
		'K', 'X', 'X',
		'G', 'X', 'X', 'X', 'X', 'X', 'X', 'X', 'X',
		'-', 'X', 'X', 'X', 'X', 'X', 'X', 'X', 'X',
	}

	be.PutUint16(buf[1:3], hexx.Put8(f.Level))
	be.PutUint16(buf[4:6], hexx.Put8(f.Kind))
	be.PutUint64(buf[7:15], hexx.Put32(f.GenLow))
	be.PutUint64(buf[16:24], hexx.Put32(f.GenHigh))
}

func ParseFile(name string) (f File, ok bool) {
	if len(name) != 24 {
		return
	}
	return File{
			GenLow:  hexx.Get32(readUint64(name[7:15])),
			GenHigh: hexx.Get32(readUint64(name[16:24])),
			Level:   hexx.Get8(readUint16(name[1:3])),
			Kind:    hexx.Get8(readUint16(name[4:6])),
		}, true &&
			name[0] == 'L' &&
			name[3] == 'K' &&
			name[6] == 'G' &&
			name[15] == '-'
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
