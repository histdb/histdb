package atomicdir

import (
	"encoding/binary"
	"path/filepath"
)

var be = binary.BigEndian

type File struct {
	Generation uint32
	Level      uint8
	Kind       uint8
}

func (f File) String() string {
	var buf [16]byte
	writeFile(&buf, f)
	return string(buf[:])
}

func writeFile(buf *[16]byte, f File) {
	*buf = [...]byte{
		'0', '0', '0', '0', '0', '0', '0', '0',
		'-', 'L', '0', '0',
		'-', 'K', '0', '0',
	}
	be.PutUint64(buf[0:8], hexUint32(f.Generation))
	be.PutUint16(buf[10:12], hexUint8(f.Level))
	be.PutUint16(buf[14:16], hexUint8(f.Kind))
}

func parseFile(name string) (f File, ok bool) {
	if len(name) == 16 {
		f.Generation = unhexUint32(readUint64(name[0:8]))
		f.Level = unhexUint8(readUint16(name[10:12]))
		f.Kind = unhexUint8(readUint16(name[14:16]))
		ok = true
	}
	return
}

func writeDirectoryName(buf *[8]byte, tid uint32) string {
	*buf = [...]byte{'0', '0', '0', '0', '0', '0', '0', '0'}
	be.PutUint64(buf[0:8], hexUint32(tid))
	return string(buf[:])
}

func parseDirectoryName(name string) (tid uint32, ok bool) {
	if len(name) == 8 {
		tid = unhexUint32(readUint64(name[0:8]))
		ok = true
	}
	return
}

func writeDirectoryFile(buf *[25]byte, tid uint32, f File) {
	*buf = [...]byte{
		'0', '0', '0', '0', '0', '0', '0', '0',
		filepath.Separator,
		'0', '0', '0', '0', '0', '0', '0', '0',
		'-', 'L',
		'0', '0',
		'-', 'K',
		'0', '0',
	}
	be.PutUint64(buf[0:8], hexUint32(tid))
	be.PutUint64(buf[9:17], hexUint32(f.Generation))
	be.PutUint16(buf[19:21], hexUint8(f.Level))
	be.PutUint16(buf[23:25], hexUint8(f.Kind))
}

//
// hex + unhex
//

func hexUint32(x uint32) (v uint64) {
	v = uint64(uint16(x)) | uint64(x)<<16
	v = (v & 0x000000FF000000FF) | ((v & 0x0000FF000000FF00) << 8)
	v = (v & 0x000F000F000F000F) | ((v & 0x00F000F000F000F0) << 4)
	return v + 0x3030303030303030 + 7*((v+0x0606060606060606)>>4&0x0101010101010101)
}

func unhexUint32(x uint64) (v uint32) {
	x = 9*(x&0x4040404040404040>>6) + (x & 0x0f0f0f0f0f0f0f0f)
	x = (x | x>>4) & 0x00FF00FF00FF00FF
	x = (x | x>>8) & 0x0000FFFF0000FFFF
	return uint32(x | x>>16)
}

// func hexUint16(x uint16) (v uint32) {
// 	v = uint32(uint8(x)) | uint32(x)<<8
// 	v = (v & 0x000F000F) | ((v & 0x00F000F0) << 4)
// 	return v + 0x30303030 + 7*((v+0x06060606)>>4&0x01010101)
// }

// func unhexUint16(x uint32) (v uint16) {
// 	x = 9*(x&0x40404040>>6) + (x & 0x0f0f0f0f)
// 	x = (x | x>>4) & 0x00FF00FF
// 	return uint16(x | x>>8)
// }

func hexUint8(x uint8) (v uint16) {
	v = uint16(x)
	v = (v & 0x000F) | ((v & 0x00F0) << 4)
	return v + 0x3030 + 7*((v+0x0606)>>4&0x0101)
}

func unhexUint8(x uint16) (v uint8) {
	x = 9*(x&0x4040>>6) + (x & 0x0f0f)
	return uint8(x | x>>4)
}

//
// binary.BigEndian for strings
//

func readUint64(b string) uint64 {
	return uint64(b[7]) | uint64(b[6])<<8 | uint64(b[5])<<16 | uint64(b[4])<<24 |
		uint64(b[3])<<32 | uint64(b[2])<<40 | uint64(b[1])<<48 | uint64(b[0])<<56
}

// func readUint32(b string) uint32 {
// 	return uint32(b[3]) | uint32(b[2])<<8 | uint32(b[1])<<16 | uint32(b[0])<<24
// }

func readUint16(b string) uint16 {
	return uint16(b[1]) | uint16(b[0])<<8
}
