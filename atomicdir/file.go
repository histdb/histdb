package atomicdir

import (
	"encoding/binary"
	"path/filepath"

	"github.com/histdb/histdb/hexx"
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
	be.PutUint64(buf[0:8], hexx.Put32(f.Generation))
	be.PutUint16(buf[10:12], hexx.Put8(f.Level))
	be.PutUint16(buf[14:16], hexx.Put8(f.Kind))
}

func parseFile(name string) (f File, ok bool) {
	if len(name) == 16 {
		f.Generation = hexx.Get32(readUint64(name[0:8]))
		f.Level = hexx.Get8(readUint16(name[10:12]))
		f.Kind = hexx.Get8(readUint16(name[14:16]))
		ok = true
	}
	return
}

func writeDirectoryName(buf *[8]byte, tid uint32) string {
	*buf = [...]byte{'0', '0', '0', '0', '0', '0', '0', '0'}
	be.PutUint64(buf[0:8], hexx.Put32(tid))
	return string(buf[:])
}

func parseDirectoryName(name string) (tid uint32, ok bool) {
	if len(name) == 8 {
		tid = hexx.Get32(readUint64(name[0:8]))
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
	be.PutUint64(buf[0:8], hexx.Put32(tid))
	be.PutUint64(buf[9:17], hexx.Put32(f.Generation))
	be.PutUint16(buf[19:21], hexx.Put8(f.Level))
	be.PutUint16(buf[23:25], hexx.Put8(f.Kind))
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
