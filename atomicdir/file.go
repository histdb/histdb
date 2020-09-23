package atomicdir

import (
	"encoding/binary"
	"path/filepath"
)

const (
	pathSep      = filepath.Separator
	hexDigits    = "0123456789ABCDEF"
	valueGEN0000 = 'G'<<56 | 'E'<<48 | 'N'<<40 | '-'<<32 | '0'<<24 | '0'<<16 | '0'<<8 | '0'
	valueL00     = 'L'<<24 | '0'<<16 | '0'<<8 | '-'
)

type File struct {
	Transaction uint16 // 0 => tmp
	Level       int8   // -1 => WAL
	Generation  uint32
}

func (f File) Name() string {
	var buf [21]byte
	f.writeName(&buf)
	return string(buf[:])
}

func (f File) writeName(buf *[21]byte) {
	*buf = [21]byte{
		't', 'e', 'm', 'p', 'd', 'a', 't', 'a', pathSep,
		'W', 'A', 'L', '-', '0', '0', '0', '0', '0', '0', '0', '0',
	}

	if f.Transaction > 0 {
		binary.BigEndian.PutUint64(buf[0:8], valueGEN0000)
		writeUint(buf[0:8], uint32(f.Transaction))
	}

	if f.Level >= 0 {
		binary.BigEndian.PutUint32(buf[9:13], valueL00)
		writeUint(buf[4:12], uint32(f.Level))
	}

	writeUint(buf[13:21], f.Generation)
}

func parseFile(name string) (f File, ok bool) {
	var txn, lvl uint32
	ok = true

	if len(name) != 21 || name[8] != pathSep || name[12] != '-' {
		goto bad
	}

	switch {
	case name[0:8] == "tempdata":
	case name[0:4] == "GEN-":
		txn, ok = readUint(name[4:8], ok)
	default:
		goto bad
	}

	switch {
	case name[9:13] == "WAL-":
		lvl = ^uint32(0)
	case name[9] == 'L':
		lvl, ok = readUint(name[10:12], ok)
	default:
		goto bad
	}

	f.Generation, ok = readUint(name[13:21], ok)
	f.Transaction = uint16(txn)
	f.Level = int8(lvl)
	return f, ok

bad:
	return File{}, false
}

func writeUint(x []byte, v uint32) {
	i := 7
next:
	if i >= 0 && v > 0 {
		x[i] = hexDigits[v%16]
		v, i = v/16, i-1
		goto next
	}
}

func readUint(x string, iok bool) (v uint32, ok bool) {
	i := 0
next:
	if i < len(x) {
		c := x[i]
		v *= 16
		if '0' <= c && c <= '9' {
			v += uint32(c - '0')
		} else if 'A' <= c && c <= 'F' {
			v += uint32(c + 10 - 'A')
		} else {
			return v, false
		}
		i++
		goto next
	}
	return v, iok && true
}
