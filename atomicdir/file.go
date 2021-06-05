package atomicdir

import (
	"encoding/binary"
	"path/filepath"
)

const (
	pathSep      = filepath.Separator
	hexDigits    = "0123456789ABCDEF"
	valueTXN0000 = 'T'<<56 | 'X'<<48 | 'N'<<40 | '-'<<32 | '0'<<24 | '0'<<16 | '0'<<8 | '0'
	valueL00     = 'L'<<24 | '0'<<16 | '0'<<8 | '-'
)

type File struct {
	Transaction uint16 // 0  => TEMPDATA
	Level       int8   // -1 => WAL
	Generation  uint32
}

func TransactionName(txn uint16) string {
	var buf [8]byte
	WriteTransactionName(&buf, txn)
	return string(buf[:])
}

func WriteTransactionName(buf *[8]byte, txn uint16) {
	if txn > 0 {
		binary.BigEndian.PutUint64(buf[0:8], valueTXN0000)
		writeUint(buf[0:8], uint32(txn))
	} else {
		*buf = [8]byte{'T', 'E', 'M', 'P', 'D', 'A', 'T', 'A'}
	}
}

func (f File) Name() string {
	var buf [21]byte
	f.WriteName(&buf)
	return string(buf[:])
}

func (f File) WriteName(buf *[21]byte) {
	*buf = [21]byte{
		'T', 'E', 'M', 'P', 'D', 'A', 'T', 'A', pathSep,
		'W', 'A', 'L', '-', '0', '0', '0', '0', '0', '0', '0', '0',
	}

	if f.Transaction > 0 {
		binary.BigEndian.PutUint64(buf[0:8], valueTXN0000)
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
	case name[0:8] == "TEMPDATA":
	case name[0:4] == "TXN-":
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
	for i := uint(7); i < uint(len(x)) && v > 0; i, v = i-1, v/16 {
		x[i] = hexDigits[v%16]
	}
}

func readUint(x string, iok bool) (v uint32, ok bool) {
	i := 0

next:
	if i < len(x) {
		v <<= 4
		switch c := uint32(x[i]); {
		case '0' <= c && c <= '9':
			v += c - '0'
		case 'A' <= c && c <= 'F':
			v += c + 10 - 'A'
		default:
			iok = false
			goto end
		}

		i++
		goto next
	}

end:
	return v, iok
}
