package leveln

import (
	"encoding/binary"
	"unsafe"

	"github.com/zeebo/errs"
	"github.com/zeebo/lsm"
	"github.com/zeebo/lsm/filesystem"
)

const (
	vwSpanSize        = 4096 * 4
	vwSpanAlignBits   = 8
	vwSpanAlign       = 1 << vwSpanAlignBits
	vwSpanMask        = vwSpanAlign - 1
	vwEntryHeaderSize = 6
)

type valueWriter struct {
	fh   filesystem.File
	n    uint64
	off  int64
	sn   uint
	span [vwSpanSize]byte
}

func (v *valueWriter) Init(fh filesystem.File) {
	v.fh = fh
}

func (v *valueWriter) CanAppend(value []byte) []byte {
	length := uint(len(value))
	begin := v.sn
	end := begin + vwEntryHeaderSize + length + 2 // final 2 for trailer 0s
	if begin < end && end < vwSpanSize {
		return v.span[begin:end]
	}
	return nil
}

func (v *valueWriter) Append(buf []byte, key lsm.Key, value []byte) {
	if len(buf) >= vwEntryHeaderSize {
		len := vwEntryHeaderSize + uint(len(value))
		binary.BigEndian.PutUint16(buf[0:2], uint16(len))
		// reduces code amount by a lot and allows Append to be inlined
		*(*[4]byte)(unsafe.Pointer(&buf[2])) = *(*[4]byte)(unsafe.Pointer(&key[16]))
		copy(buf[vwEntryHeaderSize:], value)
		v.sn += len
	}
}

func (v *valueWriter) BeginSpan(key lsm.Key) {
	v.sn = lsm.HashSize
	copy(v.span[0:lsm.HashSize], key[:lsm.HashSize])
}

func (v *valueWriter) FinishSpan() (offset, length uint32, err error) {
	// record span beginning offset and check for overflow
	offset = uint32(v.n / vwSpanAlign)
	if uint64(offset)*vwSpanAlign != v.n {
		return 0, 0, errs.New("values file too large")
	}

	// round up to the alignment and increase bytes written
	sn := (v.sn + 2 + vwSpanMask) &^ vwSpanMask
	v.n += uint64(sn)

	// add a zero entry to mark end of span
	v.span[v.sn] = 0
	v.span[v.sn+1] = 0

	// REVISIT: don't need to use WriteAt once Write doesn't leak params
	wn, err := v.fh.WriteAt(v.span[:sn], v.off)
	v.off += int64(wn)

	return offset, uint32(sn / vwSpanAlign), errs.Wrap(err)
}
