package leveln

import (
	"encoding/binary"

	"github.com/zeebo/errs"
	"github.com/zeebo/lsm"
	"github.com/zeebo/lsm/filesystem"
)

const (
	vwSpanSize      = 4096 * 4
	vwSpanAlignBits = 8
	vwSpanAlign     = 1 << vwSpanAlignBits
	vwSpanMask      = vwSpanAlign - 1
)

type valueWriter struct {
	fh   filesystem.File
	n    uint64
	sn   uint
	span [vwSpanSize]byte
}

func (v *valueWriter) Init(fh filesystem.File) {
	v.fh = fh
}

func (v *valueWriter) CanAppend(value []byte) []byte {
	length := uint(len(value))
	begin := v.sn
	end := 2 + begin + 4 + length + 2
	if begin < end && end < vwSpanSize {
		return v.span[begin:end]
	}
	return nil
}

func (v *valueWriter) Append(buf []byte, key lsm.Key, value []byte) {
	_ = buf[6]
	binary.BigEndian.PutUint16(buf[0:2], uint16(4+len(value)))
	copy(buf[2:6], key[16:20]) // inlined to allow Append to be inlined
	copy(buf[6:], value)
	v.sn += 6 + uint(len(value))
}

func (v *valueWriter) BeginSpan(key lsm.Key) {
	v.sn = 16
	copy(v.span[0:16], key[:])
}

func (v *valueWriter) FinishSpan() (offset uint32, err error) {
	// record span beginning offset and check for overflow
	offset = uint32(v.n / vwSpanAlign)
	if uint64(offset)*vwSpanAlign != v.n {
		return offset, errs.New("values file too large")
	}

	// round up to the alignment and increase bytes written
	sn := (v.sn + 2 + vwSpanMask) &^ vwSpanMask
	v.n += uint64(sn)

	// add a zero entry to mark end of span
	v.span[v.sn] = 0
	v.span[v.sn+1] = 0

	_, err = v.fh.Write(v.span[:sn])
	return offset, errs.Wrap(err)
}
