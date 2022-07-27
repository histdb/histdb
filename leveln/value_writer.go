package leveln

import (
	"github.com/zeebo/errs/v2"

	"github.com/histdb/histdb"
	"github.com/histdb/histdb/filesystem"
)

const (
	vwSpanSize        = 4096 * 4
	vwSpanAlignBits   = 8
	vwSpanAlign       = 1 << vwSpanAlignBits
	vwSpanMask        = vwSpanAlign - 1
	vwEntryHeaderSize = 6
)

type valueWriter struct {
	fh   filesystem.Handle
	n    uint64
	sn   uint
	span [vwSpanSize]byte
}

func (v *valueWriter) Init(fh filesystem.Handle) {
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

func (v *valueWriter) Append(buf []byte, ts uint32, value []byte) {
	if len(buf) >= vwEntryHeaderSize {
		len := vwEntryHeaderSize + uint(len(value))
		buf[0] = uint8(len >> 8)
		buf[1] = uint8(len)
		buf[2] = uint8(ts >> 24)
		buf[3] = uint8(ts >> 16)
		buf[4] = uint8(ts >> 8)
		buf[5] = uint8(ts)
		copy(buf[vwEntryHeaderSize:], value)
		v.sn += len
	}
}

func (v *valueWriter) BeginSpan(key histdb.Key) {
	v.sn = histdb.HashSize
	copy(v.span[0:histdb.HashSize], key[:histdb.HashSize])
}

func (v *valueWriter) FinishSpan() (offset uint32, length uint8, err error) {
	// compute span beginning offset
	offset = uint32(v.n / vwSpanAlign)

	// round up to the alignment including end of span
	sn := (v.sn + 2 + vwSpanMask) &^ vwSpanMask

	// check for overflow
	if uint64(offset)*vwSpanAlign != v.n {
		return 0, 0, errs.Errorf("values file too large")
	} else if sn/vwSpanAlign > 255 {
		return 0, 0, errs.Errorf("values span too large")
	}

	v.n += uint64(sn)

	// add a zero entry to mark end of span
	v.span[v.sn] = 0
	v.span[v.sn+1] = 0

	_, err = v.fh.Write(v.span[:sn])

	return offset, uint8(sn / vwSpanAlign), errs.Wrap(err)
}
