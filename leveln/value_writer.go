package leveln

import (
	"github.com/zeebo/errs/v2"

	"github.com/histdb/histdb"
	"github.com/histdb/histdb/filesystem"
)

const (
	vwSpanSize = 4096 * 4

	vwSpanHashStart = 0
	vwSpanHashEnd   = vwSpanHashStart + histdb.HashSize

	vwSpanNameLengthIdx = vwSpanHashEnd
	vwSpanNameStart     = vwSpanNameLengthIdx + 1

	vwSpanAlignBits   = 8
	vwSpanAlign       = 1 << vwSpanAlignBits
	vwSpanMask        = vwSpanAlign - 1
	vwEntryHeaderSize = 6
)

type valueWriter struct {
	_ [0]func() // no equality

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
	if begin <= end && end <= vwSpanSize {
		return v.span[begin:end]
	}
	return nil
}

func (v *valueWriter) Append(buf []byte, ts uint32, value []byte) {
	if vwEntryHeaderSize <= len(buf) {
		elen := vwEntryHeaderSize + uint(len(value))

		be.PutUint16(buf[0:2], uint16(elen))
		be.PutUint32(buf[2:6], uint32(ts))
		copy(buf[vwEntryHeaderSize:], value)

		v.sn += elen
	}
}

func (v *valueWriter) BeginSpan(key histdb.Key, name []byte) bool {
	sn := vwSpanNameStart + uint(len(name))
	if span := v.span[:]; uint(len(name)) < 256 && vwSpanNameStart <= sn && sn <= uint(len(span)) {
		copy(span[vwSpanHashStart:vwSpanHashEnd], key[:histdb.HashSize])
		span[vwSpanNameLengthIdx] = byte(len(name))
		copy(span[vwSpanNameStart:sn], name)

		v.sn = sn
		return true
	}

	return false
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
	if span, vsn := v.span, v.sn; vsn < uint(len(span)) && vsn+1 < uint(len(span)) && sn < uint(len(span)) {
		v.span[vsn] = 0
		v.span[vsn+1] = 0
		_, err = v.fh.Write(span[:sn])
	} else {
		err = errs.Errorf("value writer corruption")
	}

	return offset, uint8(sn / vwSpanAlign), errs.Wrap(err)
}
