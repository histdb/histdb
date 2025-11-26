package leveln

import (
	"github.com/zeebo/errs/v2"

	"github.com/histdb/histdb"
	"github.com/histdb/histdb/filesystem"
)

const (
	vwSpanSize = 4096 * 8

	vwSpanAlignBits   = 8
	vwSpanAlign       = 1 << vwSpanAlignBits
	vwSpanMask        = vwSpanAlign - 1
	vwEntryHeaderSize = 2 + 4 + 4 // elen + ts + dur
)

type valueWriter struct {
	_ [0]func() // no equality

	fh   filesystem.H
	n    uint64
	sn   uint
	span [vwSpanSize]byte
}

func (v *valueWriter) Init(fh filesystem.H) {
	v.fh = fh
	v.n = 0
	v.sn = 0
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

func (v *valueWriter) Append(buf []byte, ts, dur uint32, value []byte) {
	if vwEntryHeaderSize <= len(buf) {
		elen := vwEntryHeaderSize + uint(len(value))

		be.PutUint16(buf[0:2], uint16(elen))
		be.PutUint32(buf[2:6], uint32(ts))
		be.PutUint32(buf[6:10], uint32(dur))
		copy(buf[vwEntryHeaderSize:], value)

		v.sn += elen
	}
}

func (v *valueWriter) BeginSpan(hash histdb.Hash) {
	*(*histdb.Hash)(v.span[0:histdb.HashSize]) = hash
	v.sn = histdb.HashSize
}

func (v *valueWriter) FinishSpan() (offset uint32, length uint8, err error) {
	// compute span beginning offset
	offset = uint32(v.n / vwSpanAlign)

	// round up to the alignment including end of span
	sn := (v.sn + 2 + vwSpanMask) &^ vwSpanMask

	// check for overflow
	if uint64(offset)*vwSpanAlign != v.n {
		return 0, 0, errs.Errorf("values file too large")
	} else if sn > vwSpanSize || sn/vwSpanAlign > 256 {
		return 0, 0, errs.Errorf("values span too large")
	}

	v.n += uint64(sn)

	// add a zero entry to mark end of span
	if vsn := v.sn; vsn <= uint(len(v.span)) && sn <= uint(len(v.span)) && vsn < sn {
		r := v.span[vsn:sn]
		for i := range r {
			r[i] = 0
		}
		_, err = v.fh.Write(v.span[:sn])
	} else {
		err = errs.Errorf("value writer corruption: vsn:%d sn:%d", vsn, sn)
	}

	return offset, uint8(sn / vwSpanAlign), errs.Wrap(err)
}
