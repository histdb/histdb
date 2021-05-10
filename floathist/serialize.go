package floathist

import (
	"encoding/binary"

	"github.com/zeebo/errs/v2"
	"github.com/zeebo/lsm/floathist/internal/buffer"
)

func (h *Histogram) Serialize(mem []byte) []byte {
	le := binary.LittleEndian

	if cap(mem) < 64 {
		mem = make([]byte, 0, 64)
	}
	buf := buffer.Of(mem)

	bm := h.l0.bm.Clone()

	buf = buf.Grow()
	le.PutUint64(buf.Front8()[:], bm.uint64())
	buf = buf.Advance(l0Size / 8)

	for ; !bm.Empty(); bm.Next() {
		i := bm.Lowest()
		l1 := layer1_load(&h.l0.l1s[i])

		bm := l1.bm.Clone()

		buf = buf.Grow()
		le.PutUint64(buf.Front8()[:], bm.Uint64())
		buf = buf.Advance(l1Size / 8)

		for ; !bm.Empty(); bm.Next() {
			i := bm.Lowest()
			l2 := layer2_load(&l1.l2s[i])
			var bm l2Bitmap

			buf = buf.Grow()
			pos := buf.Pos()
			buf = buf.Advance(l2Size / 8)

			for i := uint32(0); i < l2Size; i++ {
				val := layer2_loadCounter(l2, i)
				if val == 0 {
					continue
				}

				bm.UnsafeSetIdx(i)

				buf = buf.Grow()
				nbytes := varintAppend(buf.Front9(), val)
				buf = buf.Advance(nbytes)
			}

			switch l2Size {
			case 8:
				*buf.Index(pos) = uint8(bm.Uint64())
			case 16:
				le.PutUint16(buf.Index2(pos)[:], uint16(bm.Uint64()))
			case 32:
				le.PutUint32(buf.Index4(pos)[:], uint32(bm.Uint64()))
			case 64:
				le.PutUint64(buf.Index8(pos)[:], uint64(bm.Uint64()))
			default:
				panic("unhandled level2 size")
			}
		}
	}

	return buf.Grow().Advance(9).Prefix()
}

func (h *Histogram) Load(data []byte) (err error) {
	le := binary.LittleEndian
	buf := buffer.OfLen(data)

	if buf.Remaining() < 8 {
		err = errs.Errorf("buffer too short")
		goto done
	}

	h.l0.bm.UnsafeSetUint(le.Uint64(buf.Front8()[:]))
	buf = buf.Advance(l0Size / 8)

	for bm := h.l0.bm.Clone(); !bm.Empty(); bm.Next() {
		i := bm.Lowest()
		l1 := new(layer1)
		h.l0.l1s[i] = l1

		if buf.Remaining() < 8 {
			err = errs.Errorf("buffer too short")
			goto done
		}

		l1.bm.UnsafeSetUint(le.Uint64(buf.Front8()[:]))
		buf = buf.Advance(l1Size / 8)

		for bm := l1.bm.Clone(); !bm.Empty(); bm.Next() {
			i := bm.Lowest()
			l2 := newLayer2()

			if buf.Remaining() < 8 {
				err = errs.Errorf("buffer too short")
				goto done
			}

			bm := l2Bitmap{le.Uint64(buf.Front8()[:])}
			buf = buf.Advance(l2Size / 8)

			for ; !bm.Empty(); bm.Next() {
				i := bm.Lowest()

				rem := buf.Remaining()
				if rem < 9 {
					err = errs.Errorf("buffer too short")
					goto done
				}

				nbytes, val := fastVarintConsume(buf.Front9())
				if nbytes > rem {
					err = errs.Errorf("invalid varint data")
					goto done
				}
				buf = buf.Advance(nbytes)

				if !layer2_unsafeSetCounter(l2, i, val) &&
					!layer2_upconvert(l2, &l2, false) &&
					!layer2_unsafeSetCounter(l2, i, val) {
					err = errs.Errorf("value too large to set")
					goto done
				}
			}

			l1.l2s[i] = l2
		}
	}

done:
	return err
}
