package floathist

import (
	"encoding/binary"
	"sync/atomic"

	"github.com/zeebo/errs/v2"
	"github.com/zeebo/lsm/floathist/internal/bitmap"
	"github.com/zeebo/lsm/floathist/internal/buffer"
)

func (h *Histogram) Serialize(mem []byte) []byte {
	le := binary.LittleEndian

	if cap(mem) < 64 {
		mem = make([]byte, 0, 64)
	}
	buf := buffer.Of(mem)

	bm := h.bm.Clone()

	buf = buf.Grow()
	le.PutUint64(buf.Front8()[:], bm.UnsafeUint())
	buf = buf.Advance(l0Size / 8)

	for {
		i, ok := bm.Next()
		if !ok {
			break
		}
		l1 := loadLayer1(&h.l1s[i])

		bm := l1.bm.Clone()

		buf = buf.Grow()
		le.PutUint64(buf.Front8()[:], bm.UnsafeUint())
		buf = buf.Advance(l1Size / 8)

		for {
			i, ok := bm.Next()
			if !ok {
				break
			}

			l2 := loadLayer2(&l1.l2s[i])
			var bm bitmap.B64

			buf = buf.Grow()
			pos := buf.Pos()
			buf = buf.Advance(l2Size / 8)

			for i := uint32(0); i < uint32(len(l2)); i++ {
				val := atomic.LoadUint64(&l2[i])
				if val == 0 {
					continue
				}

				bm.UnsafeSetIdx(i)

				buf = buf.Grow()
				nbytes := varintAppend(buf.Front9(), val)
				buf = buf.Advance(nbytes)
			}

			switch l2Size {
			case 16:
				le.PutUint16(buf.Index2(pos)[:], uint16(bm.UnsafeUint()))
			case 32:
				le.PutUint32(buf.Index4(pos)[:], uint32(bm.UnsafeUint()))
			case 64:
				le.PutUint64(buf.Index8(pos)[:], uint64(bm.UnsafeUint()))
			default:
				// TODO: this sucks
				panic("unhandled level2 size")
			}
		}
	}

	return buf.Prefix()
}

func (h *Histogram) Load(data []byte) (err error) {
	le := binary.LittleEndian
	buf := buffer.OfLen(data)

	var bm0 bitmap.B64
	var bm1 bitmap.B64
	var bm2 bitmap.B64

	if buf.Remaining() < 8 { // TODO: this sucks
		err = errs.Errorf("buffer too short")
		goto done
	}

	h.bm.UnsafeSetUint(le.Uint64(buf.Front8()[:]) & l0Mask)
	buf = buf.Advance(l0Size / 8)
	bm0 = h.bm.UnsafeClone()

	for {
		i, ok := bm0.Next()
		if !ok {
			break
		}

		l1 := new(layer1)
		h.l1s[i] = l1

		if buf.Remaining() < 8 { // TODO: this sucks
			err = errs.Errorf("buffer too short")
			goto done
		}

		l1.bm.UnsafeSetUint(le.Uint64(buf.Front8()[:]) & l1Mask)
		buf = buf.Advance(l1Size / 8)
		bm1 = l1.bm.UnsafeClone()

		for {
			i, ok := bm1.Next()
			if !ok {
				break
			}

			l2 := new(layer2)
			l1.l2s[i] = l2

			if buf.Remaining() < 8 { // TODO: this sucks
				err = errs.Errorf("buffer too short")
				goto done
			}

			bm2.UnsafeSetUint(le.Uint64(buf.Front8()[:]) & l2Mask)
			buf = buf.Advance(l2Size / 8)

			for {
				i, ok := bm2.Next()
				if !ok {
					break
				}

				if rem := buf.Remaining(); rem >= 9 {
					var nbytes uintptr
					nbytes, l2[i] = fastVarintConsume(buf.Front9())
					if nbytes > rem {
						err = errs.Errorf("invalid varint data")
						goto done
					}
					buf = buf.Advance(nbytes)

				} else {
					l2[i], buf, ok = safeVarintConsume(buf)
					if !ok {
						err = errs.Errorf("invalid varint data")
						goto done
					}
				}
			}
		}
	}

done:
	return err
}
