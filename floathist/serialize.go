package floathist

import (
	"encoding/binary"
	"math"

	"github.com/zeebo/errs/v2"

	"github.com/histdb/histdb/rwutils"
)

// TODO:
//   1. instead of l0 bitmaps for serialization, each l2 is indexed by 8 bits
//      this is almost certainly a win.
//   2. instead of l2 being bitmap + non-zero varints, maybe it could be
//      64*2bits of lengths, and that many bytes? the lengths could mean like
//      {0, 1, 2, 8} so we don't write zeros, or {1, 2, 4, 8} which would
//      make a l2 at least 80 bytes.
//   3. maybe some other l2 serialization options?

func WriteSingle(buf *[13]byte, v float32) {
	bits := math.Float32bits(v)
	bits ^= uint32(int32(bits)>>31) | (1 << 31)
	binary.LittleEndian.PutUint16(buf[0:2], 1<<((bits>>l0Sh)%l0S))
	binary.LittleEndian.PutUint16(buf[2:4], 1<<((bits>>l1Sh)%l1S))
	binary.LittleEndian.PutUint64(buf[4:12], 1<<((bits>>l2Sh)%l2S))
	buf[12] = 2 // varint encoding of 1
}

const (
	_ uint = (l0B - 4) * (4 - l0B) // assumption: l0 is 2^4 bits
	_ uint = (l1B - 4) * (4 - l1B) // assumption: l1 is 2^4 bits
	_ uint = (l2B - 6) * (6 - l2B) // assumption: l2 is 2^6 bits
)

// AppendTo implements rwutils.RW and is not safe to call with concurrent mutations.
func AppendTo(t *T, w *rwutils.W) {
	bm := t.l0.bm.AtomicClone()
	w.Uint16(uint16(bm.Uint64()))

	for ; !bm.Empty(); bm.Next() {
		i := bm.Lowest()
		l1 := layer1_load(&t.l0.l1s[i])

		bm := l1.bm.AtomicClone()
		w.Uint16(uint16(bm.Uint64()))

		for ; !bm.Empty(); bm.Next() {
			i := bm.Lowest()
			l2 := layer2_load(&l1.l2s[i])

			var bm l2Bitmap
			bmSlot := w.StageUint64(9 * l2S)

			for i := uint32(0); i < l2S; i++ {
				if val := layer2_loadCounter(l2, i); val > 0 {
					bm.unsafeSetIdx(i)
					w.Varint(val)
				}
			}

			binary.LittleEndian.PutUint64(bmSlot[:], bm.Uint64())
		}
	}
}

// ReadFrom implements rwutils.RW and is not safe to call with concurrent mutations.
func ReadFrom(t *T, r *rwutils.R) {
	bm := newL0Bitmap(uint64(r.Uint16()))
	for ; !bm.Empty(); bm.Next() {
		l1i := bm.Lowest()
		l1 := t.l0.l1s[l1i]

		if l1 == nil {
			l1 = new(layer1)
			t.l0.l1s[l1i] = l1
			t.l0.bm.unsafeSetIdx(l1i)
		}

		bm := newL1Bitmap(uint64(r.Uint16()))
		for ; !bm.Empty(); bm.Next() {
			l2i := bm.Lowest()
			l2 := l1.l2s[l2i]

			if l2 == nil {
				l2 = newLayer2()
				l1.bm.unsafeSetIdx(l2i)
			}

			bm := newL2Bitmap(r.Uint64())
			for ; !bm.Empty(); bm.Next() {
				k := bm.Lowest()

				val := r.Varint() + layer2_loadCounter(l2, k)
				if !layer2_unsafeSetCounter(l2, &l2, k, val) {
					r.Invalid(errs.Errorf("value too large to set"))
				}
			}

			l1.l2s[l2i] = l2
		}
	}
}
