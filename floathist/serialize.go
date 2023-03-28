package floathist

import (
	"encoding/binary"

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

// AppendTo implements rwutils.RW and is not safe to call with concurrent mutations.
func (h *Histogram) AppendTo(w *rwutils.W) {
	bm := h.l0.bm.AtomicClone()
	w.Varint(bm.Uint64())

	for ; !bm.Empty(); bm.Next() {
		i := bm.Lowest()
		l1 := layer1_load(&h.l0.l1s[i])

		bm := l1.bm.AtomicClone()
		w.Varint(bm.Uint64())

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
func (h *Histogram) ReadFrom(r *rwutils.R) {
	bm := newL0Bitmap(r.Varint())
	for ; !bm.Empty(); bm.Next() {
		l1i := bm.Lowest()
		l1 := h.l0.l1s[l1i]

		if l1 == nil {
			l1 = new(layer1)
			h.l0.l1s[l1i] = l1
			h.l0.bm.unsafeSetIdx(l1i)
		}

		bm := newL1Bitmap(r.Varint())
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
