package flathist

import (
	"encoding/binary"

	"github.com/histdb/histdb/bitmap"
	"github.com/histdb/histdb/rwutils"
)

// TODO:
//   1. instead of l0 bitmaps for serialization, each l2 is indexed by 8 bits
//      this is almost certainly a win. (it becomes 9 bytes per l2 instead of
//      8 bytes per l2 + 2 bytes per l1, so it's a win as long as there are
//      less than 2 l2s per l1. so maybe not almost certainly a win?)
//   2. instead of l2 being bitmap + non-zero varints, maybe it could be
//      64*2bits of lengths, and that many bytes? the lengths could mean like
//      {0, 1, 2, 8} so we don't write zeros, or {1, 2, 4, 8} which would
//      make a l2 at least 80 bytes.
//   3. maybe some other l2 serialization options?

const (
	_ uint = (l0Bits - 5) * (5 - l0Bits) // assumption: l0 is 2^4 bits
	_ uint = (l1Bits - 5) * (5 - l1Bits) // assumption: l1 is 2^4 bits
	_ uint = (l2Bits - 6) * (6 - l2Bits) // assumption: l2 is 2^6 bits
)

// AppendTo implements rwutils.RW and is not safe to call with concurrent mutations.
func AppendTo[T any](s *S[T], h H[T], w *rwutils.W) {
	l0 := s.l0.Get(h.v)

	bm := bitmask(&l0.l1)
	w.Uint32(bm)

	for bm := bitmap.New32(bm); !bm.Empty(); bm.ClearLowest() {
		i := bm.Lowest()
		l1 := s.getL1(l0.l1[i])

		bm := bitmask(&l1.l2)
		w.Uint32(bm)

		for bm := bitmap.New32(bm); !bm.Empty(); bm.ClearLowest() {
			i := bm.Lowest()
			l2a := l1.l2[i]
			var bm uint64

			bmSlot := w.StageUint64(9 * l2Size)

			if isAddrLarge(l2a) {
				for i, v := range &s.getL2L(l2a).cs {
					if v > 0 {
						bm |= 1 << i
						w.Varint(v)
					}
				}
			} else {
				for i, v := range &s.getL2S(l2a).cs {
					if v > 0 {
						bm |= 1 << i
						w.Varint(uint64(v))
					}
				}
			}

			binary.LittleEndian.PutUint64(bmSlot[:], bm)
		}
	}
}

// ReadFrom implements rwutils.RW and is not safe to call with concurrent mutations.
func ReadFrom[T any](s *S[T], h H[T], r *rwutils.R) {
	l0 := s.l0.Get(h.v)

	for bm := bitmap.New32(r.Uint32()); !bm.Empty(); bm.ClearLowest() {
		l1i := bm.Lowest()

		l1a := l0.l1[l1i]
		if l1a == 0 {
			l1a = s.l1.New().Raw() | (l2TagSmall << 29)
			l0.l1[l1i] = l1a
		}
		l1 := s.getL1(l1a)

		for bm := bitmap.New32(r.Uint32()); !bm.Empty(); bm.ClearLowest() {
			l2i := bm.Lowest()

			l2a := l1.l2[l2i]
			if l2a == 0 {
				l2a = s.l2s.New().Raw() | (l2TagSmall << 29)
				l1.l2[l2i] = l2a
			}

			var l2s *layer2Small
			var l2l *layer2Large
			if isAddrLarge(l2a) {
				l2l = s.getL2L(l2a)
			} else {
				l2s = s.getL2S(l2a)
			}

			for bm := bitmap.New64(r.Uint64()); !bm.Empty(); bm.ClearLowest() {
				k := bm.Lowest() % l2Size
				v := r.Varint()

				if l2l != nil {
					l2l.cs[k] += v
				} else if x := l2s.cs[k]; v > l2GrowAt || uint64(x)+v > l2GrowAt {
					l2a = s.l2l.New().Raw() | (l2TagLarge << 29)
					l1.l2[l2i] = l2a

					l2l = s.getL2L(l2a)
					for i := 0; i < l2Size; i++ {
						l2l.cs[i] = uint64(l2s.cs[i])
					}

					l2l.cs[k] = uint64(x) + v
				} else {
					l2s.cs[k] += uint32(v)
				}
			}
		}
	}
}
