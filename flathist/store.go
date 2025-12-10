package flathist

import (
	"math"
	"sync"
	"sync/atomic"

	"github.com/histdb/histdb/arena"
	"github.com/histdb/histdb/bitmap"
	"github.com/histdb/histdb/sizeof"
)

// S arena allocates histograms.
type S struct {
	_ [0]func() // no equality

	l0  arena.T[layer0]
	l1  arena.T[layer1]
	l2s arena.T[layer2Small]
	l2l arena.T[layer2Large]

	mu      sync.Mutex
	growing []growFinalize
}

func (s *S) Size() uint64 {
	return 0 +
		/* l0      */ s.l0.Size() +
		/* l1      */ s.l1.Size() +
		/* l2s     */ s.l2s.Size() +
		/* l2l     */ s.l2l.Size() +
		/* mu      */ 8 +
		/* growing */ sizeof.Slice(s.growing) +
		0
}

type Stats struct {
	Size uint64
	L0   uint32
	L1   uint32
	L2S  uint32
	L2L  uint32
}

func (s *S) Count() uint32 { return s.l0.Allocated() }

func (s *S) Stats() Stats {
	return Stats{
		Size: s.Size(),
		L0:   s.l0.Allocated(),
		L1:   s.l1.Allocated(),
		L2S:  s.l2s.Allocated(),
		L2L:  s.l2l.Allocated(),
	}
}

type growFinalize struct {
	l2so *layer2Small
	l2sc *layer2Small
	l2l  *layer2Large
}

// H is a handle to a histogram.
type H struct {
	v arena.P[layer0]
}

// UnsafeRawH lets one construct a handle for any store from a raw pointer
// value. It is obviously very unsafe and should only be used when you know
// what's up.
func UnsafeRawH(x uint32) H { return H{v: arena.Raw[layer0](x)} }

// Raw returns the raw value for the handle so that it can be reconstructed from
// UnsafeRawH.
func (h H) Raw() uint32 { return h.v.Raw() }

// New allocates a new histogram and returns a handle.
func (s *S) New() H { return H{v: s.l0.New()} }

// Iterate calls the callback with every allocated handle.
func (s *S) Iterate(cb func(h H) bool) {
	for i := range s.Count() {
		if !cb(UnsafeRawH(i + 1)) {
			return
		}
	}
}

func (s *S) getL0(h H) *layer0 {
	return s.l0.Get(h.v)
}

func (s *S) getL1(v uint32) *layer1 {
	return s.l1.Get(arena.Raw[layer1](v & lAddrMask))
}

func (s *S) getL2S(v uint32) *layer2Small {
	return s.l2s.Get(arena.Raw[layer2Small](v & lAddrMask))
}

func (s *S) getL2L(v uint32) *layer2Large {
	return s.l2l.Get(arena.Raw[layer2Large](v & lAddrMask))
}

// Finalize updates all of the histograms that were growing and perhaps missed
// an observation.
//
// It is not safe to call concurrently with Observe.
func (s *S) Finalize() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, gf := range s.growing {
		for i := range l2Size {
			if d := gf.l2so.cs[i] - gf.l2sc.cs[i]; d > 0 {
				gf.l2l.cs[i] += uint64(d)
			}
		}
	}
	s.growing = nil
}

// Merge copies the data from h into g. It is not safe to call with Observe on
// either g or h.
//
// TODO: maybe have an optimization in the arena for a small number of
// allocations like maybe a static buffer of some small size that it uses first,
// but it would suck to have to special case every Get call, so think more!
func Merge(s *S, h H, t *S, g H) {
	hl0 := s.l0.Get(h.v)
	gl0 := t.l0.Get(g.v)

	for bm := bitmap.New32(bitmask(&gl0.l1)); !bm.Empty(); bm.ClearLowest() {
		l1idx := bm.Lowest()

		hl1a := hl0.l1[l1idx]
		if hl1a == 0 {
			hl1a = s.l1.New().Raw() | (l2TagSmall << 29)
			hl0.l1[l1idx] = hl1a
		}

		hl1 := s.getL1(hl1a)
		gl1 := t.getL1(gl0.l1[l1idx])

		for bm := bitmap.New32(bitmask(&gl1.l2)); !bm.Empty(); bm.ClearLowest() {
			l2idx := bm.Lowest()

			gl2a := gl1.l2[l2idx]

			hl2a := hl1.l2[l2idx]
			if hl2a == 0 {
				if isAddrLarge(gl2a) {
					hl2a = s.l2l.New().Raw() | (l2TagLarge << 29)
					hl1.l2[l2idx] = hl2a
				} else {
					hl2a = s.l2s.New().Raw() | (l2TagSmall << 29)
					hl1.l2[l2idx] = hl2a
				}
			}

			var gl2l *layer2Large
			var gl2s *layer2Small
			if isAddrLarge(gl2a) {
				gl2l = t.getL2L(gl2a)
			} else {
				gl2s = t.getL2S(gl2a)
			}

			var hl2l *layer2Large
			var hl2s *layer2Small
			if isAddrLarge(hl2a) {
				hl2l = s.getL2L(hl2a)
			} else {
				hl2s = s.getL2S(hl2a)
			}

			for k := range l2Size {
				var gv uint64
				if gl2l != nil {
					gv = gl2l.cs[k]
				} else {
					gv = uint64(gl2s.cs[k])
				}

				if hl2l != nil {
					hl2l.cs[k] += gv
					continue
				}

				hv := uint64(hl2s.cs[k])

				if gv > l2GrowAt || gv+hv > l2GrowAt {
					hl2a = s.l2l.New().Raw() | (l2TagLarge << 29)
					hl1.l2[l2idx] = hl2a

					hl2l = s.getL2L(hl2a)
					for i := range l2Size {
						hl2l.cs[i] = uint64(hl2s.cs[i])
					}
					hl2l.cs[k] += gv
				} else {
					hl2s.cs[k] = uint32(hv + gv)
				}
			}
		}
	}
}

func Equal(s *S, h H, t *S, g H) bool {
	hl0 := s.l0.Get(h.v)
	hl0bm := bitmask(&hl0.l1)

	gl0 := t.l0.Get(g.v)
	gl0bm := bitmask(&gl0.l1)

	if hl0bm != gl0bm {
		return false
	}

	for bm := bitmap.New32(hl0bm); !bm.Empty(); bm.ClearLowest() {
		l1idx := bm.Lowest()

		hl1 := s.getL1(hl0.l1[l1idx])
		hl1bm := bitmask(&hl1.l2)

		gl1 := t.getL1(gl0.l1[l1idx])
		gl1bm := bitmask(&gl1.l2)

		if hl1bm != gl1bm {
			return false
		}

		for bm := bitmap.New32(hl1bm); !bm.Empty(); bm.ClearLowest() {
			l2idx := bm.Lowest()

			gl2a := gl1.l2[l2idx]
			gl2aLarge := isAddrLarge(gl2a)

			hl2a := hl1.l2[l2idx]
			hl2aLarge := isAddrLarge(hl2a)

			if hl2aLarge != gl2aLarge {
				return false
			}

			if hl2aLarge {
				if *s.getL2L(hl2a) != *t.getL2L(gl2a) {
					return false
				}
			} else {
				if *s.getL2S(hl2a) != *t.getL2S(gl2a) {
					return false
				}
			}
		}
	}

	return true
}

// Observe adds the value to the histogram.
//
// It is safe to be called concurrently.
func (s *S) Observe(h H, v float32) {
	if v != v || v > math.MaxFloat32 || v < -math.MaxFloat32 {
		return
	}

	l0 := s.l0.Get(h.v)

	bits := math.Float32bits(v)
	bits ^= uint32(int32(bits)>>31) | (1 << 31)

	l0i := (bits >> l0Shift) % l0Size
	l1i := (bits >> l1Shift) % l1Size
	l2i := (bits >> l2Shift) % l2Size

	l1a := atomic.LoadUint32(&l0.l1[l0i])
	if l1a == 0 {
		l1a = s.l1.New().Raw() | (l2TagSmall << 29)
		if !atomic.CompareAndSwapUint32(&l0.l1[l0i], 0, l1a) {
			l1a = atomic.LoadUint32(&l0.l1[l0i])
		}
	}
	l1 := s.getL1(l1a)

	l2a := atomic.LoadUint32(&l1.l2[l1i])
	if l2a == 0 {
		l2a = s.l2s.New().Raw() | (l2TagSmall << 29)
		if !atomic.CompareAndSwapUint32(&l1.l2[l1i], 0, l2a) {
			l2a = atomic.LoadUint32(&l1.l2[l1i])
		}
	}

	switch addrTag(l2a) {
	case l2TagSmall:
		l2s := s.getL2S(l2a)
		if atomic.AddUint32(&l2s.cs[l2i], 1) > l2GrowAt {
			l2aSlot := &l1.l2[l1i]
			if atomic.CompareAndSwapUint32(l2aSlot, l2a, l2a|(l2TagGrowing<<29)) {
				s.growLayer2(l2s, l2a, l2aSlot)
			}
		}

	case l2TagGrowing:
		atomic.AddUint32(&s.getL2S(l2a).cs[l2i], 1)

	case l2TagLarge:
		atomic.AddUint64(&s.getL2L(l2a).cs[l2i], 1)
	}
}

func (s *S) growLayer2(l2so *layer2Small, l2a uint32, l2aSlot *uint32) {
	l2la := s.l2l.New()
	l2l := s.getL2L(l2la.Raw())
	l2sc := new(layer2Small)

	for i := range l2Size {
		v := atomic.LoadUint32(&l2so.cs[i])
		l2l.cs[i] = uint64(v)
		l2sc.cs[i] = v
	}

	s.mu.Lock()
	s.growing = append(s.growing, growFinalize{
		l2so: l2so,
		l2sc: l2sc,
		l2l:  l2l,
	})
	s.mu.Unlock()

	atomic.StoreUint32(l2aSlot, l2la.Raw()|(l2TagLarge<<29))
}

// Min returns an approximation of the smallest value stored in the histogram.
//
// It is safe to be called concurrently with Observe.
func (s *S) Min(h H) float32 {
	l0 := s.l0.Get(h.v)

	i := uint32(bitmap.New32(bitmask(&l0.l1)).Lowest())
	l1a := atomic.LoadUint32(&l0.l1[i])
	if l1a == 0 {
		return float32(math.NaN())
	}
	l1 := s.getL1(l1a)

	j := uint32(bitmap.New32(bitmask(&l1.l2)).Lowest())
	l2a := atomic.LoadUint32(&l1.l2[j])
	if l2a == 0 {
		return float32(math.NaN())
	}

	if isAddrLarge(l2a) {
		l2l := s.getL2L(l2a)
		for k := range l2Size {
			if atomic.LoadUint64(&l2l.cs[k]) > 0 {
				return lowerValue(i, j, uint32(k))
			}
		}
	} else {
		l2s := s.getL2S(l2a)
		for k := range l2Size {
			if atomic.LoadUint32(&l2s.cs[k]) > 0 {
				return lowerValue(i, j, uint32(k))
			}
		}
	}

	return lowerValue(i, j, l2Size-1)
}

// Max returns an approximation of the largest value stored in the histogram.
//
// It is safe to be called concurrently with Observe.
func (s *S) Max(h H) float32 {
	l0 := s.l0.Get(h.v)

	i := uint32(bitmap.New32(bitmask(&l0.l1)).Highest())
	l1a := atomic.LoadUint32(&l0.l1[i])
	if l1a == 0 {
		return float32(math.NaN())
	}
	l1 := s.getL1(l1a)

	j := uint32(bitmap.New32(bitmask(&l1.l2)).Highest())
	l2a := atomic.LoadUint32(&l1.l2[j])
	if l2a == 0 {
		return float32(math.NaN())
	}

	if isAddrLarge(l2a) {
		l2l := s.getL2L(l2a)
		for k := l2Size - 1; k >= 0; k-- {
			if atomic.LoadUint64(&l2l.cs[k]) > 0 {
				return upperValue(i, j, uint32(k))
			}
		}
	} else {
		l2s := s.getL2S(l2a)
		for k := l2Size - 1; k >= 0; k-- {
			if atomic.LoadUint32(&l2s.cs[k]) > 0 {
				return upperValue(i, j, uint32(k))
			}
		}
	}

	return upperValue(i, j, 0)
}

// Reset clears all observations from the histogram.
//
// It is NOT safe to be called concurrently with any other method.
func (s *S) Reset(h H) {
	l0 := s.l0.Get(h.v)
	for bm := bitmap.New32(bitmask(&l0.l1)); !bm.Empty(); bm.ClearLowest() {
		l1 := s.getL1(l0.l1[bm.Lowest()])

		for bm := bitmap.New32(bitmask(&l1.l2)); !bm.Empty(); bm.ClearLowest() {
			l2a := l1.l2[bm.Lowest()]

			if isAddrLarge(l2a) {
				l2l := s.getL2L(l2a)
				for i := range l2l.cs {
					l2l.cs[i] = 0
				}
			} else {
				l2s := s.getL2S(l2a)
				for i := range l2s.cs {
					l2s.cs[i] = 0
				}
			}
		}
	}
}

// Total returns the number of observations that have been recorded.
//
// It is safe to be called concurrently with Observe.
func (s *S) Total(h H) (total uint64) {
	l0 := s.l0.Get(h.v)
	for bm := bitmap.New32(bitmask(&l0.l1)); !bm.Empty(); bm.ClearLowest() {
		l1 := s.getL1(atomic.LoadUint32(&l0.l1[bm.Lowest()]))

		for bm := bitmap.New32(bitmask(&l1.l2)); !bm.Empty(); bm.ClearLowest() {
			l2a := atomic.LoadUint32(&l1.l2[bm.Lowest()])

			if isAddrLarge(l2a) {
				total += sumLayer2Large(s.getL2L(l2a))
			} else {
				total += sumLayer2Small(s.getL2S(l2a))
			}
		}
	}

	return total
}

// Quantile returns an estimate of the value with the property that the fraction
// of values observed specified by q are smaller than it.
//
// It is safe to be called concurrently with Observe.
func (s *S) Quantile(h H, q float64) (v float32) {
	target, acc := uint64(q*float64(s.Total(h))+0.5), uint64(0)

	l0 := s.l0.Get(h.v) // TODO: total did this. hmm.

	for bm := bitmap.New32(bitmask(&l0.l1)); !bm.Empty(); bm.ClearLowest() {
		i := uint32(bm.Lowest())
		l1 := s.getL1(atomic.LoadUint32(&l0.l1[i]))

		for bm := bitmap.New32(bitmask(&l1.l2)); !bm.Empty(); bm.ClearLowest() {
			j := uint32(bm.Lowest())
			l2a := atomic.LoadUint32(&l1.l2[j])

			var l2s uint64
			if isAddrLarge(l2a) {
				l2s = sumLayer2Large(s.getL2L(l2a))
			} else {
				l2s = sumLayer2Small(s.getL2S(l2a))
			}

			if bacc := acc + l2s; bacc < target {
				acc = bacc
				continue
			}

			if isAddrLarge(l2a) {
				l2l := s.getL2L(l2a)
				for k := range uint32(l2Size) {
					acc += atomic.LoadUint64(&l2l.cs[k])
					if acc > target {
						return lowerValue(i, j, k)
					}
				}
			} else {
				l2s := s.getL2S(l2a)
				for k := range uint32(l2Size) {
					acc += uint64(atomic.LoadUint32(&l2s.cs[k]))
					if acc > target {
						return lowerValue(i, j, k)
					}
				}
			}
		}
	}

	return s.Max(h)
}

// CDF returns an estimate of the fraction of values that are smaller than the
// requested value.
//
// It is safe to be called concurrently with Observe.
func (s *S) CDF(h H, v float32) float64 {
	obs := math.Float32bits(v)
	obs ^= uint32(int32(obs)>>31) | (1 << 31)

	obsTarget := obs & ((1<<(l0Bits+l1Bits) - 1) << (32 - l0Bits - l1Bits))
	obsCounters := (obs >> l2Shift) % l2Size

	var sum, total uint64

	l0 := s.l0.Get(h.v)
	for bm := bitmap.New32(bitmask(&l0.l1)); !bm.Empty(); bm.ClearLowest() {
		i := uint32(bm.Lowest())
		l1 := s.getL1(atomic.LoadUint32(&l0.l1[i]))

		for bm := bitmap.New32(bitmask(&l1.l2)); !bm.Empty(); bm.ClearLowest() {
			j := uint32(bm.Lowest())
			l2a := atomic.LoadUint32(&l1.l2[j])

			var bacc uint64
			if isAddrLarge(l2a) {
				bacc = sumLayer2Large(s.getL2L(l2a))
			} else {
				bacc = sumLayer2Small(s.getL2S(l2a))
			}

			total += bacc

			target := i<<l0Shift | j<<l1Shift
			if target < obsTarget {
				sum += bacc
			} else if target == obsTarget {
				if isAddrLarge(l2a) {
					l2l := s.getL2L(l2a)
					for k := range obsCounters {
						sum += atomic.LoadUint64(&l2l.cs[k])
					}
					sum += atomic.LoadUint64(&l2l.cs[obsCounters]) / 2
				} else {
					l2s := s.getL2S(l2a)
					for k := range obsCounters {
						sum += uint64(atomic.LoadUint32(&l2s.cs[k]))
					}
					sum += uint64(atomic.LoadUint32(&l2s.cs[obsCounters])) / 2
				}
			}
		}
	}

	return float64(sum) / float64(total)
}

// Summary returns the total number of observations and estimates of the sum of
// the values, the average of the values, and the variance of the values.
//
// It is safe to be called concurrently with Observe.
func (s *S) Summary(h H) (total, sum, avg, vari float64) {
	var total2 float64

	l0 := s.l0.Get(h.v)
	for bm := bitmap.New32(bitmask(&l0.l1)); !bm.Empty(); bm.ClearLowest() {
		i := uint32(bm.Lowest())
		l1 := s.getL1(atomic.LoadUint32(&l0.l1[i]))

		for bm := bitmap.New32(bitmask(&l1.l2)); !bm.Empty(); bm.ClearLowest() {
			j := uint32(bm.Lowest())
			l2a := atomic.LoadUint32(&l1.l2[j])

			if isAddrLarge(l2a) {
				l2l := s.getL2L(l2a)
				for k := range uint32(l2Size) {
					count := float64(atomic.LoadUint64(&l2l.cs[k]))
					if count == 0 {
						continue
					}
					value := float64(upperValue(i, j, k))

					total += count
					total2 += count * count
					avg_ := avg
					avg += (count / total) * (value - avg_)
					sum += count * value
					vari += count * (value - avg_) * (value - avg)
				}
			} else {
				l2s := s.getL2S(l2a)
				for k := range uint32(l2Size) {
					count := float64(atomic.LoadUint32(&l2s.cs[k]))
					if count == 0 {
						continue
					}
					value := float64(upperValue(i, j, k))

					total += count
					total2 += count * count
					avg_ := avg
					avg += (count / total) * (value - avg_)
					sum += count * value
					vari += count * (value - avg_) * (value - avg)
				}
			}
		}
	}

	switch total {
	case 0:
		return 0, 0, 0, 0
	case 1:
		return 1, sum, sum, 0
	default:
		return total, sum, sum / total, vari / (total - 1)
	}
}

// Distribution calls the callback with information about the distribution
// observed by the histogram. Each call is provided with some value and the
// estimated amount of values observed smaller than or equal to it as well as
// the total number of observed values. The total may change between successive
// callbacks but will only increase and will always be at least as big as the
// count.
//
// It is safe to be called concurrently with Observe.
func (s *S) Distribution(h H, cb func(value float32, count, total uint64)) {
	acc, total := uint64(0), s.Total(h)

	l0 := s.l0.Get(h.v)
	for bm := bitmap.New32(bitmask(&l0.l1)); !bm.Empty(); bm.ClearLowest() {
		i := uint32(bm.Lowest())
		l1 := s.getL1(atomic.LoadUint32(&l0.l1[i]))

		for bm := bitmap.New32(bitmask(&l1.l2)); !bm.Empty(); bm.ClearLowest() {
			j := uint32(bm.Lowest())
			l2a := atomic.LoadUint32(&l1.l2[j])

			if isAddrLarge(l2a) {
				l2l := s.getL2L(l2a)
				for k := range uint32(l2Size) {
					count := atomic.LoadUint64(&l2l.cs[k])
					if count == 0 {
						continue
					}
					value := upperValue(i, j, k)

					acc += count
					if acc > total {
						total = max(acc, s.Total(h))
					}

					cb(value, acc, total)
				}
			} else {
				l2s := s.getL2S(l2a)
				for k := range uint32(l2Size) {
					count := uint64(atomic.LoadUint32(&l2s.cs[k]))
					if count == 0 {
						continue
					}
					value := upperValue(i, j, k)

					acc += count
					if acc > total {
						total = max(acc, s.Total(h))
					}

					cb(value, acc, total)
				}
			}
		}
	}
}
