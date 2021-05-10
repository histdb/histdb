package floathist

import (
	"math"

	"github.com/zeebo/errs/v2"
)

func lowerValue(i, j, k uint32) float32 {
	obs := i<<l0Shift | j<<l1Shift | k<<l2Shift
	obs ^= ^uint32(int32(obs)>>31) | (1 << 31)
	return math.Float32frombits(obs)
}

func upperValue(i, j, k uint32) float32 {
	obs := i<<l0Shift | j<<l1Shift | k<<l2Shift | 1<<halfShift
	obs ^= ^uint32(int32(obs)>>31) | (1 << 31)
	return math.Float32frombits(obs)
}

type Histogram struct {
	l0 layer0
}

// Merge adds all of the values from g into h. It is not safe to call with
// concurrent mutations to g or h.
func (h *Histogram) Merge(g *Histogram) error {
	for bm := g.l0.bm.Clone(); !bm.Empty(); bm.Next() {
		l1idx := bm.Lowest()
		l1g := g.l0.l1s[l1idx]
		l1h := h.l0.l1s[l1idx]

		if l1h == nil {
			l1h = new(layer1)
			h.l0.l1s[l1idx] = l1h
			h.l0.bm.SetIdx(l1idx)
		}

		for bm := l1g.bm.Clone(); !bm.Empty(); bm.Next() {
			l2idx := bm.Lowest()
			l2g := l1g.l2s[l2idx]
			l2h := l1h.l2s[l2idx]

			if l2h == nil {
				l2h = newLayer2()
				l1h.l2s[l2idx] = l2h
				l1h.bm.SetIdx(l2idx)
			}

			for k := uint32(0); k < l2Size; k++ {
				count := layer2_loadCounter(l2g, k)
				if count == 0 {
					continue
				}

				if !layer2_addCounter(l2h, k, count) &&
					!layer2_upconvert(l2h, &l2h, false) &&
					!layer2_addCounter(l2h, k, count) {

					return errs.Errorf("bucket overflow when merging histograms")
				}
			}
		}
	}

	return nil
}

// Observe adds the value to the histogram.
//
// It is safe to be called concurrently.
func (h *Histogram) Observe(v float32) {
	if v != v || v > math.MaxFloat32 || v < -math.MaxFloat32 {
		return
	}

	bits := math.Float32bits(v)
	bits ^= uint32(int32(bits)>>31) | (1 << 31)

	l1idx := (bits >> l0Shift) % l0Size
	l2idx := (bits >> l1Shift) % l1Size
	idx := (bits >> l2Shift) % l2Size

	l1_addr := &h.l0.l1s[l1idx]
	l1 := layer1_load(l1_addr)
	if l1 == nil {
		l1 = new(layer1)
		if !layer1_cas(l1_addr, l1) {
			l1 = layer1_load(l1_addr)
		} else {
			h.l0.bm.SetIdx(l1idx)
		}
	}

	l2_addr := &l1.l2s[l2idx]
	l2 := layer2_load(l2_addr)
	if l2 == nil {
		l2 = newLayer2()
		if !layer2_cas(l2_addr, nil, l2) {
			l2 = layer2_load(l2_addr)
		} else {
			l1.bm.SetIdx(l2idx)
		}
	}

	if layer2_incCounter(l2, idx) {
		layer2_upconvert(l2, l2_addr, true)
	}
}

// Min returns an approximation of the smallest value stored in the histogram.
//
// It is safe to be called concurrently.
func (h *Histogram) Min() float32 {
	i := h.l0.bm.Clone().Lowest()
	l1 := layer1_load(&h.l0.l1s[i])

	j := l1.bm.Clone().Lowest()
	l2 := layer2_load(&l1.l2s[j])

	for k := uint32(0); k < l2Size; k++ {
		if layer2_loadCounter(l2, uint32(k)) > 0 {
			return lowerValue(i, j, k)
		}
	}
	return lowerValue(i, j, l2Size-1)
}

// Max returns an approximation of the largest value stored in the histogram.
//
// It is safe to be called concurrently.
func (h *Histogram) Max() float32 {
	i := h.l0.bm.Clone().Highest()
	l1 := layer1_load(&h.l0.l1s[i])

	j := l1.bm.Clone().Highest()
	l2 := layer2_load(&l1.l2s[j])

	for k := int32(l2Size) - 1; k >= 0; k-- {
		if layer2_loadCounter(l2, uint32(k)) > 0 {
			return upperValue(i, j, uint32(k))
		}
	}
	return upperValue(i, j, 0)
}

// Total returns the number of observations that have been recorded.
//
// It is safe to be called concurrently.
func (h *Histogram) Total() (total uint64) {
	for bm := h.l0.bm.Clone(); !bm.Empty(); bm.Next() {
		i := bm.Lowest()
		l1 := layer1_load(&h.l0.l1s[i])

		for bm := l1.bm.Clone(); !bm.Empty(); bm.Next() {
			j := bm.Lowest()
			l2 := layer2_load(&l1.l2s[j])

			total += sumLayer2(l2)
		}
	}

	return total
}

// Quantile returns an estimate of the value with the property that the
// fraction of values observed specified by q are smaller than it.
//
// It is safe to be called concurrently.
func (h *Histogram) Quantile(q float64) (v float32) {
	target, acc := uint64(q*float64(h.Total())+0.5), uint64(0)

	for bm := h.l0.bm.Clone(); !bm.Empty(); bm.Next() {
		i := bm.Lowest()
		l1 := layer1_load(&h.l0.l1s[i])

		for bm := l1.bm.Clone(); !bm.Empty(); bm.Next() {
			j := bm.Lowest()
			l2 := layer2_load(&l1.l2s[j])

			if bacc := acc + sumLayer2(l2); bacc < target {
				acc = bacc
				continue
			}

			for k := uint32(0); k < l2Size; k++ {
				count := layer2_loadCounter(l2, k)
				if acc+count >= target {
					if target-acc < (acc+count)-target {
						return lowerValue(i, j, k)
					}
					return upperValue(i, j, k)
				}
				acc += count
			}
		}
	}

	return h.Max()
}

// CDF returns an estimate of the fraction of values that are smaller than
// the requested value.
//
// It is safe to be called concurrently.
func (h *Histogram) CDF(v float32) float64 {
	obs := math.Float32bits(v)
	obs ^= uint32(int32(obs)>>31) | (1 << 31)

	obsTarget := obs & ((1<<(l0Bits+l1Bits) - 1) << (32 - l0Bits - l1Bits))
	obsCounters := (obs >> l2Shift) % l2Size

	var sum, total uint64

	for bm := h.l0.bm.Clone(); !bm.Empty(); bm.Next() {
		i := bm.Lowest()
		l1 := layer1_load(&h.l0.l1s[i])

		for bm := l1.bm.Clone(); !bm.Empty(); bm.Next() {
			j := bm.Lowest()
			l2 := layer2_load(&l1.l2s[j])

			bacc := sumLayer2(l2)
			total += bacc

			target := i<<l0Shift | j<<l1Shift
			if target < obsTarget {
				sum += bacc
			} else if target == obsTarget {
				for k := uint32(0); k < obsCounters; k++ {
					sum += layer2_loadCounter(l2, k)
				}
				sum += layer2_loadCounter(l2, obsCounters) / 2
			}
		}
	}

	return float64(sum) / float64(total)
}

// Summary returns the total number of observations and estimates of the
// sum of the values, the average of the values, and the variance of
// the values.
//
// It is safe to be called concurrently.
func (h *Histogram) Summary() (total, sum, avg, vari float64) {
	var total2 float64

	for bm := h.l0.bm.Clone(); !bm.Empty(); bm.Next() {
		i := bm.Lowest()
		l1 := layer1_load(&h.l0.l1s[i])

		for bm := l1.bm.Clone(); !bm.Empty(); bm.Next() {
			j := bm.Lowest()
			l2 := layer2_load(&l1.l2s[j])

			for k := uint32(0); k < l2Size; k++ {
				count := float64(layer2_loadCounter(l2, k))
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

	if total == 0 {
		return 0, 0, 0, 0
	} else if total == 1 {
		return 1, sum, sum, 0
	}
	return total, sum, sum / total, vari / (total - 1)
}

// Distribution calls the callback with information about the distribution
// observed by the histogram. Each call is provided with some value and
// the estimated amount of values observed smaller than or equal to it
// as well as the total number of observed values. The total may change
// between successive callbacks but will only increase and will always
// be at least as big as the count.
//
// It is safe to be called concurrently.
func (h *Histogram) Distribution(cb func(value float32, count, total uint64)) {
	acc, total := uint64(0), h.Total()

	for bm := h.l0.bm.Clone(); !bm.Empty(); bm.Next() {
		i := bm.Lowest()
		l1 := layer1_load(&h.l0.l1s[i])

		for bm := l1.bm.Clone(); !bm.Empty(); bm.Next() {
			j := bm.Lowest()
			l2 := layer2_load(&l1.l2s[j])

			for k := uint32(0); k < l2Size; k++ {
				count := layer2_loadCounter(l2, k)
				if count == 0 {
					continue
				}
				value := upperValue(i, j, k)

				acc += count
				if acc > total {
					total = h.Total()
					if acc > total {
						total = acc
					}
				}

				cb(value, acc, total)
			}
		}
	}
}
