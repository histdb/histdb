package floathist

import (
	"math"
	"sync/atomic"

	"github.com/zeebo/errs/v2"
)

func lowerValue(i, j, k uint32) float32 {
	obs := i<<l0Sh | j<<l1Sh | k<<l2Sh
	obs ^= ^uint32(int32(obs)>>31) | (1 << 31)
	return math.Float32frombits(obs)
}

func upperValue(i, j, k uint32) float32 {
	obs := i<<l0Sh | j<<l1Sh | k<<l2Sh | 1<<halfSh
	obs ^= ^uint32(int32(obs)>>31) | (1 << 31)
	return math.Float32frombits(obs)
}

type T struct {
	_ [0]func() // no equality

	l0 layer0
}

// Reset clears the histogram without freeing the memory it is using. It is
// not safe to call with concurrent reads or writes to h.
func (t *T) Reset() {
	for bm := t.l0.bm.AtomicClone(); !bm.Empty(); bm.ClearLowest() {
		i := bm.Lowest()
		l1 := layer1_load(&t.l0.l1s[i])

		for bm := l1.bm.AtomicClone(); !bm.Empty(); bm.ClearLowest() {
			j := bm.Lowest()
			l2 := layer2_load(&l1.l2s[j])

			layer2_reset(l2)
		}
	}
}

// Merge adds all of the values from g into h. It is not safe to call with
// concurrent mutations to g or h.
func (t *T) Merge(g *T) error {
	for bm := g.l0.bm.AtomicClone(); !bm.Empty(); bm.ClearLowest() {
		l1idx := bm.Lowest()
		l1g := g.l0.l1s[l1idx]
		l1h := t.l0.l1s[l1idx]

		if l1h == nil {
			l1h = new(layer1)
			t.l0.l1s[l1idx] = l1h
			t.l0.bm.AtomicSetIdx(l1idx)
		}

		for bm := l1g.bm.AtomicClone(); !bm.Empty(); bm.ClearLowest() {
			l2idx := bm.Lowest()
			l2g := l1g.l2s[l2idx]
			l2h := l1h.l2s[l2idx]

			if l2h == nil {
				l2h = newLayer2()
				l1h.bm.AtomicSetIdx(l2idx)
			}

			for k := uint32(0); k < l2S; k++ {
				hn := layer2_loadCounter(l2h, k)
				gn := layer2_loadCounter(l2g, k)
				if gn == 0 {
					continue
				}

				if !layer2_unsafeSetCounter(l2h, &l2h, k, hn+gn) {
					return errs.Errorf("unexpected bucket overflow when merging")
				}
			}

			l1h.l2s[l2idx] = l2h
		}
	}

	return nil
}

// Observe adds the value to the histogram.
//
// It is safe to be called concurrently.
func (t *T) Observe(v float32) {
	if v != v || v > math.MaxFloat32 || v < -math.MaxFloat32 {
		return
	}

	bits := math.Float32bits(v)
	bits ^= uint32(int32(bits)>>31) | (1 << 31)

	l1idx := (bits >> l0Sh) % l0S
	l2idx := (bits >> l1Sh) % l1S
	idx := (bits >> l2Sh) % l2S

	l1_addr := &t.l0.l1s[l1idx]
	l1 := layer1_load(l1_addr)
	if l1 == nil {
		l1 = new(layer1)
		if !layer1_cas(l1_addr, l1) {
			l1 = layer1_load(l1_addr)
		} else {
			t.l0.bm.AtomicSetIdx(l1idx)
		}
	}

	l2_addr := &l1.l2s[l2idx]
	l2 := layer2_load(l2_addr)
	if l2 == nil {
		l2 = newLayer2()
		if !layer2_cas(l2_addr, nil, l2) {
			l2 = layer2_load(l2_addr)
		} else {
			l1.bm.AtomicSetIdx(l2idx)
		}
	}

	switch layer2_tag(l2) {
	case tagLayer2Small:
		if atomic.AddUint32(&layer2_asSmall(l2)[idx], 1) > markAt {
			layer2_mark(l2, l2_addr)
		}

	case tagLayer2Marked:
		if atomic.AddUint32(&layer2_asSmall(l2)[idx], 1) > growAt {
			layer2_grow(l2, l2_addr, true)
		}

	case tagLayer2Growing:
		atomic.AddUint32(&layer2_asSmall(l2)[idx], 1)

	case tagLayer2Large:
		atomic.AddUint64(&layer2_asLarge(l2)[idx], 1)
	}
}

// Min returns an approximation of the smallest value stored in the histogram.
//
// It is safe to be called concurrently.
func (t *T) Min() float32 {
	bm0 := t.l0.bm.AtomicClone()
	i := bm0.Lowest()
	l1 := layer1_load(&t.l0.l1s[i])
	if l1 == nil {
		return float32(math.NaN())
	}

	bm1 := l1.bm.AtomicClone()
	j := bm1.Lowest()
	l2 := layer2_load(&l1.l2s[j])
	if l2 == nil {
		return float32(math.NaN())
	}

	for k := uint32(0); k < l2S; k++ {
		if layer2_loadCounter(l2, uint32(k)) > 0 {
			return lowerValue(i, j, k)
		}
	}
	return lowerValue(i, j, l2S-1)
}

// Max returns an approximation of the largest value stored in the histogram.
//
// It is safe to be called concurrently.
func (t *T) Max() float32 {
	bm0 := t.l0.bm.AtomicClone()
	i := bm0.Highest()
	l1 := layer1_load(&t.l0.l1s[i])
	if l1 == nil {
		return float32(math.NaN())
	}

	bm1 := l1.bm.AtomicClone()
	j := bm1.Highest()
	l2 := layer2_load(&l1.l2s[j])
	if l2 == nil {
		return float32(math.NaN())
	}

	for k := int32(l2S) - 1; k >= 0; k-- {
		if layer2_loadCounter(l2, uint32(k)) > 0 {
			return upperValue(i, j, uint32(k))
		}
	}
	return upperValue(i, j, 0)
}

// Total returns the number of observations that have been recorded.
//
// It is safe to be called concurrently.
func (t *T) Total() (total uint64) {
	for bm := t.l0.bm.AtomicClone(); !bm.Empty(); bm.ClearLowest() {
		i := bm.Lowest()
		l1 := layer1_load(&t.l0.l1s[i])

		for bm := l1.bm.AtomicClone(); !bm.Empty(); bm.ClearLowest() {
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
func (t *T) Quantile(q float64) (v float32) {
	target, acc := uint64(q*float64(t.Total())+0.5), uint64(0)

	for bm := t.l0.bm.AtomicClone(); !bm.Empty(); bm.ClearLowest() {
		i := bm.Lowest()
		l1 := layer1_load(&t.l0.l1s[i])

		for bm := l1.bm.AtomicClone(); !bm.Empty(); bm.ClearLowest() {
			j := bm.Lowest()
			l2 := layer2_load(&l1.l2s[j])

			if bacc := acc + sumLayer2(l2); bacc < target {
				acc = bacc
				continue
			}

			for k := uint32(0); k < l2S; k++ {
				acc += layer2_loadCounter(l2, k)
				if acc > target {
					return lowerValue(i, j, k)
				}
			}
		}
	}

	return t.Max()
}

// CDF returns an estimate of the fraction of values that are smaller than
// the requested value.
//
// It is safe to be called concurrently.
func (t *T) CDF(v float32) float64 {
	obs := math.Float32bits(v)
	obs ^= uint32(int32(obs)>>31) | (1 << 31)

	obsTarget := obs & ((1<<(l0B+l1B) - 1) << (32 - l0B - l1B))
	obsCounters := (obs >> l2Sh) % l2S

	var sum, total uint64

	for bm := t.l0.bm.AtomicClone(); !bm.Empty(); bm.ClearLowest() {
		i := bm.Lowest()
		l1 := layer1_load(&t.l0.l1s[i])

		for bm := l1.bm.AtomicClone(); !bm.Empty(); bm.ClearLowest() {
			j := bm.Lowest()
			l2 := layer2_load(&l1.l2s[j])

			bacc := sumLayer2(l2)
			total += bacc

			target := i<<l0Sh | j<<l1Sh
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
func (t *T) Summary() (total, sum, avg, vari float64) {
	var total2 float64

	for bm := t.l0.bm.AtomicClone(); !bm.Empty(); bm.ClearLowest() {
		i := bm.Lowest()
		l1 := layer1_load(&t.l0.l1s[i])

		for bm := l1.bm.AtomicClone(); !bm.Empty(); bm.ClearLowest() {
			j := bm.Lowest()
			l2 := layer2_load(&l1.l2s[j])

			for k := uint32(0); k < l2S; k++ {
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
func (t *T) Distribution(cb func(value float32, count, total uint64)) {
	acc, total := uint64(0), t.Total()

	for bm := t.l0.bm.AtomicClone(); !bm.Empty(); bm.ClearLowest() {
		i := bm.Lowest()
		l1 := layer1_load(&t.l0.l1s[i])

		for bm := l1.bm.AtomicClone(); !bm.Empty(); bm.ClearLowest() {
			j := bm.Lowest()
			l2 := layer2_load(&l1.l2s[j])

			for k := uint32(0); k < l2S; k++ {
				count := layer2_loadCounter(l2, k)
				if count == 0 {
					continue
				}
				value := upperValue(i, j, k)

				acc += count
				if acc > total {
					total = t.Total()
					if acc > total {
						total = acc
					}
				}

				cb(value, acc, total)
			}
		}
	}
}
