package floathist

import (
	"math"
)

type Histogram struct {
	l0 layer0
}

func (h *Histogram) Observe(v float32) {
	if v != v || v > math.MaxFloat32 || v < -math.MaxFloat32 {
		return
	}

	bits := math.Float32bits(v)
	bits ^= uint32(int32(bits)>>31) | (1 << 31)

	l1idx := (bits >> l0Shift) % l0Size
	l2idx := (bits >> l1Shift) % l1Size
	idx := (bits >> l2Shift) % l2Size

	l1addr := &h.l0.l1s[l1idx]
	l1 := layer1Load(l1addr)
	if l1 == nil {
		l1 = new(layer1)
		if !layer1CAS(l1addr, l1) {
			l1 = layer1Load(l1addr)
		} else {
			h.l0.bm.SetIdx(l1idx)
		}
	}

	l2addr := &l1.l2s[l2idx]
	l2 := layer2_load(l2addr)
	if l2 == nil {
		l2 = newLayer2()
		if !layer2_cas(l2addr, nil, l2) {
			l2 = layer2_load(l2addr)
		} else {
			l1.bm.SetIdx(l2idx)
		}
	}

	if layer2_addCounter(l2, idx, 1) {
		layer2_upconvert(l2, l2addr, true)
	}
}

func (h *Histogram) Total() (total int64) {
	bm := h.l0.bm.Clone()
	for {
		i, ok := bm.Next()
		if !ok {
			break
		}
		l1 := layer1Load(&h.l0.l1s[i])

		bm := l1.bm.Clone()
		for {
			i, ok := bm.Next()
			if !ok {
				break
			}
			l2 := layer2_load(&l1.l2s[i])

			for i := uint32(0); i < l2Size; i++ {
				total += int64(layer2_loadCounter(l2, i))
			}
		}
	}

	return total
}

func (h *Histogram) Quantile(q float64) float32 {
	target, acc := uint64(q*float64(h.Total())+0.5), uint64(0)

	bm := h.l0.bm.Clone()
	for {
		i, ok := bm.Next()
		if !ok {
			break
		}
		l1 := layer1Load(&h.l0.l1s[i])

		bm := l1.bm.Clone()
		for {
			j, ok := bm.Next()
			if !ok {
				break
			}
			l2 := layer2_load(&l1.l2s[j])

			for k := uint32(0); k < l2Size; k++ {
				acc += layer2_loadCounter(l2, k)
				if acc >= target {
					obs := i<<l0Shift | j<<l1Shift | k<<l2Shift
					obs ^= ^uint32(int32(obs)>>31) | (1 << 31)
					return math.Float32frombits(obs)
				}
			}
		}
	}

	return math.Float32frombits((1<<15 - 1) << 17)
}

func (h *Histogram) CDF(v float32) float64 {
	obs := math.Float32bits(v)
	obs ^= uint32(int32(obs)>>31) | (1 << 31)

	var sum, total uint64

	bm := h.l0.bm.Clone()
	for {
		i, ok := bm.Next()
		if !ok {
			break
		}
		l1 := layer1Load(&h.l0.l1s[i])

		bm := l1.bm.Clone()
		for {
			j, ok := bm.Next()
			if !ok {
				break
			}
			l2 := layer2_load(&l1.l2s[j])

			target := i<<l0Shift | j<<l1Shift

			for k := uint32(0); k < l2Size; k++ {
				count := layer2_loadCounter(l2, k)
				if obs >= target {
					sum += count
					target += 1 << l2Shift
				}
				total += count
			}
		}
	}

	return float64(sum) / float64(total)
}

func (h *Histogram) Sum() (sum float64) {
	bm := h.l0.bm.Clone()
	for {
		i, ok := bm.Next()
		if !ok {
			break
		}
		l1 := layer1Load(&h.l0.l1s[i])

		bm := l1.bm.Clone()
		for {
			j, ok := bm.Next()
			if !ok {
				break
			}
			l2 := layer2_load(&l1.l2s[j])

			for k := uint32(0); k < l2Size; k++ {
				count := float64(layer2_loadCounter(l2, k))
				obs := i<<l0Shift | j<<l1Shift | k<<l2Shift | 1<<halfShift
				obs ^= ^uint32(int32(obs)>>31) | (1 << 31)
				value := float64(math.Float32frombits(obs))

				sum += count * value
			}
		}
	}

	return sum
}

func (h *Histogram) Average() (sum, avg float64) {
	var total float64

	bm := h.l0.bm.Clone()
	for {
		i, ok := bm.Next()
		if !ok {
			break
		}
		l1 := layer1Load(&h.l0.l1s[i])

		bm := l1.bm.Clone()
		for {
			j, ok := bm.Next()
			if !ok {
				break
			}
			l2 := layer2_load(&l1.l2s[j])

			for k := uint32(0); k < l2Size; k++ {
				count := float64(layer2_loadCounter(l2, k))
				obs := i<<l0Shift | j<<l1Shift | k<<l2Shift | 1<<halfShift
				obs ^= ^uint32(int32(obs)>>31) | (1 << 31)
				value := float64(math.Float32frombits(obs))

				total += count
				sum += count * value
			}
		}
	}

	if total == 0 {
		return 0, 0
	}
	return sum, sum / total
}

func (h *Histogram) Variance() (sum, avg, vari float64) {
	var total, total2 float64

	bm := h.l0.bm.Clone()
	for {
		i, ok := bm.Next()
		if !ok {
			break
		}
		l1 := layer1Load(&h.l0.l1s[i])

		bm := l1.bm.Clone()
		for {
			j, ok := bm.Next()
			if !ok {
				break
			}
			l2 := layer2_load(&l1.l2s[j])

			for k := uint32(0); k < l2Size; k++ {
				count := float64(layer2_loadCounter(l2, k))
				obs := i<<l0Shift | j<<l1Shift | k<<l2Shift | 1<<halfShift
				obs ^= ^uint32(int32(obs)>>31) | (1 << 31)
				value := float64(math.Float32frombits(obs))

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
		return 0, 0, 0
	} else if total == 1 {
		return sum, sum / total, 0
	}
	return sum, sum / total, vari / (total - 1)
}
