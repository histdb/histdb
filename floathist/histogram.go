package floathist

import (
	"math"
	"sync/atomic"
	"unsafe"

	"github.com/zeebo/lsm/floathist/internal/bitmap"
)

const (
	l0Bits = 4
	l1Bits = 4
	l2Bits = 6

	l0Size = 1 << l0Bits
	l1Size = 1 << l1Bits
	l2Size = 1 << l2Bits

	l0Mask = 1<<l0Size - 1
	l1Mask = 1<<l1Size - 1
	l2Mask = 1<<l2Size - 1

	l0Shift   = 32 - l0Bits
	l1Shift   = l0Shift - l1Bits
	l2Shift   = l1Shift - l2Bits
	halfShift = l2Shift - 1
)

type (
	ptr = unsafe.Pointer

	Histogram struct {
		bm  bitmap.B64
		l1s [l0Size]*layer1
	}

	layer1 struct {
		bm  bitmap.B64
		l2s [l1Size]*layer2
	}

	layer2 [l2Size]uint64
)

func loadLayer1(addr **layer1) *layer1 {
	return (*layer1)(atomic.LoadPointer((*ptr)(ptr(addr))))
}

func casLayer1(addr **layer1, b *layer1) bool {
	return atomic.CompareAndSwapPointer((*ptr)(ptr(addr)), nil, ptr(b))
}

func loadLayer2(addr **layer2) *layer2 {
	return (*layer2)(atomic.LoadPointer((*ptr)(ptr(addr))))
}

func casLayer2(addr **layer2, b *layer2) bool {
	return atomic.CompareAndSwapPointer((*ptr)(ptr(addr)), nil, ptr(b))
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

	l1addr := &h.l1s[l1idx]
	l1 := loadLayer1(l1addr)
	if l1 == nil {
		l1 = new(layer1)
		if !casLayer1(l1addr, l1) {
			l1 = loadLayer1(l1addr)
		} else {
			h.bm.SetIdx(l1idx)
		}
	}

	l2addr := &l1.l2s[l2idx]
	l2 := loadLayer2(l2addr)
	if l2 == nil {
		l2 = new(layer2)
		if !casLayer2(l2addr, l2) {
			l2 = loadLayer2(l2addr)
		} else {
			l1.bm.SetIdx(l2idx)
		}
	}

	atomic.AddUint64(&l2[idx], 1)
}

func (h *Histogram) Total() (total int64) {
	bm := h.bm.Clone()
	for {
		i, ok := bm.Next()
		if !ok {
			break
		}
		l1 := loadLayer1(&h.l1s[i])

		bm := l1.bm.Clone()
		for {
			i, ok := bm.Next()
			if !ok {
				break
			}
			l2 := loadLayer2(&l1.l2s[i])

			for i := 0; i < len(l2); i++ {
				total += int64(atomic.LoadUint64(&l2[i]))
			}
		}
	}

	return total
}

func (h *Histogram) Quantile(q float64) float32 {
	target, acc := uint64(q*float64(h.Total())+0.5), uint64(0)

	bm := h.bm.Clone()
	for {
		i, ok := bm.Next()
		if !ok {
			break
		}
		l1 := loadLayer1(&h.l1s[i])

		bm := l1.bm.Clone()
		for {
			j, ok := bm.Next()
			if !ok {
				break
			}
			l2 := loadLayer2(&l1.l2s[j])

			for k := uint32(0); k < uint32(len(l2)); k++ {
				acc += atomic.LoadUint64(&l2[k])
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

	bm := h.bm.Clone()
	for {
		i, ok := bm.Next()
		if !ok {
			break
		}
		l1 := loadLayer1(&h.l1s[i])

		bm := l1.bm.Clone()
		for {
			j, ok := bm.Next()
			if !ok {
				break
			}
			l2 := loadLayer2(&l1.l2s[j])

			target := i<<l0Shift | j<<l1Shift

			for k := uint32(0); k < uint32(len(l2)); k++ {
				count := atomic.LoadUint64(&l2[k])
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
	bm := h.bm.Clone()
	for {
		i, ok := bm.Next()
		if !ok {
			break
		}
		l1 := loadLayer1(&h.l1s[i])

		bm := l1.bm.Clone()
		for {
			j, ok := bm.Next()
			if !ok {
				break
			}
			l2 := loadLayer2(&l1.l2s[j])

			for k := uint32(0); k < uint32(len(l2)); k++ {
				count := float64(atomic.LoadUint64(&l2[k]))
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

	bm := h.bm.Clone()
	for {
		i, ok := bm.Next()
		if !ok {
			break
		}
		l1 := loadLayer1(&h.l1s[i])

		bm := l1.bm.Clone()
		for {
			j, ok := bm.Next()
			if !ok {
				break
			}
			l2 := loadLayer2(&l1.l2s[j])

			for k := uint32(0); k < uint32(len(l2)); k++ {
				count := float64(atomic.LoadUint64(&l2[k]))
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

	bm := h.bm.Clone()
	for {
		i, ok := bm.Next()
		if !ok {
			break
		}
		l1 := loadLayer1(&h.l1s[i])

		bm := l1.bm.Clone()
		for {
			j, ok := bm.Next()
			if !ok {
				break
			}
			l2 := loadLayer2(&l1.l2s[j])

			for k := uint32(0); k < uint32(len(l2)); k++ {
				count := float64(atomic.LoadUint64(&l2[k]))
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
