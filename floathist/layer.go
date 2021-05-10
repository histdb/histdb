package floathist

import (
	"runtime"
	"sync/atomic"
	"unsafe"
)

type (
	uptr = uintptr
	ptr  = unsafe.Pointer
)

const (
	l0Bits = 4
	l1Bits = 4
	l2Bits = 6

	l0Bitmask = 1<<l0Bits - 1
	l1Bitmask = 1<<l1Bits - 1
	l2Bitmask = 1<<l2Bits - 1

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

//
// layer 0
//

type layer0 struct {
	bm  l0Bitmap
	l1s [l0Size]*layer1
}

//
// layer 1
//

type layer1 struct {
	bm  l1Bitmap
	l2s [l1Size]layer2
}

func layer1Load(addr **layer1) *layer1 { return (*layer1)(atomic.LoadPointer((*ptr)(ptr(addr)))) }
func layer1CAS(addr **layer1, b *layer1) bool {
	return atomic.CompareAndSwapPointer((*ptr)(ptr(addr)), nil, ptr(b))
}

//
// layer 2
//

type (
	layer2      = ptr // either layer2Small or layer2Large
	layer2Small [l2Size]uint32
	layer2Large [l2Size]uint64
)

func (l *layer2Large) asLayer2() layer2 { return (layer2)(ptr(uptr(ptr(l))&^0b10 + 0b10)) }
func (l *layer2Small) asLayer2() layer2 { return (layer2)(ptr(uptr(ptr(l))&^0b10 + 0b00)) }

func newLayer2() layer2 { return new(layer2Small).asLayer2() }

const upconvertAt = 1 << 32 / 4

func layer2_load(addr *layer2) layer2           { return atomic.LoadPointer(addr) }
func layer2_store(addr *layer2, l layer2)       { atomic.StorePointer(addr, l) }
func layer2_cas(addr *layer2, o, n layer2) bool { return atomic.CompareAndSwapPointer(addr, o, n) }

func layer2_isSmall(l layer2) bool { return uintptr(l)&0b11 == 0b00 }
func layer2_isLarge(l layer2) bool { return uintptr(l)&0b11 == 0b10 }

func layer2_asLarge(l layer2) *layer2Large  { return (*layer2Large)(ptr(uptr(l) &^ 0b11)) }
func layer2_asSmall(l layer2) *layer2Small  { return (*layer2Small)(ptr(uptr(l) &^ 0b11)) }
func layer2_asUpconverting(l layer2) layer2 { return ptr(uptr(l)&^0b01 + 0b01) }

func layer2_addCounter(l layer2, i, n uint32) bool {
	if layer2_isLarge(l) {
		atomic.AddUint64(&layer2_asLarge(l)[i%l2Size], uint64(n))
		return false
	} else {
		return atomic.AddUint32(&layer2_asSmall(l)[i%l2Size], n) > upconvertAt
	}
}

func layer2_loadCounter(l layer2, i uint32) uint64 {
	if layer2_isLarge(l) {
		return atomic.LoadUint64(&layer2_asLarge(l)[i%l2Size])
	} else {
		return uint64(atomic.LoadUint32(&layer2_asSmall(l)[i%l2Size]))
	}
}

func layer2_unsafeSetCounter(l layer2, i uint32, n uint64) bool {
	if layer2_isLarge(l) {
		layer2_asLarge(l)[i%l2Size] = n
		return true
	} else if n > upconvertAt {
		return false
	} else {
		layer2_asSmall(l)[i%l2Size] = uint32(n)
		return true
	}
}

func layer2_upconvert(l layer2, addr *layer2, finalize bool) bool {
	// if no point in upconverting if we're small.
	if !layer2_isSmall(l) {
		return false
	}

	// tag the bit and claim ownership of doing the upconvert
	if !layer2_cas(addr, l, layer2_asUpconverting(l)) {
		return false
	}

	// create a new large value and clone the small value
	lg := new(layer2Large)
	sme := layer2_asSmall(l)

	if finalize {
		smc := new(layer2Small)

		for i := uint32(0); i < l2Size; i++ {
			v := atomic.LoadUint32(&sme[i])
			lg[i] = uint64(v)
			smc[i] = v
		}

		runtime.SetFinalizer(sme, func(sme *layer2Small) {
			for i := uint32(0); i < l2Size; i++ {
				if d := atomic.LoadUint32(&sme[i]) - smc[i]; d > 0 {
					atomic.AddUint64(&lg[i], uint64(d))
				}
			}
		})
	} else {
		for i := uint32(0); i < l2Size; i++ {
			lg[i] = uint64(atomic.LoadUint32(&sme[i]))
		}
	}

	// store the tagged large value in
	layer2_store(addr, lg.asLayer2())

	return true
}
