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
	l0B = 4
	l1B = 4
	l2B = 6

	l0BM = 1<<l0B - 1
	l1BM = 1<<l1B - 1
	l2BM = 1<<l2B - 1

	l0S = 1 << l0B
	l1S = 1 << l1B
	l2S = 1 << l2B

	l0SM = 1<<l0S - 1
	l1SM = 1<<l1S - 1
	l2SM = 1<<l2S - 1

	l0Sh   = 32 - l0B
	l1Sh   = l0Sh - l1B
	l2Sh   = l1Sh - l2B
	halfSh = l2Sh - 1
)

//
// layer 0
//

type layer0 struct {
	_ [0]func() // no equality

	bm  l0Bitmap
	l1s [l0S]*layer1
}

//
// layer 1
//

type layer1 struct {
	_ [0]func() // no equality

	bm  l1Bitmap
	l2s [l1S]layer2
}

func layer1_load(addr **layer1) *layer1 { return (*layer1)(atomic.LoadPointer((*ptr)(ptr(addr)))) }
func layer1_cas(addr **layer1, b *layer1) bool {
	return atomic.CompareAndSwapPointer((*ptr)(ptr(addr)), nil, ptr(b))
}

//
// layer 2
//

type (
	layer2      = ptr // either layer2Small or layer2Large
	layer2Small [l2S]uint32
	layer2Large [l2S]uint64
)

func layer2_reset(l layer2) {
	if layer2_isLarge(l) {
		*layer2_asLarge(l) = layer2Large{}
	} else {
		*layer2_asSmall(l) = layer2Small{}
	}
}

func (l *layer2Small) asLayer2() layer2 { return (layer2)(ptr(uptr(ptr(l))&^0b10 + 0b00)) }
func (l *layer2Large) asLayer2() layer2 { return (layer2)(ptr(uptr(ptr(l))&^0b10 + 0b11)) }

func newLayer2() layer2        { return new(layer2Small).asLayer2() }
func newLayer2_small() layer2  { return new(layer2Small).asLayer2() }
func newLayer2_marked() layer2 { return layer2_asMarked(new(layer2Small).asLayer2()) }
func newLayer2_large() layer2  { return new(layer2Large).asLayer2() }

const (
	// since i already forgot this once: before the mark state is set, we
	// can use faster avx2 code that doesn't have to handle the 64 counters
	// overflowing. it only exists to enable that fast path.

	growAt = 1 << 32 / 4   // set when about to overflow a 32 bit value
	markAt = 1 << 32 / 128 // set when 64 additions may overflow a 32 bit value

	tagLayer2Small   = 0b00
	tagLayer2Marked  = 0b01
	tagLayer2Growing = 0b10
	tagLayer2Large   = 0b11
)

func layer2_load(addr *layer2) layer2           { return atomic.LoadPointer(addr) }
func layer2_store(addr *layer2, l layer2)       { atomic.StorePointer(addr, l) }
func layer2_cas(addr *layer2, o, n layer2) bool { return atomic.CompareAndSwapPointer(addr, o, n) }

func layer2_tag(l layer2) uptr { return uptr(l) & 0b11 }

func layer2_asSmall(l layer2) *layer2Small { return (*layer2Small)(ptr(uptr(l) &^ 0b11)) }
func layer2_asMarked(l layer2) layer2      { return ptr(uptr(l)&^0b11 + 0b01) }
func layer2_asGrowing(l layer2) layer2     { return ptr(uptr(l)&^0b11 + 0b10) }
func layer2_asLarge(l layer2) *layer2Large { return (*layer2Large)(ptr(uptr(l) &^ 0b11)) }
func layer2_truncate(l layer2) layer2      { return ptr(uptr(l) &^ 0b11) }

func layer2_isLarge(l layer2) bool { return uptr(l)&0b11 == 0b11 }
func layer2_canMark(l layer2) bool { return uptr(l)&0b11 == 0b00 }
func layer2_canGrow(l layer2) bool { return uptr(l)&0b10 == 0b00 }

func layer2_loadCounter(l layer2, i uint32) uint64 {
	if layer2_isLarge(l) {
		return atomic.LoadUint64(&layer2_asLarge(l)[i%l2S])
	} else {
		return uint64(atomic.LoadUint32(&layer2_asSmall(l)[i%l2S]))
	}
}

func layer2_unsafeSetCounter(l layer2, addr *layer2, i uint32, n uint64) bool {
	i %= l2S

	if n > growAt && layer2_canGrow(l) {
		if !layer2_grow(l, addr, false) {
			return false
		}
		l = *addr
	} else if n > markAt && layer2_canMark(l) {
		if !layer2_mark(l, addr) {
			return false
		}
		l = *addr
	}

	if layer2_isLarge(l) {
		layer2_asLarge(l)[i] = n
	} else {
		layer2_asSmall(l)[i] = uint32(n)
	}

	return true
}

func layer2_mark(l layer2, addr *layer2) bool {
	return layer2_cas(addr, l, layer2_asMarked(l))
}

func layer2_grow(l layer2, addr *layer2, finalize bool) bool {
	// only try to do so if we're small and not already growing
	if !layer2_canGrow(l) {
		return false
	}

	// tag the bit and claim ownership of doing the growth
	if !layer2_cas(addr, l, layer2_asGrowing(l)) {
		return false
	}

	// create a new large value and clone the small value
	lg := new(layer2Large)
	sme := layer2_asSmall(l)

	if finalize {
		smc := new(layer2Small)

		for i := uint32(0); i < l2S; i++ {
			v := atomic.LoadUint32(&sme[i])
			lg[i] = uint64(v)
			smc[i] = v
		}

		runtime.SetFinalizer(sme, func(sme *layer2Small) {
			for i := uint32(0); i < l2S; i++ {
				if d := atomic.LoadUint32(&sme[i]) - smc[i]; d > 0 {
					atomic.AddUint64(&lg[i], uint64(d))
				}
			}
		})
	} else {
		for i := uint32(0); i < l2S; i++ {
			lg[i] = uint64(atomic.LoadUint32(&sme[i]))
		}
	}

	// store the tagged large value in
	layer2_store(addr, lg.asLayer2())

	return true
}
