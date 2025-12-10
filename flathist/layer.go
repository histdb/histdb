package flathist

import (
	"math"
	"unsafe"
)

func lowerValue(i, j, k uint32) float32 {
	obs := i<<l0Shift | j<<l1Shift | k<<l2Shift
	obs ^= ^uint32(int32(obs)>>31) | (1 << 31)
	return math.Float32frombits(obs)
}

func upperValue(i, j, k uint32) float32 {
	obs := i<<l0Shift | j<<l1Shift | k<<l2Shift | 1<<l2halfShift
	obs ^= ^uint32(int32(obs)>>31) | (1 << 31)
	return math.Float32frombits(obs)
}

const (
	l0Bits = 5
	l1Bits = 5
	l2Bits = 6

	l0Size = 1 << l0Bits
	l1Size = 1 << l1Bits
	l2Size = 1 << l2Bits

	l0Mask = 1<<l0Bits - 1
	l1Mask = 1<<l1Bits - 1
	l2Mask = 1<<l2Bits - 1

	l0Shift     = 32 - l0Bits
	l1Shift     = l0Shift - l1Bits
	l2Shift     = l1Shift - l2Bits
	l2halfShift = l2Shift - 1
)

type layer0 struct {
	_  [0]func() // no equality
	l1 [l0Size]uint32
}

type layer1 struct {
	_  [0]func() // no equality
	l2 [l1Size]uint32
}

type layer2Small struct {
	cs [l2Size]uint32
}

type layer2Large struct {
	cs [l2Size]uint64
}

const (
	_ uintptr = (unsafe.Sizeof(layer0{}) - 128) * (128 - unsafe.Sizeof(layer0{}))
	_ uintptr = (unsafe.Sizeof(layer1{}) - 128) * (128 - unsafe.Sizeof(layer1{}))
	_ uintptr = (unsafe.Sizeof(layer2Small{}) - 256) * (256 - unsafe.Sizeof(layer2Small{}))
	_ uintptr = (unsafe.Sizeof(layer2Large{}) - 512) * (512 - unsafe.Sizeof(layer2Large{}))
)

const (
	lAddrMask = 1<<29 - 1

	l2GrowAt = (1 << 32) >> 4 // set when about to overflow a 32 bit value

	l2TagSmall   = 0b100
	l2TagGrowing = 0b110
	l2TagLarge   = 0b111
)

func addrTag(v uint32) uint32   { return v >> 29 }
func isAddrLarge(v uint32) bool { return addrTag(v) == l2TagLarge }
