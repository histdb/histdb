package floathist

import "golang.org/x/sys/cpu"

var sumFuncs = map[bool][4]func(layer2) uint64{
	true: {
		0b00: sumLayer2SmallAVX2_32,
		0b01: sumLayer2SmallAVX2,
		0b10: sumLayer2SmallAVX2,
		0b11: sumLayer2LargeAVX2,
	},
	false: {
		0b00: sumLayer2SmallSlow,
		0b01: sumLayer2SmallSlow,
		0b10: sumLayer2SmallSlow,
		0b11: sumLayer2LargeSlow,
	},
}[l2S == 64 && cpu.X86.HasAVX2]

func sumLayer2(l2 layer2) uint64 {
	return sumFuncs[layer2_tag(l2)](layer2_truncate(l2))
}

// Used to sum a layer2Small when the total addition is guaranteed
// to not overflow a 32 bit value.
//
//go:noescape
func sumLayer2SmallAVX2_32(layer2) uint64

// Used to sum a layer2Small with no assumptions about the size
// of the individual counters contained.
//
//go:noescape
func sumLayer2SmallAVX2(layer2) uint64

// sumLayer2SmallSlow sums the histogram buffers using an unrolled loop.
func sumLayer2SmallSlow(l2 layer2) (total uint64) {
	// we have no worry of overflow because we will grow buckets when
	// any individual counter would be large enough to overflow the additions.
	l2s := layer2_asSmall(l2)
	for i := 0; i <= l2S-8; i += 8 {
		total += uint64(l2s[i+0] + l2s[i+1])
		total += uint64(l2s[i+2] + l2s[i+3])
		total += uint64(l2s[i+4] + l2s[i+5])
		total += uint64(l2s[i+6] + l2s[i+7])
	}
	return
}

//go:noescape
func sumLayer2LargeAVX2(layer2) uint64

// sumLayer2LargeSlow sums the histogram buffers using an unrolled loop.
func sumLayer2LargeSlow(l2 layer2) (total uint64) {
	l2l := layer2_asLarge(l2)
	for i := 0; i <= l2S-8; i += 8 {
		total += l2l[i+0] + l2l[i+1]
		total += l2l[i+2] + l2l[i+3]
		total += l2l[i+4] + l2l[i+5]
		total += l2l[i+6] + l2l[i+7]
	}
	return
}
