package floathist

import "github.com/klauspost/cpuid/v2"

var hasAVX2 = cpuid.CPU.Has(cpuid.AVX2)

var sumFuncs = map[bool][4]func(layer2) uint64{
	true: {
		tagLayer2Small:   sumLayer2SmallAVX2,
		tagLayer2Marked:  sumLayer2SmallAVX2,
		tagLayer2Growing: sumLayer2SmallAVX2,
		tagLayer2Large:   sumLayer2LargeAVX2,
	},
	false: {
		tagLayer2Small:   sumLayer2SmallFallback,
		tagLayer2Marked:  sumLayer2SmallFallback,
		tagLayer2Growing: sumLayer2SmallFallback,
		tagLayer2Large:   sumLayer2LargeFallback,
	},
}[l2S == 64 && hasAVX2]

func sumLayer2(l2 layer2) uint64 {
	return sumFuncs[layer2_tag(l2)](layer2_truncate(l2))
}

// sumLayer2SmallFallback sums the histogram buffers using an unrolled loop, using
// 64bit additions to avoid overflows.
func sumLayer2SmallFallback(l2 layer2) (total uint64) {
	l2s := layer2_asSmall(l2)
	for i := 0; i <= l2S-8; i += 8 {
		total += uint64(l2s[i+0]) + uint64(l2s[i+1])
		total += uint64(l2s[i+2]) + uint64(l2s[i+3])
		total += uint64(l2s[i+4]) + uint64(l2s[i+5])
		total += uint64(l2s[i+6]) + uint64(l2s[i+7])
	}
	return total
}

// sumLayer2LargeFallback sums the histogram buffers using an unrolled loop.
func sumLayer2LargeFallback(l2 layer2) (total uint64) {
	l2l := layer2_asLarge(l2)
	for i := 0; i <= l2S-8; i += 8 {
		total += l2l[i+0] + l2l[i+1]
		total += l2l[i+2] + l2l[i+3]
		total += l2l[i+4] + l2l[i+5]
		total += l2l[i+6] + l2l[i+7]
	}
	return
}
