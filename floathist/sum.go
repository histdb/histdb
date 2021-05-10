package floathist

import "golang.org/x/sys/cpu"

func sumLayer2(l2 layer2) uint64 {
	fn := sumLayer2Small
	if layer2_isLarge(l2) {
		fn = sumLayer2Large
	}
	return fn(l2)
}

//go:noescape
func sumLayer2SmallAVX2(layer2) uint64

// sumLayer2Small is either backed by AVX2 or a partially unrolled loop.
var sumLayer2Small = map[bool]func(layer2) uint64{
	true:  sumLayer2SmallAVX2,
	false: sumLayer2SmallSlow,
}[l2Size == 64 && cpu.X86.HasAVX2]

// sumLayer2SmallSlow sums the histogram buffers using an unrolled loop.
func sumLayer2SmallSlow(l2 layer2) (total uint64) {
	// we have no worry of overflow because we will upconvert buckets
	// when any individual counter hits 1<<32/4.
	l2s := layer2_asSmall(l2)
	for i := 0; i <= l2Size-8; i += 8 {
		total += uint64(l2s[i] + l2s[i+1])
		total += uint64(l2s[i+2] + l2s[i+3])
		total += uint64(l2s[i+4] + l2s[i+5])
		total += uint64(l2s[i+6] + l2s[i+7])
	}
	return
}

//go:noescape
func sumLayer2LargeAVX2(layer2) uint64

// sumLayer2Large is either backed by AVX2 or a partially unrolled loop.
var sumLayer2Large = map[bool]func(layer2) uint64{
	true:  sumLayer2LargeAVX2,
	false: sumLayer2LargeSlow,
}[l2Size == 64 && cpu.X86.HasAVX2]

// sumLayer2LargeSlow sums the histogram buffers using an unrolled loop.
func sumLayer2LargeSlow(l2 layer2) (total uint64) {
	l2l := layer2_asLarge(l2)
	for i := 0; i <= l2Size-8; i += 8 {
		total += l2l[i] + l2l[i+1]
		total += l2l[i+2] + l2l[i+3]
		total += l2l[i+4] + l2l[i+5]
		total += l2l[i+6] + l2l[i+7]
	}
	return
}
