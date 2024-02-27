package flathist

// sumLayer2SmallFallback sums the histogram buffers using an unrolled loop, using
// 64bit additions to avoid overflows.
func sumLayer2SmallFallback(l2 *layer2Small) (total uint64) {
	for i := 0; i <= l2Size-8; i += 8 {
		total += uint64(l2.cs[i+0]) + uint64(l2.cs[i+1])
		total += uint64(l2.cs[i+2]) + uint64(l2.cs[i+3])
		total += uint64(l2.cs[i+4]) + uint64(l2.cs[i+5])
		total += uint64(l2.cs[i+6]) + uint64(l2.cs[i+7])
	}
	return total
}

// sumLayer2LargeFallback sums the histogram buffers using an unrolled loop.
func sumLayer2LargeFallback(l2 *layer2Large) (total uint64) {
	for i := 0; i <= l2Size-8; i += 8 {
		total += l2.cs[i+0] + l2.cs[i+1]
		total += l2.cs[i+2] + l2.cs[i+3]
		total += l2.cs[i+4] + l2.cs[i+5]
		total += l2.cs[i+6] + l2.cs[i+7]
	}
	return
}
