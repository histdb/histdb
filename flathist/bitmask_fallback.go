package flathist

func bitmaskFallback(data *[16]uint32) uint32 {
	var mask uint32
	for i, v := range data {
		mask |= (v >> 31) << uint(i)
	}
	return mask
}
