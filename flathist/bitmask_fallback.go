package flathist

func bitmaskFallback(data *[32]uint32) (m uint32) {
	for i, v := range data {
		m |= (v >> 31) << i
	}
	return
}
