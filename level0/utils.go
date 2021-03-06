package level0

func appendUint16(x []byte, v uint16) []byte {
	return append(x,
		byte(v>>0x08), byte(v>>0x00),
	)
}

func appendUint32(x []byte, v uint32) []byte {
	return append(x,
		byte(v>>0x18), byte(v>>0x10),
		byte(v>>0x08), byte(v>>0x00),
	)
}

func appendUint64(x []byte, v uint64) []byte {
	return append(x,
		byte(v>>0x38), byte(v>>0x30),
		byte(v>>0x28), byte(v>>0x20),
		byte(v>>0x18), byte(v>>0x10),
		byte(v>>0x08), byte(v>>0x00),
	)
}
