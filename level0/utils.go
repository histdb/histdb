package level0

import "github.com/zeebo/xxh3"

func checksum(x []byte) uint32 {
	return uint32(xxh3.Hash(x))
}

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
