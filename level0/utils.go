package level0

import "hash/crc32"

var castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

func checksum(x []byte) uint32 {
	return crc32.Checksum(x, castagnoliTable)
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
