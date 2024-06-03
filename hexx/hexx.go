package hexx

func Put32(x uint32) (v uint64) {
	v = uint64(uint16(x)) | uint64(x)<<16
	v = (v & 0x000000FF000000FF) | ((v & 0x0000FF000000FF00) << 8)
	v = (v & 0x000F000F000F000F) | ((v & 0x00F000F000F000F0) << 4)
	return v + 0x3030303030303030 + 7*((v+0x0606060606060606)>>4&0x0101010101010101)
}

func Get32(x uint64) (v uint32) {
	x = 9*(x&0x4040404040404040>>6) + (x & 0x0f0f0f0f0f0f0f0f)
	x = (x | x>>4) & 0x00FF00FF00FF00FF
	x = (x | x>>8) & 0x0000FFFF0000FFFF
	return uint32(x | x>>16)
}

func Put16(x uint16) (v uint32) {
	v = uint32(uint8(x)) | uint32(x)<<8
	v = (v & 0x000F000F) | ((v & 0x00F000F0) << 4)
	return v + 0x30303030 + 7*((v+0x06060606)>>4&0x01010101)
}

func Get16(x uint32) (v uint16) {
	x = 9*(x&0x40404040>>6) + (x & 0x0f0f0f0f)
	x = (x | x>>4) & 0x00FF00FF
	return uint16(x | x>>8)
}

func Put8(x uint8) (v uint16) {
	v = uint16(x)
	v = (v & 0x000F) | ((v & 0x00F0) << 4)
	return v + 0x3030 + 7*((v+0x0606)>>4&0x0101)
}

func Get8(x uint16) (v uint8) {
	x = 9*(x&0x4040>>6) + (x & 0x0f0f)
	return uint8(x | x>>4)
}
