package level0

type buffer struct {
	data []byte
	cap  int
}

func newBuffer(cap int) buffer {
	return buffer{cap: cap}
}

func (b *buffer) append(data []byte) bool {
	if len(b.data)+len(data) > b.cap {
		return false
	}
	b.data = append(b.data, data...)
	return true
}

func (b *buffer) appendUint16(v uint16) bool {
	return b.append((&[2]byte{
		byte(v >> 0x08), byte(v >> 0x00),
	})[:])
}

func (b *buffer) appendUint32(v uint32) bool {
	return b.append((&[4]byte{
		byte(v >> 0x18), byte(v >> 0x10),
		byte(v >> 0x08), byte(v >> 0x00),
	})[:])
}

func (b *buffer) appendUint64(v uint64) bool {
	return b.append((&[8]byte{
		byte(v >> 0x38), byte(v >> 0x30),
		byte(v >> 0x28), byte(v >> 0x20),
		byte(v >> 0x18), byte(v >> 0x10),
		byte(v >> 0x08), byte(v >> 0x00),
	})[:])
}

func (b *buffer) take() []byte {
	data := b.data
	if rem := b.cap - len(data); rem > 0 {
		data = append(data, make([]byte, rem)...)
	}
	b.data = data[:0]
	return data
}
