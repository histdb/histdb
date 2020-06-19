package utils

type Buffer struct {
	data []byte
	cap  int
}

func NewBuffer(cap int) Buffer {
	return Buffer{cap: cap}
}

func (b *Buffer) Append(data []byte) bool {
	if len(b.data)+len(data) > b.cap {
		return false
	}
	b.data = append(b.data, data...)
	return true
}

func (b *Buffer) AppendUint16(v uint16) bool {
	return b.Append((&[2]byte{
		byte(v >> 0x08), byte(v >> 0x00),
	})[:])
}

func (b *Buffer) AppendUint32(v uint32) bool {
	return b.Append((&[4]byte{
		byte(v >> 0x18), byte(v >> 0x10),
		byte(v >> 0x08), byte(v >> 0x00),
	})[:])
}

func (b *Buffer) AppendUint64(v uint64) bool {
	return b.Append((&[8]byte{
		byte(v >> 0x38), byte(v >> 0x30),
		byte(v >> 0x28), byte(v >> 0x20),
		byte(v >> 0x18), byte(v >> 0x10),
		byte(v >> 0x08), byte(v >> 0x00),
	})[:])
}

func (b *Buffer) Take() []byte {
	data := b.data
	if rem := b.cap - len(data); rem > 0 {
		data = append(data, make([]byte, rem)...)
	}
	b.data = data[:0]
	return data
}
