package varint

import (
	"encoding/binary"
	"math/bits"

	"github.com/histdb/histdb/buffer"
)

var le = binary.LittleEndian

//
// varint support
//

func Append(dst *[9]byte, val uint64) (nbytes uintptr) {
	nbytes = 575*uintptr(bits.Len64(val))/4096 + 1

	if nbytes < 9 {
		enc := val<<nbytes + 1<<((nbytes-1)&63) - 1
		le.PutUint64(dst[:], enc)
		return
	}

	dst[0] = 0xff
	le.PutUint64(dst[1:], val)
	return
}

func FastConsume(src *[9]byte) (nbytes uintptr, dec uint64) {
	nbytes = uintptr(bits.TrailingZeros8(^src[0])) + 1

	if nbytes < 9 {
		dec = le.Uint64(src[:]) >> nbytes
		dec &= 1<<((8*nbytes-nbytes)&63) - 1
		return
	}

	dec = le.Uint64(src[1:])
	return
}

func Consume(buf buffer.T) (uint64, buffer.T, bool) {
	rem := buf.Remaining()
	if rem == 0 {
		return 0, buf, false
	} else if rem >= 9 {
		nbytes, out := FastConsume(buf.Front9())
		return out, buf.Advance(nbytes), true
	}

	// slow path: can't create or use any pointers past the end of the buf
	out := uint64(*buf.Front())
	nbytes := uint8(bits.TrailingZeros8(^uint8(out)) + 1)
	out >>= nbytes

	if uintptr(nbytes) > rem {
		return 0, buf, false
	}

	switch nbytes {
	case 9:
		out |= le.Uint64(buf.Index8(1)[:])
	case 8:
		out |= uint64(le.Uint32(buf.Index4(1)[:]))
		out |= uint64(le.Uint32(buf.Index4(4)[:])) << 24
	case 7:
		out |= uint64(le.Uint32(buf.Index4(1)[:])) << 1
		out |= uint64(le.Uint16(buf.Index2(5)[:])) << 33
	case 6:
		out |= uint64(le.Uint32(buf.Index4(1)[:])) << 2
		out |= uint64(*buf.Index(5)) << 34
	case 5:
		out |= uint64(le.Uint32(buf.Index4(1)[:])) << 3
	case 4:
		out |= uint64(le.Uint16(buf.Index2(1)[:])) << 4
		out |= uint64(*buf.Index(3)) << 20
	case 3:
		out |= uint64(le.Uint16(buf.Index2(1)[:])) << 5
	case 2:
		out |= uint64(*buf.Index(1)) << 6
	}

	return out, buf.Advance(uintptr(nbytes)), true
}
