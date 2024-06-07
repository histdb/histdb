package rwutils

import (
	"encoding/binary"

	"github.com/zeebo/errs/v2"

	"github.com/histdb/histdb/buffer"
	"github.com/histdb/histdb/varint"
)

var le = binary.LittleEndian

type Bytes interface {
	~[0x00]byte | ~[0x01]byte | ~[0x02]byte | ~[0x03]byte |
		~[0x04]byte | ~[0x05]byte | ~[0x06]byte | ~[0x07]byte |
		~[0x08]byte | ~[0x09]byte | ~[0x0a]byte | ~[0x0b]byte |
		~[0x0c]byte | ~[0x0d]byte | ~[0x0e]byte | ~[0x0f]byte |
		~[0x10]byte | ~[0x11]byte | ~[0x12]byte | ~[0x13]byte |
		~[0x14]byte | ~[0x15]byte | ~[0x16]byte | ~[0x17]byte |
		~[0x18]byte | ~[0x19]byte | ~[0x1a]byte | ~[0x1b]byte |
		~[0x1c]byte | ~[0x1d]byte | ~[0x1e]byte | ~[0x1f]byte
}

type RW[T any] interface {
	*T

	AppendTo(w *W)
	ReadFrom(r *R)
}

type W struct {
	_ [0]func() // no equality

	buf buffer.T
}

func (w *W) Init(buf buffer.T) {
	*w = W{buf: buf}
}

func (w *W) Done() buffer.T {
	return w.buf
}

func (w *W) Varint(x uint64) {
	w.buf = w.buf.Grow9()
	n := varint.Append(w.buf.Front9(), x)
	w.buf = w.buf.Advance(n)
}

func (w *W) StageUint64(n uintptr) *[8]byte {
	pos := w.buf.Pos()
	w.buf = w.buf.Grow(n + 8).Advance(8)
	return w.buf.Index8(pos)
}

func (w *W) Bytes4(x [4]byte) {
	w.buf = w.buf.Grow(4)
	*w.buf.Front4() = x
	w.buf = w.buf.Advance(4)
}

func (w *W) Bytes8(x [8]byte) {
	w.buf = w.buf.Grow(8)
	*w.buf.Front8() = x
	w.buf = w.buf.Advance(8)
}

func (w *W) Bytes10(x [10]byte) {
	w.buf = w.buf.Grow(10)
	*w.buf.Front10() = x
	w.buf = w.buf.Advance(10)
}

func (w *W) Bytes12(x [12]byte) {
	w.buf = w.buf.Grow(12)
	*w.buf.Front12() = x
	w.buf = w.buf.Advance(12)
}

func (w *W) Bytes16(x [16]byte) {
	w.buf = w.buf.Grow(16)
	*w.buf.Front16() = x
	w.buf = w.buf.Advance(16)
}

func (w *W) Bytes18(x [18]byte) {
	w.buf = w.buf.Grow(18)
	*w.buf.Front18() = x
	w.buf = w.buf.Advance(18)
}

func (w *W) Bytes20(x [20]byte) {
	w.buf = w.buf.Grow(20)
	*w.buf.Front20() = x
	w.buf = w.buf.Advance(20)
}

func (w *W) Bytes24(x [24]byte) {
	w.buf = w.buf.Grow(24)
	*w.buf.Front24() = x
	w.buf = w.buf.Advance(24)
}

func (w *W) Uint64(x uint64) {
	w.buf = w.buf.Grow(8)
	le.PutUint64(w.buf.Front8()[:], x)
	w.buf = w.buf.Advance(8)
}

func (w *W) Uint32(x uint32) {
	w.buf = w.buf.Grow(4)
	le.PutUint32(w.buf.Front4()[:], x)
	w.buf = w.buf.Advance(4)
}

func (w *W) Uint16(x uint16) {
	w.buf = w.buf.Grow(2)
	le.PutUint16(w.buf.Front2()[:], x)
	w.buf = w.buf.Advance(2)
}

func (w *W) Uint8(x uint8) {
	w.buf = w.buf.Grow(1)
	*w.buf.Front() = x
	w.buf = w.buf.Advance(1)
}

func (w *W) Bytes(buf []byte) {
	w.buf = w.buf.Grow(uintptr(len(buf)))
	copy(w.buf.Suffix(), buf)
	w.buf = w.buf.Advance(uintptr(len(buf)))
}

type R struct {
	_ [0]func() // no equality

	buf buffer.T
	err error
}

func (r *R) Init(buf buffer.T) {
	*r = R{buf: buf}
}

func (r *R) Done() (buffer.T, error) {
	return r.buf, errs.Wrap(r.err)
}

func (r *R) Remaining() uintptr {
	return r.buf.Remaining()
}

func (r *R) Varint() (x uint64) {
	var ok bool
	x, r.buf, ok = varint.Consume(r.buf)
	if !ok {
		r.Invalid(errs.Errorf("short buffer: varint truncated"))
	}
	return
}

func (r *R) Uint64() (x uint64) {
	if r.buf.Remaining() >= 8 {
		x = le.Uint64(r.buf.Front8()[:])
		r.buf = r.buf.Advance(8)
	} else {
		r.Invalid(errs.Errorf("short buffer: needed 8 bytes"))
	}
	return
}

func (r *R) Uint32() (x uint32) {
	if r.buf.Remaining() >= 4 {
		x = le.Uint32(r.buf.Front4()[:])
		r.buf = r.buf.Advance(4)
	} else {
		r.Invalid(errs.Errorf("short buffer: needed 4 bytes"))
	}
	return
}

func (r *R) Uint16() (x uint16) {
	if r.buf.Remaining() >= 2 {
		x = le.Uint16(r.buf.Front2()[:])
		r.buf = r.buf.Advance(2)
	} else {
		r.Invalid(errs.Errorf("short buffer: needed 2 bytes"))
	}
	return
}

func (r *R) Uint8() (x uint8) {
	if r.buf.Remaining() >= 1 {
		x = *r.buf.Front()
		r.buf = r.buf.Advance(1)
	} else {
		r.Invalid(errs.Errorf("short buffer: needed 1 byte"))
	}
	return
}

func (r *R) Bytes4() (x [4]byte) {
	if r.buf.Remaining() >= 4 {
		x = *r.buf.Front4()
		r.buf = r.buf.Advance(4)
	} else {
		r.Invalid(errs.Errorf("short buffer: needed 4 bytes"))
	}
	return
}

func (r *R) Bytes8() (x [8]byte) {
	if r.buf.Remaining() >= 8 {
		x = *r.buf.Front8()
		r.buf = r.buf.Advance(8)
	} else {
		r.Invalid(errs.Errorf("short buffer: needed 8 bytes"))
	}
	return
}

func (r *R) Bytes10() (x [10]byte) {
	if r.buf.Remaining() >= 10 {
		x = *r.buf.Front10()
		r.buf = r.buf.Advance(10)
	} else {
		r.Invalid(errs.Errorf("short buffer: needed 10 bytes"))
	}
	return
}

func (r *R) Bytes12() (x [12]byte) {
	if r.buf.Remaining() >= 12 {
		x = *r.buf.Front12()
		r.buf = r.buf.Advance(12)
	} else {
		r.Invalid(errs.Errorf("short buffer: needed 12 bytes"))
	}
	return
}

func (r *R) Bytes16() (x [16]byte) {
	if r.buf.Remaining() >= 16 {
		x = *r.buf.Front16()
		r.buf = r.buf.Advance(16)
	} else {
		r.Invalid(errs.Errorf("short buffer: needed 16 bytes"))
	}
	return
}

func (r *R) Bytes18() (x [18]byte) {
	if r.buf.Remaining() >= 18 {
		x = *r.buf.Front18()
		r.buf = r.buf.Advance(18)
	} else {
		r.Invalid(errs.Errorf("short buffer: needed 18 bytes"))
	}
	return
}

func (r *R) Bytes20() (x [20]byte) {
	if r.buf.Remaining() >= 20 {
		x = *r.buf.Front20()
		r.buf = r.buf.Advance(20)
	} else {
		r.Invalid(errs.Errorf("short buffer: needed 20 bytes"))
	}
	return
}

func (r *R) Bytes24() (x [24]byte) {
	if r.buf.Remaining() >= 24 {
		x = *r.buf.Front24()
		r.buf = r.buf.Advance(24)
	} else {
		r.Invalid(errs.Errorf("short buffer: needed 20 bytes"))
	}
	return
}

func (r *R) Bytes(n int) (x []byte) {
	if r.buf.Remaining() >= uintptr(n) {
		x = r.buf.FrontN(n)
		r.buf = r.buf.Advance(uintptr(n))
	} else {
		r.Invalid(errs.Errorf("short buffer: needed %d bytes", n))
	}
	return
}

func (r *R) Invalid(err error) {
	if r.err == nil {
		r.err = errs.Wrap(err)
		r.buf = buffer.T{}
	}
}
