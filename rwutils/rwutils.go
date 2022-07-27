package rwutils

import (
	"encoding/binary"
	"io"

	"github.com/zeebo/errs/v2"

	"github.com/histdb/histdb/buffer"
)

var le = binary.LittleEndian

type RW interface {
	AppendTo(w *W)
	ReadFrom(r *R)
}

type W struct {
	buf []byte
	err error
	w   io.Writer
}

func (w *W) Init(wr io.Writer, buf []byte) {
	*w = W{
		buf: buf[:0],
		w:   wr,
	}
}

func (w *W) Done() error {
	w.flush()
	return w.err
}

func (w *W) Uint64(x uint64) {
	if len(w.buf)+8 > cap(w.buf) {
		w.flush()
	}
	w.buf = append(w.buf,
		byte(x), byte(x>>8), byte(x>>16), byte(x>>24),
		byte(x>>32), byte(x>>40), byte(x>>48), byte(x>>56),
	)
}

func (w *W) Uint32(x uint32) {
	if len(w.buf)+4 > cap(w.buf) {
		w.flush()
	}
	w.buf = append(w.buf, byte(x), byte(x>>8), byte(x>>16), byte(x>>24))
}

func (w *W) Bytes(buf []byte) {
	if len(w.buf)+len(buf) > cap(w.buf) {
		w.flush()
		if len(buf) > cap(w.buf) {
			if w.err == nil {
				_, w.err = w.w.Write(buf)
			}
			return
		}
	}
	w.buf = append(w.buf, buf...)
}

//go:noinline
func (w *W) flush() {
	if w.err == nil {
		_, w.err = w.w.Write(w.buf)
	}
	w.buf = w.buf[:0]
}

type R struct {
	buf buffer.T
	err error
}

func (r *R) Init(buf []byte) {
	*r = R{
		buf: buffer.OfLen(buf),
	}
}

func (r *R) Done() ([]byte, error) {
	return r.buf.Suffix(), r.err
}

func (r *R) Uint64() (x uint64) {
	if r.err == nil {
		if r.buf.Remaining() >= 8 {
			x = le.Uint64(r.buf.Front8()[:])
			r.buf = r.buf.Advance(8)
		} else {
			r.bad(8)
		}
	}
	return
}

func (r *R) Uint32() (x uint32) {
	if r.err == nil {
		if r.buf.Remaining() >= 4 {
			x = le.Uint32(r.buf.Front4()[:])
			r.buf = r.buf.Advance(4)
		} else {
			r.bad(4)
		}
	}
	return
}

func (r *R) Bytes(n int) (x []byte) {
	if r.err == nil {
		if r.buf.Remaining() >= uintptr(n) {
			x = r.buf.FrontN(n)
			r.buf = r.buf.Advance(uintptr(n))
		} else {
			r.bad(n)
		}
	}
	return
}

func (r *R) bad(n int) {
	r.err = errs.Errorf("short buffer: needed %d bytes", n)
	r.buf = r.buf.Advance(r.buf.Remaining())
}
