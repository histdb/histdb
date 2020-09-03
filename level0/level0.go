package level0

import (
	"encoding/binary"
	"io"

	"github.com/zeebo/errs"
	"github.com/zeebo/lsm"
	"github.com/zeebo/lsm/filesystem"
	"github.com/zeebo/lsm/utils"
)

const (
	l0EntryHeaderSize = 24
	l0DataSize        = 2 << 20
	l0IndexSize       = 256 << 10
	l0BufferSize      = 64 << 10
)

type T struct {
	buf  []byte
	fh   filesystem.File
	len  uint32
	keys keyHeap
	pos  map[lsm.Key]idxBuf
	err  error
	done bool
}

func (t *T) Init(fh filesystem.File) error {
	// TODO: should try to resume
	// TODO: should check done

	if err := fh.Fallocate(l0DataSize + l0IndexSize); err != nil {
		return errs.Wrap(err)
	}

	*t = T{
		fh:   fh,
		buf:  t.buf[:0],
		keys: t.keys[:0],
		pos:  t.pos,
	}

	if t.pos == nil {
		t.pos = make(map[lsm.Key]idxBuf)
	} else {
		// compiles into a runtime map-clear call
		for key := range t.pos {
			delete(t.pos, key)
		}
	}

	return nil
}

func (t *T) File() filesystem.File {
	return t.fh
}

func (t *T) Append(key lsm.Key, value []byte) (bool, error) {
	ok, err := t.append(key, value)
	if err != nil {
		return false, err
	} else if !ok {
		return false, t.finish()
	} else if len(t.buf) >= l0BufferSize {
		return true, t.flush()
	} else {
		return true, nil
	}
}

func (t *T) append(key lsm.Key, value []byte) (bool, error) {
	if t.err != nil {
		return false, t.err
	} else if t.len&^31 != t.len {
		return false, t.storeErr(errs.New("unaligned corrupted length in level0 file"))
	}

	// reserved header
	if t.len == 0 {
		t.buf = append(t.buf, make([]byte, 32)...)
		t.len = 32
	}

	length := l0EntryHeaderSize + uint32(len(value))
	pad := ((length + 31) &^ 31) - length

	if t.len+length > l0DataSize {
		return false, nil
	}

	t.buf = utils.AppendUint32(t.buf, length)
	t.buf = append(t.buf, key[:]...)
	t.buf = append(t.buf, value...)

	// REVISIT: this checks if pad is non-negative for no reason (it's a uint32)
	if pad > 0 {
		t.buf = append(t.buf, make([]byte, pad)...)
	}

	if _, ok := t.pos[key]; !ok {
		t.keys = t.keys.Push(key)
	}

	ibuf := t.pos[key]
	ibuf.Append(uint16(t.len / 32))
	t.pos[key] = ibuf

	t.len += length + pad
	return true, nil
}

func (t *T) storeErr(err error) error {
	t.err = errs.Wrap(err)
	return t.err
}

func (t *T) flush() error {
	if _, err := t.fh.Write(t.buf); err != nil {
		return t.storeErr(err)
	}

	t.buf = t.buf[:0]
	return nil
}

func (t *T) finish() error {
	if len(t.buf) > 0 && t.flush() != nil {
		return t.err
	}

	if _, err := t.fh.Seek(l0DataSize, io.SeekStart); err != nil {
		return t.storeErr(err)
	}

	buf := make([]byte, 0, len(t.keys)*4)
	var key lsm.Key
	for len(t.keys) > 0 {
		t.keys, key = t.keys.Pop()
		ibuf := t.pos[key]
		kp := binary.BigEndian.Uint16(key[0:2])

		buf = utils.AppendUint16(buf, kp)
		buf = utils.AppendUint16(buf, ibuf.x)
		for _, idx := range ibuf.b {
			buf = utils.AppendUint16(buf, kp)
			buf = utils.AppendUint16(buf, idx)
		}
	}

	if _, err := t.fh.Write(buf); err != nil {
		return t.storeErr(err)
	}

	t.done = true
	return nil
}

func (t *T) Iterator() (it Iterator, err error) {
	if t.err != nil {
		err = t.err
	} else if !t.done {
		err = errs.New("iterate on incomplete level0 file")
	} else {
		it.Init(t.fh)
	}
	return it, err
}

//////////////////////////////////////

type idxBuf struct {
	b []uint16
	x uint16
}

func (i *idxBuf) Append(x uint16) {
	if i.x == 0 {
		i.x = x
	} else {
		i.b = append(i.b, x)
	}
}
