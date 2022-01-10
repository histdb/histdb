package level0

import (
	"encoding/binary"
	"io"

	"github.com/zeebo/errs/v2"

	"github.com/histdb/histdb"
	"github.com/histdb/histdb/filesystem"
)

const (
	l0EntryHeaderSize    = 28
	l0EntryAlignment     = 32
	l0EntryAlignmentMask = l0EntryAlignment - 1
	l0ChecksumSize       = 4
	l0DataSize           = 2 << 20
	l0IndexSize          = 256 << 10
	l0BufferSize         = 64 << 10

	L0Size = l0DataSize + l0IndexSize
)

type T struct {
	buf  []byte
	fh   filesystem.Handle
	len  uint32
	keys keyHeap
	pos  map[histdb.Key]idxBuf
	err  error
	done bool
	ro   bool // readonly
}

func (t *T) reset(fh filesystem.Handle) error {
	if _, err := fh.Seek(0, io.SeekStart); err != nil {
		return errs.Wrap(err)
	}

	*t = T{
		fh:   fh,
		buf:  t.buf[:0],
		keys: t.keys[:0],
		pos:  t.pos,
	}

	if t.pos == nil {
		t.pos = make(map[histdb.Key]idxBuf)
	} else {
		// compiles into a runtime map-clear call
		for key := range t.pos {
			delete(t.pos, key)
		}
	}

	return nil
}

func (t *T) InitFinished(fh filesystem.Handle) error {
	if err := t.reset(fh); err != nil {
		return errs.Wrap(err)
	}

	t.ro = true
	t.done = true

	return nil
}

func (t *T) InitNew(fh filesystem.Handle) error {
	if err := fh.Fallocate(l0DataSize); err != nil {
		return errs.Wrap(err)
	}

	if err := t.reset(fh); err != nil {
		return errs.Wrap(err)
	}

	return nil
}

func (t *T) InitCurrent(fh filesystem.Handle) error {
	if err := t.reset(fh); err != nil {
		return errs.Wrap(err)
	}

	t.ro = true

	var it Iterator
	it.Init(fh)

	offset := int64(l0EntryAlignment)
	for offset < l0DataSize {
		it.readEntryHeader(offset)
		if it.err != nil {
			return errs.Wrap(it.err)
		} else if it.Key().Zero() {
			break
		}

		offset += it.readNameAndValue(offset)
		if it.err != nil {
			return errs.Wrap(it.err)
		}

		ok, err := t.Append(it.Key(), it.Name(), it.Value())
		if !ok {
			return errs.Errorf("unable to reopen L0 file from Append failure")
		} else if err != nil {
			return errs.Wrap(it.err)
		}
	}

	t.ro = false
	return nil
}

func (t *T) File() filesystem.Handle {
	return t.fh
}

func (t *T) Append(key histdb.Key, name, value []byte) (bool, error) {
	ok, err := t.append(key, name, value)
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

func (t *T) append(key histdb.Key, name, value []byte) (bool, error) {
	if t.err != nil {
		return false, t.err
	} else if t.len&^l0EntryAlignmentMask != t.len {
		return false, t.storeErr(errs.Errorf("unaligned corrupted length in level0 file"))
	} else if t.done {
		return false, errs.Errorf("attempt to append to done l0 file")
	} else if key.Zero() {
		return false, errs.Errorf("cannot append zero key")
	}

	// reserved header
	if t.len == 0 {
		t.buf = append(t.buf, make([]byte, l0EntryAlignment)...)
		t.len = l0EntryAlignment
	}

	length := l0EntryHeaderSize + uint32(len(name)) + uint32(len(value)) + l0ChecksumSize
	padded := (length + l0EntryAlignmentMask) &^ l0EntryAlignmentMask

	if t.len+padded > l0DataSize {
		return false, nil
	}

	start := len(t.buf)
	t.buf = appendUint32(t.buf, uint32(len(name)))
	t.buf = appendUint32(t.buf, uint32(len(value)))
	t.buf = append(t.buf, key[:]...)
	t.buf = append(t.buf, name...)
	t.buf = append(t.buf, value...)
	t.buf = appendUint32(t.buf, checksum(t.buf[start:]))
	t.buf = append(t.buf, make([]byte, padded-length)...)

	ibuf, ok := t.pos[key]
	if !ok {
		t.keys = t.keys.Push(key)
	}

	ibuf.Append(uint16(t.len / l0EntryAlignment))
	t.pos[key] = ibuf

	t.len += padded
	return true, nil
}

func (t *T) storeErr(err error) error {
	t.err = errs.Wrap(err)
	return t.err
}

func (t *T) flush() error {
	if !t.ro {
		if _, err := t.fh.Write(t.buf); err != nil {
			return t.storeErr(err)
		}
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

	buf := t.buf[:0]
	if cap(buf) < 4*len(t.keys)+4 {
		buf = make([]byte, 0, 4*len(t.keys)+4)
	}

	var key histdb.Key
	for len(t.keys) > 0 {
		t.keys, key = t.keys.Pop()
		ibuf := t.pos[key]
		kp := binary.BigEndian.Uint16(key[0:2])

		buf = appendUint16(buf, kp)
		buf = appendUint16(buf, ibuf.x)
		for _, idx := range ibuf.b {
			buf = appendUint16(buf, kp)
			buf = appendUint16(buf, idx)
		}
	}

	buf = appendUint32(buf, checksum(buf))

	if !t.ro {
		if _, err := t.fh.Write(buf); err != nil {
			return t.storeErr(err)
		}
	}

	t.done = true
	return nil
}

func (t *T) InitIterator(it *Iterator) (err error) {
	if t.err != nil {
		err = t.err
	} else if !t.done {
		err = errs.Errorf("iterate on incomplete level0 file")
	} else {
		it.Init(t.fh)
	}
	return err
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
