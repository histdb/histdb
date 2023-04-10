package level0

import (
	"encoding/binary"
	"io"
	"sort"
	"sync"

	"github.com/zeebo/errs/v2"

	"github.com/histdb/histdb"
	"github.com/histdb/histdb/filesystem"
)

const (
	l0EntryHeaderSize    = 28
	l0EntryAlignment     = 32
	l0EntryAlignmentMask = l0EntryAlignment - 1
	l0ChecksumSize       = 4
	l0BufferSize         = 64 << 10

	L0DataSize  = 2 << 20
	L0IndexSize = 256 << 10
	L0Size      = L0DataSize + L0IndexSize
)

type keyPos struct {
	key histdb.Key
	pos uint16
}

type keyPoss []keyPos

func (k keyPoss) Len() int               { return len(k) }
func (k keyPoss) Less(i int, j int) bool { return string(k[i].key[:]) < string(k[j].key[:]) }
func (k keyPoss) Swap(i int, j int)      { k[i], k[j] = k[j], k[i] }

type T struct {
	buf   []byte
	fh    filesystem.Handle
	len   uint32
	wrote int64
	keys  keyPoss
	err   error
	done  bool
	ro    bool // readonly
}

func (t *T) reset(fh filesystem.Handle) {
	*t = T{
		fh:   fh,
		buf:  t.buf[:0],
		keys: t.keys[:0],
	}
}

func (t *T) InitFinished(fh filesystem.Handle) {
	t.reset(fh)
	t.ro = true
	t.done = true
}

var ebufPool = sync.Pool{New: func() any { return new([1024]byte) }}

func (t *T) Init(fh filesystem.Handle, cb func(key histdb.Key, name, value []byte)) (err error) {
	t.reset(fh)
	t.ro = true

	var it Iterator
	it.Init(fh)

	// since we control the lifetime of the iterator we can use a pool of
	// fixed size buffers for the common allocation that happens.
	ebuf, _ := ebufPool.Get().(*[1024]byte)
	defer ebufPool.Put(ebuf)
	it.ebuf = ebuf[:0]

	for offset := int64(l0EntryAlignment); offset < L0DataSize; {
		ok := it.readEntryHeader(offset)
		if it.err != nil {
			return it.err
		} else if !ok {
			break
		}

		doffset := it.readNameAndValue(offset)
		if it.err != nil || doffset == 0 {
			break
		}
		offset += doffset

		ok, err := t.Append(it.Key(), it.Name(), it.Value())
		if !ok {
			return errs.Errorf("unable to reopen L0 file from Append failure")
		} else if err != nil {
			return errs.Wrap(it.err)
		}
		if cb != nil {
			cb(it.Key(), it.Name(), it.Value())
		}
	}

	if _, err := t.fh.Seek(t.wrote, io.SeekStart); err != nil {
		return errs.Wrap(err)
	}

	t.ro = false
	return nil
}

func (t *T) File() filesystem.Handle {
	return t.fh
}

func (t *T) Done() bool { return t.done }

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

	if t.len+padded > L0DataSize {
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

	t.keys = append(t.keys, keyPos{
		key: key,
		pos: uint16(t.len / l0EntryAlignment),
	})

	t.len += padded
	return true, nil
}

func (t *T) storeErr(err error) error {
	t.err = errs.Wrap(err)
	return t.err
}

func (t *T) flush() error {
	if !t.ro && len(t.buf) > 0 {
		if _, err := t.fh.Write(t.buf); err != nil {
			return t.storeErr(err)
		}
	}
	t.wrote += int64(len(t.buf))
	t.buf = t.buf[:0]
	return nil
}

func (t *T) finish() error {
	if len(t.buf) > 0 && t.flush() != nil {
		return t.err
	}
	t.done = true

	if t.ro {
		return nil
	}

	if err := t.fh.Truncate(L0DataSize); err != nil {
		return t.storeErr(err)
	}

	if _, err := t.fh.Seek(L0DataSize, io.SeekStart); err != nil {
		return t.storeErr(err)
	}

	if c := 4*len(t.keys) + 4; cap(t.buf) < c {
		t.buf = make([]byte, 0, c)
	}
	buf := t.buf[:0]

	sort.Sort(&t.keys)

	for _, ent := range t.keys {
		kp := binary.BigEndian.Uint16(ent.key[0:2])
		buf = appendUint16(buf, kp)
		buf = appendUint16(buf, ent.pos)
	}

	buf = appendUint32(buf, checksum(buf))

	if _, err := t.fh.Write(buf); err != nil {
		return t.storeErr(err)
	}

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
