package level0

import (
	"encoding/binary"

	"github.com/zeebo/errs"
	"github.com/zeebo/lsm"
	"github.com/zeebo/lsm/filesystem"
)

type T struct {
	buf  []byte
	fh   filesystem.File
	len  uint32
	mcap uint32
	fcap uint32
	keys keyHeap
	pos  map[lsm.Key][2]uint32
	done bool
}

func (t *T) Init(fh filesystem.File, mcap, fcap uint32) {
	*t = T{
		buf:  make([]byte, 0, mcap),
		fh:   fh,
		mcap: mcap,
		fcap: fcap,
		pos:  make(map[lsm.Key][2]uint32),
	}
}

func (t *T) appendUint32(x uint32) {
	var lbuf [4]byte
	binary.BigEndian.PutUint32(lbuf[:], x)
	t.buf = append(t.buf, lbuf[:]...)
}

func (t *T) appendMem(key lsm.Key, value []byte) (err error) {
	if t.done {
		return errs.New("write to finished level0 log file")
	}

	t.buf = append(t.buf, key[:]...)
	t.appendUint32(uint32(len(value)))
	t.buf = append(t.buf, value...)

	if _, ok := t.pos[key]; !ok {
		t.keys = t.keys.Push(key)
	}
	entrySize := uint32(len(value)) + uint32(len(key)) + 4
	t.pos[key] = [2]uint32{t.len, entrySize}
	t.len += entrySize

	return nil
}

func (t *T) Append(key lsm.Key, value []byte) (bool, error) {
	if err := t.appendMem(key, value); err != nil {
		return false, err
	} else if t.len >= t.fcap {
		return true, t.fullFlush()
	} else if uint32(len(t.buf)) > t.mcap {
		return false, t.memFlush()
	} else {
		return false, nil
	}
}

func (t *T) fullFlush() (err error) {
	// flush any pending
	if len(t.buf) > 0 {
		if err := t.memFlush(); err != nil {
			return err
		}
	}

	// write out the index and index start position
	var key lsm.Key
	for len(t.keys) > 0 {
		t.keys, key = t.keys.Pop()
		ent := t.pos[key]
		t.appendUint32(ent[0])
		t.appendUint32(ent[1])
	}
	t.appendUint32(t.len)

	// flush the index
	if err := t.memFlush(); err != nil {
		return err
	}

	t.done = true
	return nil
}

func (t *T) memFlush() (err error) {
	if _, err := t.fh.Write(t.buf); err != nil {
		t.done = true
		return err
	} else if err = t.fh.Sync(); err != nil {
		t.done = true
		return err
	}

	t.buf = t.buf[:0]
	return nil
}

func (t *T) Iterator() (it Iterator, err error) {
	if t.done {
		it.Init(t.fh)
	} else {
		err = errs.New("iterator on unfinished level0 file")
	}
	return
}
