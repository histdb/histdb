package atomicdir

import (
	"github.com/zeebo/errs/v2"

	"github.com/histdb/histdb/filesystem"
)

type Ops struct {
	_ [0]func() // no equality

	ops []op

	fs     *filesystem.T
	id     uint32
	closed bool
}

type op struct {
	dir *Dir
	sz  int64
	f   File
	in  bool
}

func (ops *Ops) close(fs *filesystem.T) error {
	if ops.closed {
		return nil
	}
	ops.closed = true
	if fs != ops.fs {
		return errs.Errorf("invalid operation return value")
	}

	return nil
}

func (ops *Ops) getPath(tid uint32, f File) string {
	var buf [25]byte
	writeDirectoryFile(&buf, tid, f)
	return string(buf[:])
}

func (ops *Ops) IncludeAll(txn *Dir) {
	for _, fh := range txn.fhs {
		ops.Include(txn, fh.File)
	}
}

func (ops *Ops) Exclude(txn *Dir, f File) {
	ops.ops = append(ops.ops, op{in: false, dir: txn, f: f})
}

func (ops *Ops) Include(txn *Dir, f File) {
	ops.ops = append(ops.ops, op{in: true, dir: txn, f: f})
}

func (ops *Ops) Allocate(f File, size int64) {
	ops.ops = append(ops.ops, op{f: f, sz: size})
}
