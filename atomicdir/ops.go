package atomicdir

import (
	"github.com/zeebo/errs/v2"

	"github.com/histdb/histdb/filesystem"
)

type Ops struct {
	fs     *filesystem.T
	err    error
	tid    uint32
	closed bool
}

func (ops *Ops) close(fs *filesystem.T) error {
	ops.closed = true
	if ops.err == nil && fs != ops.fs {
		ops.err = errs.Errorf("invalid operation return value")
	}
	return ops.err
}

func (ops *Ops) getPath(tid uint32, f File) string {
	var buf [20]byte
	writeTransactionFile(&buf, tid, f)
	return string(buf[:])
}

func (ops *Ops) done() bool {
	return ops.closed || ops.err != nil
}

func (ops *Ops) store(err error) {
	if ops.err == nil && err != nil {
		ops.err = errs.Wrap(err)
	}
}

func (ops *Ops) Include(txn *Txn, f File) {
	if ops.done() {
		return
	}

	src := ops.getPath(txn.tid, f)
	dst := ops.getPath(ops.tid, f)

	err := ops.fs.Link(src, dst)
	ops.store(err)
}

func (ops *Ops) Allocate(f File, size int64) {
	if ops.done() {
		return
	}

	dst := ops.getPath(ops.tid, f)

	fh, err := ops.fs.Create(dst)
	ops.store(err)

	if err == nil {
		ops.store(fh.Fallocate(size))
		ops.store(fh.Close())
	}
}
