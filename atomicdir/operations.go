package atomicdir

import (
	"sync"

	"github.com/zeebo/errs/v2"

	"github.com/histdb/histdb/filesystem"
)

type Operations struct {
	fs  *filesystem.T
	txn uint16

	mu     sync.Mutex
	closed bool
	err    error
}

func (ops *Operations) close() error {
	ops.mu.Lock()
	defer ops.mu.Unlock()

	ops.closed = true
	return ops.err
}

func (ops *Operations) getPath(tx uint16, f File) string {
	var buf [16]byte
	writeTransactionFile(&buf, tx, f)
	return string(buf[:])
}

func (ops *Operations) done() bool {
	return ops.closed || ops.err != nil
}

func (ops *Operations) store(err error) {
	if ops.err == nil && err != nil {
		ops.err = errs.Wrap(err)
	}
}

func (ops *Operations) Include(tx *Transaction, f File) {
	ops.mu.Lock()
	defer ops.mu.Unlock()

	if ops.done() {
		return
	}

	src := ops.getPath(tx.txn, f)
	dst := ops.getPath(ops.txn, f)

	err := ops.fs.Link(src, dst)
	ops.store(err)
}

func (ops *Operations) Allocate(f File, size int64) {
	ops.mu.Lock()
	defer ops.mu.Unlock()

	if ops.done() {
		return
	}

	dst := ops.getPath(ops.txn, f)

	fh, err := ops.fs.Create(dst)
	ops.store(err)

	if err == nil {
		ops.store(fh.Fallocate(size))
		ops.store(fh.Close())
	}
}
