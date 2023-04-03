package atomicdir

import (
	"errors"
	"io"
	"os"

	"github.com/zeebo/errs/v2"

	"github.com/histdb/histdb/filesystem"
)

type T struct {
	fs  *filesystem.T
	tid uint32
}

func (t *T) Init(fs *filesystem.T) error {
	*t = T{
		fs: fs,
	}

	fh, err := t.fs.OpenRead(".")
	if err != nil {
		return errs.Wrap(err)
	}
	defer fh.Close()

	for {
		names, err := fh.Readdirnames(16)
		for _, name := range names {
			tid, ok := parseTransaction(name)
			if ok && tid > t.tid {
				t.tid = tid
			}
		}
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return errs.Wrap(err)
		}
	}

	current, err := fs.Readlink("current")
	if errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return errs.Wrap(err)
	} else if _, ok := parseTransaction(current); !ok {
		return errs.Errorf("invalid current: %q", current)
	}

	return nil
}

func (t *T) InitCurrent(txn *Txn) (bool, error) {
	current, err := t.fs.Readlink("current")
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	} else if err != nil {
		return false, errs.Wrap(err)
	}
	return true, t.openTxn(txn, current)
}

func (t *T) SetCurrent(txn *Txn) error {
	var buf [8]byte
	writeTransaction(&buf, txn.tid)

	if err := t.fs.Symlink(string(buf[:]), "current-next"); err != nil {
		return errs.Wrap(err)
	}
	if err := t.fs.Rename("current-next", "current"); err != nil {
		return errs.Wrap(err)
	}

	// TODO: figure out how and what to fsync
	// TODO: can remove any non-open, non-current transactions

	return nil
}

func (t *T) openTxn(txn *Txn, name string) (err error) {
	defer func() {
		if err != nil {
			err = errs.Combine(err, txn.Close())
		}
	}()

	tid, ok := parseTransaction(name)
	if !ok {
		return errs.Errorf("invalid name: %q", name)
	}
	txn.tid = tid

	fh, err := t.fs.OpenRead(name)
	if err != nil {
		return errs.Wrap(err)
	}
	defer fh.Close()

	for {
		names, err := fh.Readdirnames(16)
		for _, name := range names {
			f, ok := parseFile(name)
			if !ok {
				continue
			}

			var buf [20]byte
			writeTransactionFile(&buf, tid, f)

			fh, err := t.fs.OpenWrite(string(buf[:]))
			if err != nil {
				return errs.Wrap(err)
			}

			txn.include(f, fh)
		}

		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return errs.Wrap(err)
		}
	}

	if err := txn.validate(); err != nil {
		return err
	}

	return nil
}

func (t *T) InitTxn(txn *Txn, fn func(Ops) Ops) error {
	t.tid++

	var buf [8]byte
	writeTransaction(&buf, t.tid)
	dir := string(buf[:])

	if err := t.fs.Mkdir(dir); err != nil {
		return errs.Wrap(err)
	}

	ops := fn(Ops{fs: t.fs, tid: t.tid})

	if err := ops.close(t.fs); err != nil {
		return errs.Combine(
			err,
			txn.Close(),
			t.fs.RemoveAll(dir),
		)
	}

	return t.openTxn(txn, dir)
}
