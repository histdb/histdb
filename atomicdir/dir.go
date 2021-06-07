package atomicdir

import (
	"errors"
	"io"
	"os"
	"sync"

	"github.com/zeebo/errs/v2"

	"github.com/histdb/histdb/filesystem"
)

type T struct {
	fs *filesystem.T

	mu   sync.Mutex
	txn  uint16
	txns map[uint16]struct{}
}

func (t *T) Init(fs *filesystem.T) error {
	t.fs = fs
	t.txns = make(map[uint16]struct{})

	fh, err := t.fs.OpenRead(".")
	if err != nil {
		return errs.Wrap(err)
	}
	defer fh.Close()

	for {
		names, err := fh.Readdirnames(16)
		for _, name := range names {
			txn, ok := parseTransaction(name)
			if ok {
				t.txns[txn] = struct{}{}
				if txn > t.txn {
					t.txn = txn
				}
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

func (t *T) OpenCurrent() (*Transaction, error) {
	current, err := t.fs.Readlink("current")
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	} else if err != nil {
		return nil, errs.Wrap(err)
	}
	return t.openTxn(current)
}

func (t *T) SetCurrent(tx *Transaction) error {
	var buf [5]byte
	writeTransaction(&buf, tx.txn)

	if err := t.fs.Link("current", string(buf[:])); err != nil {
		return errs.Wrap(err)
	}

	// TODO: figure out how and what to fsync
	// TODO: can remove any non-open, non-current transactions

	return nil
}

func (t *T) openTxn(name string) (_ *Transaction, err error) {
	tx := new(Transaction)
	defer func() {
		if err != nil {
			err = errs.Combine(err, tx.Close())
		}
	}()

	txn, ok := parseTransaction(name)
	if !ok {
		return nil, errs.Errorf("invalid name: %q", name)
	}
	tx.txn = txn

	fh, err := t.fs.OpenRead(name)
	if err != nil {
		return nil, errs.Wrap(err)
	}
	defer fh.Close()

	for {
		names, err := fh.Readdirnames(16)
		for _, name := range names {
			f, ok := parseFile(name)
			if !ok {
				continue
			}

			var buf [22]byte
			writeTransactionFile(&buf, txn, f)

			fh, err := t.fs.OpenRead(string(buf[:]))
			if err != nil {
				return nil, errs.Wrap(err)
			}

			tx.include(f, fh)
		}

		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return nil, errs.Wrap(err)
		}
	}

	if err := tx.validate(); err != nil {
		return nil, err
	}

	return tx, nil
}

func (t *T) nextTxn() (uint16, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	for i := 0; i < 1<<16; i++ {
		t.txn++
		if _, ok := t.txns[t.txn]; !ok {
			return t.txn, true
		}
	}

	return 0, false
}

func (t *T) NewTransaction(fn func(*Operations)) (*Transaction, error) {
	txn, ok := t.nextTxn()
	if !ok {
		return nil, errs.Errorf("no available transaction number")
	}

	var buf [5]byte
	writeTransaction(&buf, txn)
	dir := string(buf[:])

	if err := t.fs.Mkdir(dir); err != nil {
		return nil, errs.Wrap(err)
	}

	ops := &Operations{fs: t.fs, txn: txn}

	fn(ops)

	if err := ops.close(); err != nil {
		_ = t.fs.RemoveAll(dir)
		return nil, err
	}

	return t.openTxn(dir)
}
