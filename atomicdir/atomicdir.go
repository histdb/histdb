package atomicdir

import (
	"errors"
	"io"
	"os"
	"sort"

	"github.com/zeebo/errs/v2"

	"github.com/histdb/histdb/filesystem"
)

type Dir struct {
	gen     uint32
	txn     uint16
	handles []NamedHandle
}

type NamedHandle struct {
	File   File
	Handle filesystem.Handle
}

func (d *Dir) include(f File, fh filesystem.Handle) {
	if f.Generation > d.gen {
		d.gen = f.Generation
	}
	if f.Transaction > d.txn {
		d.txn = f.Transaction
	}
	d.handles = append(d.handles, NamedHandle{
		File:   f,
		Handle: fh,
	})
}

func (d *Dir) sort() {
	sort.Slice(d.handles, func(i, j int) bool {
		// newest data is in later generations and lower levels
		// so we want to sort by largest generation first, then
		// by smallest level.
		switch nhi, nhj := d.handles[i], d.handles[j]; {
		case nhi.File.Generation < nhj.File.Generation:
			return false
		case nhi.File.Generation > nhj.File.Generation:
			return true
		case nhi.File.Level < nhj.File.Level:
			return true
		case nhi.File.Level > nhj.File.Level:
			return false
		default:
			return false
		}
	})
}

func (d *Dir) Close() error {
	var eg errs.Group
	for _, nh := range d.handles {
		eg.Add(nh.Handle.Close())
	}
	return eg.Err()
}

func Open(fs filesystem.T) (_ *Dir, err error) {
	dir := new(Dir)
	defer func() {
		if err != nil {
			err = errs.Combine(err, dir.Close())
		}
	}()

	current, err := fs.Readlink("current")
	if err != nil && errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}

	// parse all the files in the current directory
	fh, err := fs.OpenRead(current)
	if err != nil {
		return nil, errs.Wrap(err)
	}
	defer fh.Close()

	buf := [21]byte{8: pathSep}
	copy(buf[:], current)

	for {
		names, err := fh.Readdirnames(16)
		for _, name := range names {
			if len(name) != 12 {
				continue
			}
			copy(buf[9:21], name)

			f, ok := parseFile(string(buf[:]))
			if !ok {
				continue
			}

			fh, err := fs.OpenRead(string(buf[:]))
			if err != nil {
				return nil, errs.Wrap(err)
			}

			dir.include(f, fh)
		}

		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return nil, errs.Wrap(err)
		}
	}

	dir.sort()
	return dir, nil
}

func Create(fs filesystem.T, fn func(*Operations)) (*Dir, error) {
	ops := &Operations{fs: fs}
	fn(ops)
	ops.closed = true

	if ops.err != nil {
		return nil, ops.err
	}

	return Open(fs)
}

type Operations struct {
	fs     filesystem.T
	closed bool
	err    error
}

func (ops *Operations) done() bool {
	return ops.closed || ops.err != nil
}

func (ops *Operations) store(err error) {
	if ops.err == nil && err != nil {
		ops.err = err
	}
}

func (ops *Operations) Link(old, new File) {
	if ops.done() {
		return
	}

	// source := ops.base.Child(old.Name())
	// dest := ops.base.Child(new.Name())
	// ops.store(ops.base.Filesystem().Link(source, dest))
}

// type Transaction struct{}

// func Write(base filesystem.File, fn func(Transaction) error) error {
// 	return nil
// }

// func (txn Transaction) MaxGeneration() uint32

// func (txn Transaction) Link(from, to File) error

// func (txn Transaction) Touch(f File) error

/*
func (t *T) recover() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// find the generation that is the current active one
	// assumes current == "" if err != nil
	current, err := t.fs.Readlink(t.base.Child("current"))
	if err != nil && errors.Is(err, os.ErrNotExist) {
		return errs.Wrap(err)
	}

	// seek to the start of the directory entries
	if _, err := t.base.Seek(0, io.SeekStart); err != nil {
		return errs.Wrap(err)
	}

	// remove all of the non-current files
	for {
		names, err := t.base.Readdirnames(8)
		for _, name := range names {
			if name != "current" && name != current {
				if err := t.fs.RemoveAll(t.base.Child(name)); err != nil {
					return errs.Wrap(err)
				}
			}
		}
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return errs.Wrap(err)
		}
	}

	// parse all the files in the current directory
	fh, err := t.fs.OpenRead(t.base.Child(current))
	if err != nil {
		return errs.Wrap(err)
	}
	defer fh.Close()

	buf := [21]byte{8: pathSep}
	copy(buf[:], current)

	for {
		names, err := fh.Readdirnames(8)
		for _, name := range names {
			if len(name) != 12 {
				return errs.Errorf("invalid file: %v", name)
			}
			copy(buf[9:21], name)

			f, ok := parseFile(string(buf[:]))
			if !ok {
				return errs.Errorf("invalid file: %v", name)
			}

			t.files = append(t.files, f)
			if f.Generation > t.gen {
				t.gen = f.Generation
			}
			if f.Transaction > t.txn {
				t.txn = f.Transaction
			}
		}

		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return errs.Wrap(err)
		}
	}

	return nil
}
*/
