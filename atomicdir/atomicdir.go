package atomicdir

import (
	"errors"
	"io"
	"os"

	"github.com/zeebo/errs/v2"

	"github.com/histdb/histdb/filesystem"
)

type T struct {
	_ [0]func() // no equality

	fs *filesystem.T
	id uint32
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
			tid, ok := parseDirectoryName(name)
			if ok && tid > t.id {
				t.id = tid
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
	} else if _, ok := parseDirectoryName(current); !ok {
		return errs.Errorf("invalid current: %q", current)
	}

	return nil
}

func (t *T) InitCurrent(dir *Dir) (bool, error) {
	current, err := t.fs.Readlink("current")
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	} else if err != nil {
		return false, errs.Wrap(err)
	}
	return true, t.openDir(dir, current)
}

func (t *T) SetCurrent(dir *Dir) error {
	var buf [8]byte
	writeDirectoryName(&buf, dir.id)

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

func (t *T) openDir(dir *Dir, name string) (err error) {
	defer func() {
		if err != nil {
			err = errs.Combine(err, dir.Close())
		}
	}()

	id, ok := parseDirectoryName(name)
	if !ok {
		return errs.Errorf("invalid name: %q", name)
	}

	dir.reset(id)

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

			var buf [25]byte
			writeDirectoryFile(&buf, id, f)

			fh, err := t.fs.OpenWrite(string(buf[:]))
			if err != nil {
				return errs.Wrap(err)
			}

			dir.include(f, fh)
		}

		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return errs.Wrap(err)
		}
	}

	if err := dir.validate(); err != nil {
		return errs.Wrap(err)
	}

	return nil
}

func (t *T) InitDir(dir *Dir, fn func(Ops) Ops) error {
	t.id++

	var buf [8]byte
	writeDirectoryName(&buf, t.id)
	name := string(buf[:])

	if err := t.fs.Mkdir(name); err != nil {
		return errs.Wrap(err)
	}

	ops := fn(Ops{fs: t.fs, id: t.id})

	if err := ops.close(t.fs); err != nil {
		return errs.Combine(
			err,
			dir.Close(),
			t.fs.RemoveAll(name),
		)
	}

	return t.openDir(dir, name)
}
