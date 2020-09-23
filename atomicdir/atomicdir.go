package atomicdir

import "github.com/zeebo/lsm/filesystem"

type Transaction struct{}

func Write(base filesystem.File, fn func(Transaction) error) error {
	return nil
}

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
