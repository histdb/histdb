package filesystem

import (
	"io"
	"os"
	"path/filepath"

	"github.com/zeebo/errs/v2"
)

type H struct {
	_ [0]func() // no equality

	fs *T
	fh *os.File
}

func wrap(err error) error {
	if err != nil && err != io.EOF {
		return errs.Wrap(err)
	}
	return err
}

func (h H) Valid() bool { return h.fs != nil && h.fh != nil }

func (h H) Close() (err error) {
	if !h.Valid() {
		return nil
	}
	return wrap(h.fh.Close())
}

func (h *H) Remove() error {
	if !h.Valid() {
		return nil
	}
	err := errs.Combine(
		h.Close(),
		os.Remove(h.fh.Name()), // N.B. not h.fs.Remove
	)
	h.fs = nil
	h.fh = nil
	return err
}

func (h H) Filesystem() *T {
	return h.fs
}

func (h H) Name() string {
	return h.fh.Name()
}

func (h H) Child(name string) string {
	return filepath.Join(h.fh.Name(), name)
}

func (h H) Write(p []byte) (n int, err error) {
	n, err = h.fh.Write(p)
	return n, wrap(err)
}

func (h H) WriteAt(p []byte, off int64) (n int, err error) {
	n, err = h.fh.WriteAt(p, off)
	return n, wrap(err)
}

func (h H) Read(p []byte) (n int, err error) {
	n, err = h.fh.Read(p)
	return n, wrap(err)
}

func (h H) ReadAt(p []byte, off int64) (n int, err error) {
	n, err = h.fh.ReadAt(p, off)
	return n, wrap(err)
}

func (h H) Seek(offset int64, whence int) (off int64, err error) {
	off, err = h.fh.Seek(offset, whence)
	return off, wrap(err)
}

func (h H) Truncate(n int64) (err error) {
	return wrap(h.fh.Truncate(n))
}

func (h H) Sync() (err error) {
	return wrap(h.fh.Sync())
}

func (h H) Size() (int64, error) {
	fi, err := h.fh.Stat()
	if err != nil {
		return 0, wrap(err)
	}
	return fi.Size(), nil
}

func (h H) Readdirnames(n int) (names []string, err error) {
	names, err = h.fh.Readdirnames(n)
	return names, wrap(err)
}
