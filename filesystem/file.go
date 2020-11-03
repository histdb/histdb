package filesystem

import (
	"io"
	"os"
	"path/filepath"
	"syscall"

	"github.com/zeebo/errs/v2"
)

type Handle struct {
	fs *T
	fh *os.File
}

func wrap(err error) error {
	if err != nil && err != io.EOF {
		return errs.Wrap(err)
	}
	return err
}

func (fh Handle) Filesystem() *T {
	return fh.fs
}

func (fh Handle) Fd() int {
	return int(fh.fh.Fd())
}

func (fh Handle) Name() string {
	return fh.fh.Name()
}

func (fh Handle) Child(name string) string {
	return filepath.Join(fh.fh.Name(), name)
}

func (fh Handle) Close() (err error) {
	return wrap(fh.fh.Close())
}

func (fh Handle) Write(p []byte) (n int, err error) {
	n, err = fh.fh.Write(p)
	return n, wrap(err)
}

func (fh Handle) WriteAt(p []byte, off int64) (n int, err error) {
	n, err = fh.fh.WriteAt(p, off)
	return n, wrap(err)
}

func (fh Handle) Read(p []byte) (n int, err error) {
	n, err = fh.fh.Read(p)
	return n, wrap(err)
}

func (fh Handle) ReadAt(p []byte, off int64) (n int, err error) {
	n, err = fh.fh.ReadAt(p, off)
	return n, wrap(err)
}

func (fh Handle) Seek(offset int64, whence int) (off int64, err error) {
	off, err = fh.fh.Seek(offset, whence)
	return off, wrap(err)
}

func (fh Handle) Truncate(n int64) (err error) {
	return wrap(fh.fh.Truncate(n))
}

func (fh Handle) Sync() (err error) {
	return wrap(fh.fh.Sync())
}

func (fh Handle) Size() (int64, error) {
	fi, err := fh.fh.Stat()
	if err != nil {
		return 0, wrap(err)
	}
	return fi.Size(), nil
}

func (fh Handle) Fallocate(n int64) (err error) {
intr:
	err = syscall.Fallocate(fh.Fd(), 0, 0, n)
	if err == syscall.EINTR {
		goto intr
	}
	return wrap(err)
}

func (fh Handle) Readdirnames(n int) (names []string, err error) {
	names, err = fh.fh.Readdirnames(n)
	return names, wrap(err)
}
