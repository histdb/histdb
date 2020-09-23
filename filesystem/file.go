package filesystem

import (
	"os"
	"path/filepath"
	"syscall"

	"github.com/zeebo/errs/v2"
)

type File struct {
	fs *T
	fh *os.File
}

func (fh File) Filesystem() *T {
	return fh.fs
}

func (fh File) Fd() int {
	return int(fh.fh.Fd())
}

func (fh File) Name() string {
	return fh.fh.Name()
}

func (fh File) Child(name string) string {
	return filepath.Join(fh.fh.Name(), name)
}

func (fh File) Close() (err error) {
	return errs.Wrap(fh.fh.Close())
}

func (fh File) Write(p []byte) (n int, err error) {
	n, err = fh.fh.Write(p)
	return n, errs.Wrap(err)
}

func (fh File) WriteAt(p []byte, off int64) (n int, err error) {
	n, err = fh.fh.WriteAt(p, off)
	return n, errs.Wrap(err)
}

func (fh File) Read(p []byte) (n int, err error) {
	n, err = fh.fh.Read(p)
	return n, errs.Wrap(err)
}

func (fh File) ReadAt(p []byte, off int64) (n int, err error) {
	n, err = fh.fh.ReadAt(p, off)
	return n, errs.Wrap(err)
}

func (fh File) Seek(offset int64, whence int) (off int64, err error) {
	off, err = fh.fh.Seek(offset, whence)
	return off, errs.Wrap(err)
}

func (fh File) Truncate(n int64) (err error) {
	return errs.Wrap(fh.fh.Truncate(n))
}

func (fh File) Sync() (err error) {
	return errs.Wrap(fh.fh.Sync())
}

func (fh File) Size() (int64, error) {
	fi, err := fh.fh.Stat()
	if err != nil {
		return 0, errs.Wrap(err)
	}
	return fi.Size(), nil
}

func (fh File) Fallocate(n int64) (err error) {
intr:
	err = syscall.Fallocate(fh.Fd(), 0, 0, n)
	if err == syscall.EINTR {
		goto intr
	}
	return errs.Wrap(err)
}

func (fh File) Readdirnames(n int) (names []string, err error) {
	names, err = fh.fh.Readdirnames(n)
	return names, errs.Wrap(err)
}
