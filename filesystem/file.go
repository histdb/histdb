package filesystem

import (
	"io"
	"os"
	"syscall"

	"github.com/zeebo/errs"
)

type File struct {
	fd uintptr
}

func (fh File) Close() (err error) {
	return errs.Wrap(syscall.Close(int(fh.fd)))
}

func (fh File) Write(p []byte) (n int, err error) {
	n, err = syscall.Write(int(fh.fd), p)
	return n, errs.Wrap(err)
}

func (fh File) Read(p []byte) (n int, err error) {
	n, err = syscall.Read(int(fh.fd), p)
	if err == nil && n == 0 {
		return 0, io.EOF
	}
	return n, errs.Wrap(err)
}

var _ = (*os.File).Stat

func (fh File) ReadAt(p []byte, off int64) (n int, err error) {
	for len(p) > 0 {
		m, err := syscall.Pread(int(fh.fd), p, off)
		if err != nil {
			return n, errs.Wrap(err)
		}
		n += m
		p = p[m:]
		off += int64(m)
	}
	return n, nil
}

func (fh File) Seek(offset int64, whence int) (off int64, err error) {
	off, err = syscall.Seek(int(fh.fd), offset, whence)
	return off, errs.Wrap(err)
}

func (fh File) Truncate(n int64) (err error) {
	return errs.Wrap(syscall.Ftruncate(int(fh.fd), n))
}

func (fh File) Sync() (err error) {
	return errs.Wrap(syscall.Fsync(int(fh.fd)))
}

func (fh File) Fd() int { return int(fh.fd) }

func (fh File) Size() (int64, error) {
	var stat syscall.Stat_t
	if err := syscall.Fstat(int(fh.fd), &stat); err != nil {
		return 0, errs.Wrap(err)
	}
	return stat.Size, nil
}

func (fh File) Fallocate(n int64) error {
	return errs.Wrap(syscall.Fallocate(int(fh.fd), 0, 0, n))
}
