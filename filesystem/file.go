package filesystem

import (
	"io"
	"syscall"

	"github.com/zeebo/errs"
)

type File struct {
	fd uintptr
}

func (fh File) Fd() int {
	return int(fh.fd)
}

func (fh File) Close() (err error) {
intr:
	err = syscall.Close(int(fh.fd))
	if err == syscall.EINTR {
		goto intr
	}
	return errs.Wrap(err)
}

func (fh File) Write(p []byte) (n int, err error) {
intr:
	n, err = syscall.Write(int(fh.fd), p)
	if err == syscall.EINTR {
		goto intr
	}
	return n, errs.Wrap(err)
}

func (fh File) Read(p []byte) (n int, err error) {
intr:
	n, err = syscall.Read(int(fh.fd), p)
	if err == nil && n == 0 {
		return 0, io.EOF
	} else if err == syscall.EINTR {
		goto intr
	}
	return n, errs.Wrap(err)
}

func (fh File) ReadAt(p []byte, off int64) (n int, err error) {
	for len(p) > 0 {
		m, err := syscall.Pread(int(fh.fd), p, off)
		if err == syscall.EINTR {
			continue
		} else if err != nil {
			return n, errs.Wrap(err)
		} else if m == 0 {
			return n, io.EOF
		}
		n += m
		p = p[m:]
		off += int64(m)
	}
	return n, nil
}

func (fh File) Seek(offset int64, whence int) (off int64, err error) {
intr:
	off, err = syscall.Seek(int(fh.fd), offset, whence)
	if err == syscall.EINTR {
		goto intr
	}
	return off, errs.Wrap(err)
}

func (fh File) Truncate(n int64) (err error) {
intr:
	err = syscall.Ftruncate(int(fh.fd), n)
	if err == syscall.EINTR {
		goto intr
	}
	return errs.Wrap(err)
}

func (fh File) Sync() (err error) {
intr:
	err = syscall.Fsync(int(fh.fd))
	if err == syscall.EINTR {
		goto intr
	}
	return errs.Wrap(err)
}

func (fh File) Size() (int64, error) {
intr:
	var stat syscall.Stat_t
	err := syscall.Fstat(int(fh.fd), &stat)
	if err == syscall.EINTR {
		goto intr
	} else if err != nil {
		return 0, errs.Wrap(err)
	}
	return stat.Size, nil
}

func (fh File) Fallocate(n int64) (err error) {
intr:
	err = syscall.Fallocate(int(fh.fd), 0, 0, n)
	if err == syscall.EINTR {
		goto intr
	}
	return errs.Wrap(err)
}
