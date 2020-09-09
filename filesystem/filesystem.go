package filesystem

import (
	"os"
	"reflect"
	"runtime"
	"syscall"
	"unsafe"
)

type T struct {
}

// strData returns the data pointer for the string. It does not
// keep the string alive, so be sure to use runtime.KeepAlive.
func strData(x string) uintptr {
	return (*reflect.StringHeader)(unsafe.Pointer(&x)).Data
}

func (t *T) Create(path string) (fh File, err error) {
intr:
	fd, _, ec := syscall.Syscall(syscall.SYS_OPEN,
		strData(path),
		uintptr(os.O_RDWR|os.O_CREATE),
		0644,
	)
	if ec == syscall.EINTR {
		goto intr
	} else if ec != 0 {
		return File{}, ec
	}

	runtime.KeepAlive(path)
	return File{fd}, nil
}

func (t *T) Open(path string) (fh File, err error) {
intr:
	fd, _, ec := syscall.Syscall(syscall.SYS_OPEN,
		strData(path),
		uintptr(os.O_RDONLY),
		0644,
	)
	if ec == syscall.EINTR {
		goto intr
	} else if ec != 0 {
		return File{}, ec
	}

	runtime.KeepAlive(path)
	return File{fd}, nil
}

const _AT_FDCWD = ^uintptr(0x64) + 1

func (t *T) Rename(old, new string) error {
intr:
	_, _, ec := syscall.Syscall6(syscall.SYS_RENAMEAT,
		_AT_FDCWD, strData(old),
		_AT_FDCWD, strData(new),
		0, 0)
	if ec == syscall.EINTR {
		goto intr
	} else if ec != 0 {
		return ec
	}

	runtime.KeepAlive(old)
	runtime.KeepAlive(new)
	return nil
}

func (t *T) Remove(path string) error {
intr:
	_, _, ec := syscall.Syscall(syscall.SYS_UNLINKAT,
		_AT_FDCWD, strData(path),
		0)
	if ec == syscall.EINTR {
		goto intr
	} else if ec != 0 {
		return ec
	}

	runtime.KeepAlive(path)
	return nil
}
