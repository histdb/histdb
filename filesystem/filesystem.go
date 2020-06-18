package filesystem

import (
	"os"
	"runtime"
	"syscall"
	"unsafe"
)

type T struct {
}

func (t *T) Create(path string) (fh File, err error) {
	fd, _, ec := syscall.Syscall(
		syscall.SYS_OPEN,
		uintptr(*(*unsafe.Pointer)(unsafe.Pointer(&path))),
		uintptr(os.O_RDWR|os.O_CREATE),
		0644,
	)
	if ec != 0 {
		return File{}, ec
	}

	runtime.KeepAlive(path)
	return File{fd}, nil
}

func (t *T) Open(path string) (fh File, err error) {
	fd, _, ec := syscall.Syscall(
		syscall.SYS_OPEN,
		uintptr(*(*unsafe.Pointer)(unsafe.Pointer(&path))),
		uintptr(os.O_RDONLY),
		0644,
	)
	if ec != 0 {
		return File{}, ec
	}

	runtime.KeepAlive(path)
	return File{fd}, nil
}

var _AT_FDCWD = -0x64

func (t *T) Rename(old, new string) error {
	_, _, ec := syscall.Syscall6(syscall.SYS_RENAMEAT,
		uintptr(_AT_FDCWD),
		uintptr(*(*unsafe.Pointer)(unsafe.Pointer(&old))),
		uintptr(_AT_FDCWD),
		uintptr(*(*unsafe.Pointer)(unsafe.Pointer(&new))),
		0, 0)
	if ec != 0 {
		return ec
	}

	runtime.KeepAlive(old)
	runtime.KeepAlive(new)
	return nil
}

func (t *T) Remove(path string) error {
	_, _, ec := syscall.Syscall(syscall.SYS_UNLINKAT,
		uintptr(_AT_FDCWD),
		uintptr(*(*unsafe.Pointer)(unsafe.Pointer(&path))),
		0)

	if ec != 0 {
		return ec
	}

	runtime.KeepAlive(path)
	return nil
}
