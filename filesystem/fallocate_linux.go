package filesystem

import "syscall"

func fallocate(fd int, mode uint32, off int64, len int64) error {
	return syscall.Fallocate(fd, mode, off, len)
}
