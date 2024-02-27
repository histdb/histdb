//go:build !linux

package filesystem

import (
	"github.com/zeebo/errs/v2"
)

func fallocate(fd int, mode uint32, off int64, len int64) error {
	return errs.Errorf("fallocate not supported")
}
