package filesystem

import (
	"os"

	"github.com/zeebo/errs"
)

type T struct {
}

func (t *T) Create(path string) (fh File, err error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	return File{f}, errs.Wrap(err)
}

func (t *T) Open(path string) (fh File, err error) {
	f, err := os.Open(path)
	return File{f}, errs.Wrap(err)
}

func (t *T) Rename(old, new string) error {
	return errs.Wrap(os.Rename(old, new))
}

func (t *T) Remove(path string) error {
	return errs.Wrap(os.Remove(path))
}
