package filesystem

import (
	"os"
	"strings"

	"github.com/zeebo/errs/v2"
)

type T struct {
}

func (t *T) Create(path string) (fh File, err error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	return File{t, f}, errs.Wrap(err)
}

func (t *T) OpenWrite(path string) (fh File, err error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	return File{t, f}, errs.Wrap(err)
}

func (t *T) OpenRead(path string) (fh File, err error) {
	f, err := os.Open(path)
	return File{t, f}, errs.Wrap(err)
}

func (t *T) Rename(old, new string) error {
	return errs.Wrap(os.Rename(old, new))
}

func (t *T) Remove(path string) error {
	return errs.Wrap(os.Remove(path))
}

func (t *T) RemoveAll(path string) error {
	if !strings.HasPrefix(path, "/tmp/") {
		return errs.Errorf("path must begin with /tmp/: %q", path)
	}
	return errs.Wrap(os.RemoveAll(path))
}

func (t *T) Readlink(path string) (link string, err error) {
	link, err = os.Readlink(path)
	return link, errs.Wrap(err)
}

func (t *T) Mkdir(path string) (err error) {
	err = os.MkdirAll(path, 0755)
	return errs.Wrap(err)
}

func (t *T) Symlink(oldname, newname string) (err error) {
	err = os.Symlink(oldname, newname)
	return errs.Wrap(err)
}
