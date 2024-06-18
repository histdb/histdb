package filesystem

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/zeebo/errs/v2"
)

type T struct {
	_ [0]func() // no equality

	Base string
}

func (t *T) child(path string) string {
	return filepath.Join(t.Base, path)
}

func (t *T) Create(path string) (fh H, err error) {
	path = t.child(path)

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	return H{fs: t, fh: f}, errs.Wrap(err)
}

func (t *T) OpenWrite(path string) (fh H, err error) {
	path = t.child(path)

	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	return H{fs: t, fh: f}, errs.Wrap(err)
}

func (t *T) OpenRead(path string) (fh H, err error) {
	path = t.child(path)

	f, err := os.Open(path)
	return H{fs: t, fh: f}, errs.Wrap(err)
}

func (t *T) Rename(old, new string) error {
	old = t.child(old)
	new = t.child(new)

	return errs.Wrap(os.Rename(old, new))
}

func (t *T) Remove(path string) error {
	path = t.child(path)

	return errs.Wrap(os.Remove(path))
}

func (t *T) RemoveAll(path string) error {
	path = t.child(path)

	if !strings.HasPrefix(path, "/tmp/") {
		return errs.Errorf("path must begin with /tmp/: %q", path)
	}
	return errs.Wrap(os.RemoveAll(path))
}

func (t *T) Readlink(path string) (link string, err error) {
	path = t.child(path)

	link, err = os.Readlink(path)
	return link, errs.Wrap(err)
}

func (t *T) Mkdir(path string) (err error) {
	path = t.child(path)

	return errs.Wrap(os.MkdirAll(path, 0755))
}

func (t *T) Symlink(old, new string) (err error) {
	old = t.child(old)
	new = t.child(new)

	return errs.Wrap(os.Symlink(old, new))
}

func (t *T) Link(old, new string) (err error) {
	old = t.child(old)
	new = t.child(new)

	return errs.Wrap(os.Link(old, new))
}
