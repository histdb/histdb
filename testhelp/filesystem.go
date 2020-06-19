package testhelp

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/zeebo/assert"
	"github.com/zeebo/errs"
	"github.com/zeebo/lsm/filesystem"
)

func Tempfile(tb testing.TB, fs *filesystem.T) (filesystem.File, func()) {
	tmpdir := os.Getenv("TMPDIR")
	if tmpdir == "" {
		tmpdir = "/tmp"
	}
	name := filepath.Join(tmpdir, fmt.Sprint(time.Now().UnixNano())+"\x00")

	fh, err := fs.Create(name)
	assert.NoError(tb, err)
	return fh, func() {
		assert.NoError(tb, errs.Combine(fh.Close(), fs.Remove(name)))
	}
}
