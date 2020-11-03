package testhelp

import (
	"fmt"
	"io"
	"io/ioutil"
	"testing"
	"time"

	"github.com/zeebo/assert"
	"github.com/zeebo/errs/v2"
	"github.com/zeebo/lsm/filesystem"
)

func Tempfile(tb testing.TB, fs *filesystem.T) (filesystem.Handle, func()) {
	name := fmt.Sprint(time.Now().UnixNano())
	fh, err := fs.Create(name)
	assert.NoError(tb, err)
	return fh, func() {
		assert.NoError(tb, errs.Combine(fh.Close(), fs.Remove(name)))
	}
}

func ReadFile(tb testing.TB, fh filesystem.Handle) []byte {
	pos, err := fh.Seek(0, io.SeekCurrent)
	assert.NoError(tb, err)
	_, err = fh.Seek(0, io.SeekStart)
	assert.NoError(tb, err)
	data, err := ioutil.ReadAll(fh)
	assert.NoError(tb, err)
	_, err = fh.Seek(pos, io.SeekStart)
	assert.NoError(tb, err)
	return data
}
