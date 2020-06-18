package level0

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/zeebo/assert"
	"github.com/zeebo/errs"
	"github.com/zeebo/lsm"
	"github.com/zeebo/lsm/filesystem"
	"github.com/zeebo/pcg"
)

func newKey(tb testing.TB) (key lsm.Key) {
	for i := range key {
		key[i] = byte(pcg.Uint32n(256))
	}
	key[len(key)-1] = 0x80
	return key
}

func newValue(tb testing.TB, n int) []byte {
	v := make([]byte, n)
	for i := range v {
		v[i] = byte(pcg.Uint32n(256))
	}
	return v
}

func newTempfile(tb testing.TB, fs *filesystem.T) (filesystem.File, func()) {
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

type testEntry struct {
	key   lsm.Key
	value []byte
}

func (l0e testEntry) String() string {
	return fmt.Sprintf("(%s %x)", l0e.key, l0e.value)
}

func newLevel0(tb testing.TB, fs *filesystem.T, mcap, fcap int) (*T, []testEntry, func()) {
	ok := false
	fh, cleanup := newTempfile(tb, fs)
	defer func() {
		if !ok {
			cleanup()
		}
	}()

	var l0 T
	l0.Init(fh, uint32(mcap), uint32(fcap))

	var entries []testEntry
	for {
		ent := testEntry{key: newKey(tb), value: newValue(tb, 12)}

		entries = append(entries, ent)

		done, err := l0.Append(ent.key, ent.value)
		assert.NoError(tb, err)
		if done {
			break
		}
	}

	sort.Slice(entries, func(i, j int) bool {
		return lsm.KeyCmp.Less(entries[i].key, entries[j].key)
	})

	ok = true
	return &l0, entries, cleanup
}
