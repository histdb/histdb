package level0

import (
	"fmt"
	"sort"
	"testing"

	"github.com/zeebo/assert"

	"github.com/histdb/histdb"
	"github.com/histdb/histdb/filesystem"
	"github.com/histdb/histdb/testhelp"
)

type Entry struct {
	Key   histdb.Key
	Name  []byte
	Value []byte
}

func (e Entry) String() string {
	return fmt.Sprintf("(%s %x)", e.Key, e.Value)
}

func Level0(tb testing.TB, fs *filesystem.T, nlen, vlen int) (*T, []Entry, func()) {
	ok := false
	fh, cleanup := testhelp.Tempfile(tb, fs)
	defer func() {
		if !ok {
			cleanup()
		}
	}()

	var l0 T
	assert.NoError(tb, l0.Init(fh, nil))

	var entries []Entry
	for {
		ent := Entry{
			Key:   testhelp.Key(),
			Name:  testhelp.Name(nlen),
			Value: testhelp.Value(vlen),
		}

		ok, err := l0.Append(ent.Key, ent.Name, ent.Value)
		assert.NoError(tb, err)
		if !ok {
			break
		}

		entries = append(entries, ent)
	}

	sort.Slice(entries, func(i, j int) bool {
		return string(entries[i].Key[:]) < string(entries[j].Key[:])
	})

	ok = true
	return &l0, entries, cleanup
}
