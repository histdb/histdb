package store

import (
	"github.com/zeebo/errs/v2"

	"github.com/histdb/histdb"
	"github.com/histdb/histdb/filesystem"
	"github.com/histdb/histdb/level0"
	"github.com/histdb/histdb/memindex"
)

// design: each level n has an assocaited memindex used for name lookups/queries
// there's a top level memindex for the level0s? but how to clean out during
// compaction the property could be that the level0 memindex has a superset of
// the level0 keys. the full names aren't stored anywhere then, though. is that
// a problem? maybe they go into the levelns as well? ugh. nah just rely on the
// memindex. it's the same thing. we don't actually need to store the name then
// because the hash identifies it.

type Config struct {
	_ [0]func() // no equality

	L0Width int // number of files in l0 before compacting
}

type T struct {
	_ [0]func() // no equality

	cfg Config

	fs *filesystem.T

	l0   level0.T
	l0m  memindex.T
	norm []byte
}

func (t *T) Init(fs *filesystem.T) (err error) {
	*t = T{
		fs:   fs,
		norm: t.norm[:0],
	}

	if t.norm == nil {
		t.norm = make([]byte, 0, 64)
	}

	return nil
}

func (t *T) Write(name, value []byte, ts uint32, dur uint16) (err error) {
	var key histdb.Key
	*key.HashPtr(), _, name, _ = t.l0m.Add(name, t.norm[:0], nil)
	key.SetTimestamp(ts)

	for {
		ok, err := t.l0.Append(key, name, value)
		if err != nil {
			return errs.Wrap(err)
		} else if ok {
			return nil
		}

	}
}
