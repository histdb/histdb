package store

import (
	"github.com/zeebo/errs/v2"

	"github.com/histdb/histdb/atomicdir"
	"github.com/histdb/histdb/filesystem"
	"github.com/histdb/histdb/level0"
	"github.com/histdb/histdb/leveln"
	"github.com/histdb/histdb/memindex"
)

const (
	kindLevel0       = 0
	kindLevelNKeys   = 1
	kindLevelNValues = 2
	kindMemindex     = 3
)

type lev0 struct {
	idx  memindex.T
	data level0.T
}

type levN struct {
	idx  memindex.T
	data leveln.Reader
}

type T struct {
	fs   *filesystem.T
	adir *atomicdir.T
	curr *atomicdir.Transaction

	head  lev0
	tails []levN
}

func (t *T) Init(fs *filesystem.T) (err error) {
	*t = T{
		fs: fs,
	}

	if err := t.adir.Init(fs); err != nil {
		return err
	}

	if t.curr, err = t.adir.OpenCurrent(); err != nil {
		return err
	}

	new := t.curr == nil
	if new {
		t.curr, err = t.adir.NewTransaction(func(ops *atomicdir.Operations) {
			ops.Allocate(atomicdir.File{Kind: kindLevel0}, level0.L0Size)
			ops.Allocate(atomicdir.File{Kind: kindMemindex}, 0)
		})
		if err != nil {
			return err
		}
	}
	defer func() {
		if err != nil {
			err = errs.Combine(err, t.curr.Close())
		}
	}()
	if err != nil {
		return err
	}

	handles := t.curr.Handles()

	return nil
}
