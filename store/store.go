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

type T struct {
	fs  *filesystem.T
	dir atomicdir.T
	txn atomicdir.Txn

	l0  level0.T
	l0m memindex.T

	lns  []leveln.Reader
	lnms []memindex.T
}

func (t *T) Init(fs *filesystem.T) (err error) {
	*t = T{
		fs: fs,
	}

	if err := t.dir.Init(fs); err != nil {
		return err
	}

	if ok, err := t.dir.InitCurrent(&t.txn); err != nil {
		return err
	} else if !ok {
		err := t.dir.InitTxn(&t.txn, func(ops atomicdir.Ops) atomicdir.Ops {
			ops.Allocate(atomicdir.File{Generation: 0, Kind: kindLevel0}, level0.L0DataSize)
			return ops
		})
		if err != nil {
			return err
		}
	}
	defer func() {
		if err != nil {
			err = errs.Combine(err, t.txn.Close())
		}
	}()

	return nil
}
