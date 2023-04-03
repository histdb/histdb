package store

import (
	"io"

	"github.com/zeebo/errs/v2"

	"github.com/histdb/histdb"
	"github.com/histdb/histdb/atomicdir"
	"github.com/histdb/histdb/buffer"
	"github.com/histdb/histdb/filesystem"
	"github.com/histdb/histdb/level0"
	"github.com/histdb/histdb/leveln"
	"github.com/histdb/histdb/memindex"
	"github.com/histdb/histdb/rwutils"
)

type T struct {
	fs  *filesystem.T
	dir atomicdir.T
	txn atomicdir.Txn

	l0  level0.T
	l0m memindex.T // all l0s share memindex

	// readonly data
	l0s  []level0.T
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
			ops.Allocate(atomicdir.File{
				Generation: 0,
				Kind:       atomicdir.KindLevel0,
			}, level0.L0DataSize)
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

	addIndex := func(key histdb.Key, name, value []byte) { t.l0m.Add(name) }

	l0s := t.txn.L0s()
	lns := t.txn.LNs()

	// load the earliest l0
	if err := t.l0.Init(l0s[0].Handle, addIndex); err != nil {
		return err
	}
	l0s = l0s[1:]

	// load the remaining l0s
	t.l0s = make([]level0.T, len(l0s))
	for i, fh := range l0s {
		if err := t.l0s[i].Init(fh.Handle, addIndex); err != nil {
			return err
		}
	}

	// load the lns
	t.lns = make([]leveln.Reader, len(lns))
	t.lnms = make([]memindex.T, len(lns))

	for i, ln := range lns {
		t.lns[i].Init(ln.Keys.Handle, ln.Values.Handle)

		// memindex holds on to the underlying bytes, so
		// we do this. maybe it should copy. hmm.
		data, err := io.ReadAll(ln.Memindex.Handle)
		if err != nil {
			return errs.Wrap(err)
		}

		var r rwutils.R
		r.Init(buffer.OfLen(data))
		t.lnms[i].ReadFrom(&r)
		if _, err := r.Done(); err != nil {
			return errs.Wrap(err)
		}
	}

	return nil
}

func (t *T) Write(ts uint32, name, value []byte) (err error) {
	hash, _ := t.l0m.Add(name)

	var key histdb.Key
	*key.HashPtr() = hash
	*key.TimestampPtr() = ts

	for {
		ok, err := t.l0.Append(key, name, value)
		if err != nil {
			return err
		} else if ok {
			return nil
		}

		var txn atomicdir.Txn
		defer func() {
			if err != nil {
				err = errs.Combine(err, txn.Close())
			}
		}()

		if err := t.dir.InitTxn(&txn, func(ops atomicdir.Ops) atomicdir.Ops {
			for _, fh := range t.txn.FHs() {
				ops.Include(&t.txn, fh.File)
			}
			ops.Allocate(atomicdir.File{
				Generation: t.txn.MaxGen() + 1,
				Kind:       atomicdir.KindLevel0,
			}, level0.L0DataSize)
			return ops
		}); err != nil {
			return err
		}

		if err := t.dir.SetCurrent(&txn); err != nil {
			return err
		}

		var l0 level0.T
		if err := l0.Init(txn.L0s()[0].Handle, nil); err != nil {
			return err
		}

		if err := t.txn.Close(); err != nil {
			return err
		}

		t.txn = txn
		t.l0 = l0
	}
}
