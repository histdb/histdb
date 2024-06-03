package store

import (
	"github.com/zeebo/errs/v2"

	"github.com/histdb/histdb"
	"github.com/histdb/histdb/atomicdir"
	"github.com/histdb/histdb/filesystem"
	"github.com/histdb/histdb/level0"
	"github.com/histdb/histdb/leveln"
	"github.com/histdb/histdb/memindex"
	"github.com/histdb/histdb/mergeiter"
)

type Config struct {
	_ [0]func() // no equality

	L0Width int // number of files in l0 before compacting
}

type T struct {
	_ [0]func() // no equality

	cfg Config

	fs   *filesystem.T
	at   atomicdir.T
	cdir atomicdir.Dir
	ndir atomicdir.Dir

	comp struct {
		dir atomicdir.Dir
		its []mergeiter.Iterator
		lnw leveln.Writer
		mi  mergeiter.T
	}

	l0   level0.T
	l0m  memindex.T // all l0s share memindex
	norm []byte
}

func (t *T) Init(fs *filesystem.T) (err error) {
	*t = T{
		fs: fs,
	}

	if err := t.at.Init(fs); err != nil {
		return errs.Wrap(err)
	}

	if ok, err := t.at.InitCurrent(&t.cdir); err != nil {
		return errs.Wrap(err)
	} else if !ok {
		err := t.at.InitDir(&t.cdir, func(ops atomicdir.Ops) atomicdir.Ops {
			ops.Allocate(atomicdir.File{
				Generation: 0,
				Kind:       atomicdir.KindLevel0,
			}, level0.L0DataSize)
			return ops
		})
		if err != nil {
			return errs.Wrap(err)
		}
	}
	defer func() {
		if err != nil {
			err = errs.Combine(err, t.cdir.Close())
		}
	}()

	addIndex := func(key histdb.Key, name, value []byte) {
		t.l0m.Add(name, nil, nil)
	}

	l0s := t.cdir.L0s()

	// load the earliest l0
	if err := t.l0.Init(l0s[0].Handle, addIndex); err != nil {
		return errs.Wrap(err)
	}
	l0s = l0s[1:]

	return nil
}

func (t *T) Write(ts uint32, name, value []byte) (err error) {
	if t.norm == nil {
		t.norm = make([]byte, 0, 64)
	}
	hash, _, name, _ := t.l0m.Add(name, t.norm[:0], nil)

	var key histdb.Key
	*key.HashPtr() = hash
	*key.TimestampPtr() = ts

	for {
		ok, err := t.l0.Append(key, name, value)
		if err != nil {
			return errs.Wrap(err)
		} else if ok {
			return nil
		}

		if err := t.at.InitDir(&t.ndir, func(ops atomicdir.Ops) atomicdir.Ops {
			for _, fh := range t.cdir.FHs() {
				ops.Include(&t.cdir, fh.File)
			}
			ops.Allocate(atomicdir.File{
				Generation: t.cdir.MaxGen() + 1,
				Kind:       atomicdir.KindLevel0,
			}, level0.L0DataSize)
			return ops
		}); err != nil {
			return errs.Wrap(errs.Combine(err, t.ndir.Close()))
		}

		if err := t.at.SetCurrent(&t.ndir); err != nil {
			return errs.Wrap(errs.Combine(err, t.ndir.Close()))
		}

		// TODO: errors here should put the store into an invalid state
		// where it has to reopen or something.

		// safety: l0.Init can't cause any mutations to t.l0
		l0 := t.l0
		if err := l0.Init(t.ndir.L0s()[0].Handle, nil); err != nil {
			return errs.Wrap(errs.Combine(err, t.ndir.Close()))
		}

		if err := t.cdir.Close(); err != nil {
			return errs.Wrap(errs.Combine(err, t.ndir.Close()))
		}

		// swap curr and next now that we've fully succeeded
		t.cdir, t.ndir = t.ndir, t.cdir
	}
}
