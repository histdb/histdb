package store

import (
	"io"

	"github.com/zeebo/errs/v2"

	"github.com/histdb/histdb"
	"github.com/histdb/histdb/buffer"
	"github.com/histdb/histdb/filesystem"
	"github.com/histdb/histdb/leveln"
	"github.com/histdb/histdb/memindex"
	"github.com/histdb/histdb/rwutils"
)

type levelN struct {
	_ [0]func() // no equality

	low  uint32
	high uint32
	indx filesystem.H
	keys filesystem.H
	vals filesystem.H

	it  *leveln.Iterator
	idx *memindex.T
}

func newLevelN(fs *filesystem.T, low, high uint32) (ln levelN, err error) {
	defer func() {
		if err != nil {
			_ = ln.Remove()
			ln = levelN{}
		}
	}()

	ln.low, ln.high = low, high

	ln.indx, err = fs.Create(ln.file(filesystem.KindIndx))
	if err != nil {
		return ln, errs.Wrap(err)
	}

	ln.keys, err = fs.Create(ln.file(filesystem.KindKeys))
	if err != nil {
		return ln, errs.Wrap(err)
	}

	ln.vals, err = fs.Create(ln.file(filesystem.KindVals))
	if err != nil {
		return ln, errs.Wrap(err)
	}

	return ln, nil
}

func openLevelN(fs *filesystem.T, low, high uint32) (ln levelN, err error) {
	defer func() {
		if err != nil {
			ln = levelN{}
		}
	}()

	ln.low, ln.high = low, high

	ln.indx, err = fs.OpenRead(ln.file(filesystem.KindIndx))
	if err != nil {
		return ln, errs.Wrap(err)
	}

	ln.keys, err = fs.OpenRead(ln.file(filesystem.KindKeys))
	if err != nil {
		return ln, errs.Wrap(err)
	}

	ln.vals, err = fs.OpenRead(ln.file(filesystem.KindVals))
	if err != nil {
		return ln, errs.Wrap(err)
	}

	return ln, nil
}

func (ln *levelN) file(kind uint8) string {
	return filesystem.File{Low: ln.low, High: ln.high, Kind: kind}.String()
}

func (ln *levelN) all(cb func(h *filesystem.H) error) error {
	return errs.Wrap(errs.Combine(cb(&ln.indx), cb(&ln.keys), cb(&ln.vals)))
}

func (ln *levelN) Sync() error   { return ln.all((*filesystem.H).Sync) }
func (ln *levelN) Remove() error { return ln.all((*filesystem.H).Remove) }
func (ln *levelN) Close() error  { return ln.all((*filesystem.H).Close) }

func (ln *levelN) Index() (*memindex.T, error) {
	if ln.idx != nil {
		return ln.idx, nil
	}

	if _, err := ln.indx.Seek(0, io.SeekStart); err != nil {
		return nil, errs.Wrap(err)
	}
	data, err := io.ReadAll(ln.indx)
	if err != nil {
		return nil, errs.Wrap(err)
	}

	var r rwutils.R
	r.Init(buffer.OfLen(data))

	ln.idx = new(memindex.T)
	memindex.ReadFrom(ln.idx, &r)

	if _, err := r.Done(); err != nil {
		ln.idx = nil
		return nil, errs.Wrap(err)
	}

	return ln.idx, nil
}

func (ln *levelN) Query(hash histdb.Hash, after uint32, cb func(key histdb.Key, val []byte) (bool, error)) (bool, error) {
	if ln.it == nil {
		ln.it = new(leveln.Iterator)
		ln.it.Init(ln.keys, ln.vals)
	}

	var key histdb.Key
	*key.HashPtr() = hash
	key.SetTimestamp(after)

	ln.it.Seek(key)

	for ln.it.Err() == nil {
		if ln.it.Key().Hash() != hash {
			break
		}
		ok, err := cb(ln.it.Key(), ln.it.Value())
		if !ok || err != nil {
			return ok, errs.Combine(err, ln.it.Err())
		}
		if !ln.it.Next() {
			break
		}
	}
	return true, ln.it.Err()
}
