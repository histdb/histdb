package store

import (
	"math/bits"

	"github.com/zeebo/errs/v2"

	"github.com/histdb/histdb/filesystem"
	"github.com/histdb/histdb/memindex"
)

type levelN struct {
	_ [0]func() // no equality

	low  uint32
	high uint32

	fh struct {
		indx filesystem.H
		keys filesystem.H
		vals filesystem.H
	}

	idx memindex.T
}

func newLevelN(fs *filesystem.T, low, high uint32) (ln *levelN, err error) {
	if low >= high {
		return nil, errs.Errorf("invalid range: %d >= %d", low, high)
	}

	defer func() {
		if err != nil {
			_ = ln.Remove()
			ln = nil
		}
	}()

	ln = &levelN{low: low, high: high}

	ln.fh.indx, err = fs.Create(ln.file(filesystem.KindIndx))
	if err != nil {
		return ln, errs.Wrap(err)
	}

	ln.fh.keys, err = fs.Create(ln.file(filesystem.KindKeys))
	if err != nil {
		return ln, errs.Wrap(err)
	}

	ln.fh.vals, err = fs.Create(ln.file(filesystem.KindVals))
	if err != nil {
		return ln, errs.Wrap(err)
	}

	return ln, nil
}

func openLevelN(fs *filesystem.T, low, high uint32) (ln *levelN, err error) {
	if low >= high {
		return nil, errs.Errorf("invalid range: %d >= %d", low, high)
	}

	defer func() {
		if err != nil {
			_ = ln.Close()
			ln = nil
		}
	}()

	// TODO: all of these files should have some suffix block that the
	// serialization code adds and skips during deserialization so that we
	// can do a quick sanity check that it was fully written.

	ln = &levelN{low: low, high: high}

	ln.fh.indx, err = fs.OpenRead(ln.file(filesystem.KindIndx))
	if err != nil {
		return ln, errs.Wrap(err)
	}

	ln.fh.keys, err = fs.OpenRead(ln.file(filesystem.KindKeys))
	if err != nil {
		return ln, errs.Wrap(err)
	}

	ln.fh.vals, err = fs.OpenRead(ln.file(filesystem.KindVals))
	if err != nil {
		return ln, errs.Wrap(err)
	}

	return ln, errs.Wrap(loadMemindex(ln.fh.indx, &ln.idx))
}

func (ln *levelN) file(kind uint8) string {
	return filesystem.File{Low: ln.low, High: ln.high, Kind: kind}.String()
}

func (ln *levelN) all(cb func(h *filesystem.H) error) error {
	return errs.Wrap(errs.Combine(
		cb(&ln.fh.indx),
		cb(&ln.fh.keys),
		cb(&ln.fh.vals),
	))
}

func (ln *levelN) Sync() error   { return ln.all((*filesystem.H).Sync) }
func (ln *levelN) Remove() error { return ln.all((*filesystem.H).Remove) }
func (ln *levelN) Close() error  { return ln.all((*filesystem.H).Close) }
func (ln *levelN) Depth() int    { return depth(ln.low, ln.high) }

func depth(low, high uint32) int { return bits.Len32(high - low) }
