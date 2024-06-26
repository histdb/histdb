package store

import (
	"github.com/zeebo/errs/v2"

	"github.com/histdb/histdb"
	"github.com/histdb/histdb/filesystem"
	"github.com/histdb/histdb/leveln"
	"github.com/histdb/histdb/memindex"
	"github.com/histdb/histdb/mergeiter"
	"github.com/histdb/histdb/rwutils"
)

func compact(fs *filesystem.T, lns []*levelN) (_ *levelN, err error) {
	if len(lns) == 0 {
		return nil, errs.Errorf("must compact at least 1 leveln")
	}
	nextLow := lns[0].low

	its := make([]mergeiter.Iterator, len(lns))
	for i := range lns {
		if lns[i].low != nextLow {
			return nil, errs.Errorf("non-contiguous lns: %d != %d", nextLow, lns[i].low)
		}
		nextLow = lns[i].high

		var it leveln.Iterator
		it.Init(lns[i].fh.keys, lns[i].fh.vals)
		its[i] = &it
	}

	ln, err := newLevelN(fs, lns[0].low, nextLow)
	if err != nil {
		return nil, errs.Errorf("unable to create leveln: %w", err)
	}
	defer func() {
		if err != nil {
			_ = ln.Remove()
		}
	}()

	var lnw leveln.Writer
	var mi mergeiter.T
	var name []byte
	var w rwutils.W
	var hash histdb.Hash

	lnw.Init(ln.fh.keys, ln.fh.vals)
	mi.Init(its)

	for mi.Next() {
		// TODO: merge values as they age?
		// TODO: card fix is problematic because it can cause the keys to become
		// unsorted.

		key := mi.Key()
		if key.Hash() != hash {
			name, ok := lns[mi.Iter()].idx.AppendNameByHash(key.Hash(), name[:0])
			if !ok {
				return nil, errs.Errorf("append name failed")
			}
			ln.idx.Add(name, nil, nil)
			hash = key.Hash()
		}

		if err := lnw.Append(key, mi.Value()); err != nil {
			return nil, errs.Errorf("unable to append value: %w", err)
		}
	}
	if err := lnw.Finish(); err != nil {
		return nil, errs.Errorf("unable to finish leveln: %w", err)
	}

	memindex.AppendTo(&ln.idx, &w)
	if _, err := ln.fh.indx.Write(w.Done().Prefix()); err != nil {
		return nil, errs.Errorf("unable to write memindex: %w", err)
	}

	if err := ln.Sync(); err != nil {
		return nil, errs.Errorf("unable to sync leveln: %w", err)
	}

	return ln, nil
}
