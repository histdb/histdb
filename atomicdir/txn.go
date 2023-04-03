package atomicdir

import (
	"sort"

	"github.com/zeebo/errs/v2"

	"github.com/histdb/histdb/filesystem"
)

const (
	KindLevel0       = 0
	KindLevelNKeys   = 1
	KindLevelNValues = 2
	KindMemindex     = 3
)

type Txn struct {
	tid uint32
	fhs []FH
	l0s []FH
	lns []LN
}

type LN struct {
	Keys     FH
	Values   FH
	Memindex FH
}

type FH struct {
	File
	filesystem.Handle
}

func (txn *Txn) MaxGen() uint32 { return txn.fhs[0].Generation }
func (txn *Txn) FHs() []FH      { return txn.fhs }
func (txn *Txn) L0s() []FH      { return txn.l0s }
func (txn *Txn) LNs() []LN      { return txn.lns }

func (txn *Txn) include(f File, fh filesystem.Handle) {
	txn.fhs = append(txn.fhs, FH{
		File:   f,
		Handle: fh,
	})
}

func (txn *Txn) sort() {
	sort.Slice(txn.fhs, func(i, j int) bool {
		// newest data is in later generations and lower levels
		// so we want to sort by largest generation first, then
		// by smallest level.
		switch nhi, nhj := txn.fhs[i], txn.fhs[j]; {
		case nhi.Generation < nhj.Generation:
			return false
		case nhi.Generation > nhj.Generation:
			return true
		case nhi.Kind < nhj.Kind:
			return true
		case nhi.Kind > nhj.Kind:
			return false
		default:
			return false
		}
	})
}

func (txn *Txn) validate() error {
	txn.sort()

	handles := txn.fhs
	gen := uint32(0)

	for len(handles) > 0 && handles[0].Kind == KindLevel0 {
		l0 := handles[0]
		if len(txn.l0s) > 0 && l0.Generation == gen {
			return errs.Errorf("duplicate generation in l0 (how? lol)")
		}
		gen = l0.Generation

		txn.l0s = append(txn.l0s, l0)
		handles = handles[1:]
	}

	if len(txn.l0s) == 0 {
		return errs.Errorf("no level0 found")
	}

	for len(handles) > 0 {
		if len(handles) < 3 {
			return errs.Errorf("leveln is missing some files")
		}

		keys, values, memindex := handles[0], handles[1], handles[2]
		if keys.Generation != values.Generation || values.Generation != memindex.Generation {
			return errs.Errorf("inconsistent generation for leveln")
		} else if keys.Generation == gen {
			return errs.Errorf("duplicate generation in leveln")
		}
		gen = keys.Generation

		txn.lns = append(txn.lns, LN{
			Keys:     keys,
			Values:   values,
			Memindex: memindex,
		})
		handles = handles[3:]
	}

	return nil
}

func (txn *Txn) Close() error {
	var eg errs.Group
	for _, fh := range txn.fhs {
		eg.Add(fh.Handle.Close())
	}
	txn.fhs = nil
	txn.l0s = nil
	txn.lns = nil
	return eg.Err()
}
