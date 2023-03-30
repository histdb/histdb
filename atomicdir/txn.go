package atomicdir

import (
	"sort"

	"github.com/zeebo/errs/v2"

	"github.com/histdb/histdb/filesystem"
)

type Txn struct {
	tid     uint32
	handles []FH
}

type FH struct {
	File   File
	Handle filesystem.Handle
}

func (tx *Txn) Handles() []FH         { return tx.handles }
func (tx *Txn) MaxGeneration() uint32 { return tx.handles[0].File.Generation }

func (tx *Txn) include(f File, fh filesystem.Handle) {
	tx.handles = append(tx.handles, FH{
		File:   f,
		Handle: fh,
	})
}

func (tx *Txn) sort() {
	sort.Slice(tx.handles, func(i, j int) bool {
		// newest data is in later generations and lower levels
		// so we want to sort by largest generation first, then
		// by smallest level.
		switch nhi, nhj := tx.handles[i], tx.handles[j]; {
		case nhi.File.Generation < nhj.File.Generation:
			return false
		case nhi.File.Generation > nhj.File.Generation:
			return true
		case nhi.File.Kind < nhj.File.Kind:
			return true
		case nhi.File.Kind > nhj.File.Kind:
			return false
		default:
			return false
		}
	})
}

func (tx *Txn) Close() error {
	var eg errs.Group
	for _, fh := range tx.handles {
		eg.Add(fh.Handle.Close())
	}
	tx.handles = nil
	return eg.Err()
}
