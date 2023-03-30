package atomicdir

import (
	"sort"

	"github.com/zeebo/errs/v2"

	"github.com/histdb/histdb/filesystem"
	"github.com/histdb/histdb/hashtbl"
)

type Transaction struct {
	txn     uint16
	gens    []uint32
	genset  hashtbl.T[hashtbl.U32, *hashtbl.U32, hashtbl.E, *hashtbl.E]
	handles []FileHandle
}

type FileHandle struct {
	File   File
	Handle filesystem.Handle
}

func (tx *Transaction) Handles() []FileHandle { return tx.handles }
func (tx *Transaction) MaxGeneration() uint32 { return tx.gens[0] }

func (tx *Transaction) include(f File, fh filesystem.Handle) {
	if _, ok := tx.genset.Insert(hashtbl.U32(f.Generation), hashtbl.E{}); ok {
		tx.gens = append(tx.gens, f.Generation)
	}
	tx.handles = append(tx.handles, FileHandle{
		File:   f,
		Handle: fh,
	})
}

func (tx *Transaction) sort() {
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

func (tx *Transaction) Close() error {
	var eg errs.Group
	for _, nh := range tx.handles {
		eg.Add(nh.Handle.Close())
	}
	return eg.Err()
}
