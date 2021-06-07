package atomicdir

import (
	"sort"

	"github.com/zeebo/errs/v2"

	"github.com/histdb/histdb/filesystem"
)

type Transaction struct {
	gen     uint32
	txn     uint16
	handles []fileHandle
}

type fileHandle struct {
	File   File
	Handle filesystem.Handle
}

func (tx *Transaction) WAL() fileHandle       { return tx.handles[0] }
func (tx *Transaction) MaxGeneration() uint32 { return tx.gen }

func (tx *Transaction) include(f File, fh filesystem.Handle) {
	if f.Generation > tx.gen {
		tx.gen = f.Generation
	}
	tx.handles = append(tx.handles, fileHandle{
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
		case nhi.File.Level < nhj.File.Level:
			return true
		case nhi.File.Level > nhj.File.Level:
			return false
		default:
			return false
		}
	})
}

func (tx *Transaction) validate() error {
	tx.sort()

	if len(tx.handles) == 0 {
		return errs.Errorf("empty directory")
	}
	if wal := tx.WAL(); wal.File.Level != 0 {
		return errs.Errorf("largest file not level0: %s", wal.File.String())
	}

	return nil
}

func (tx *Transaction) Close() error {
	var eg errs.Group
	for _, nh := range tx.handles {
		eg.Add(nh.Handle.Close())
	}
	return eg.Err()
}
