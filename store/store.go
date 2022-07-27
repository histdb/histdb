package store

import (
	"github.com/histdb/histdb/filesystem"
	"github.com/histdb/histdb/memindex"
)

type T struct {
	fs *filesystem.T
	m  memindex.T
}

func (t *T) Init(fs *filesystem.T) error {
	*t = T{
		fs: fs,
	}

	return nil
}
