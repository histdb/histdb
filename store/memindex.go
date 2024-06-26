package store

import (
	"io"

	"github.com/zeebo/errs/v2"

	"github.com/histdb/histdb/buffer"
	"github.com/histdb/histdb/filesystem"
	"github.com/histdb/histdb/memindex"
	"github.com/histdb/histdb/rwutils"
)

func loadMemindex(fh filesystem.H, idx *memindex.T) error {
	if _, err := fh.Seek(0, io.SeekStart); err != nil {
		return errs.Wrap(err)
	}
	data, err := io.ReadAll(fh)
	if err != nil {
		return errs.Wrap(err)
	}

	var r rwutils.R
	r.Init(buffer.OfLen(data))

	memindex.ReadFrom(idx, &r)

	if _, err := r.Done(); err != nil {
		return errs.Wrap(err)
	}

	return nil
}
