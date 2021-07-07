package leveln

import (
	"github.com/zeebo/errs/v2"

	"github.com/histdb/histdb"
	"github.com/histdb/histdb/filesystem"
)

type Writer struct {
	err   error
	key   histdb.Key
	first bool
	kw    keyWriter
	vw    valueWriter
}

func (w *Writer) Init(keys, values filesystem.Handle) {
	w.err = nil
	w.key = histdb.Key{}
	w.first = true
	w.kw.Init(keys)
	w.vw.Init(values)
}

func (w *Writer) storeErr(err error) error {
	w.err = errs.Wrap(err)
	return w.err
}

func (w *Writer) Append(key histdb.Key, value []byte) error {
	if w.err != nil {
		return w.err
	}

	// if not first, we may either append or finish an old span
	if !w.first {
		if key.Hash() == w.key.Hash() {
			if buf := w.vw.CanAppend(value); buf != nil {
				w.vw.Append(buf, key.Timestamp(), value)
				return nil
			}
		}

		// either mismatch or span full, so finish and record offset
		offset, length, err := w.vw.FinishSpan()
		if err != nil {
			return w.storeErr(err)
		}

		var ent kwEntry
		ent.Set(w.key, offset, length)

		if w.kw.CanAppendFast() {
			w.kw.AppendFast(ent)
		} else if err := w.kw.AppendSlow(ent); err != nil {
			return w.storeErr(err)
		}
	} else {
		// no longer first
		w.first = false
	}

	// start the new span, and since it's new, the value must fit
	w.vw.BeginSpan(key)
	w.key = key

	if buf := w.vw.CanAppend(value); buf != nil {
		w.vw.Append(buf, key.Timestamp(), value)
		return nil
	}
	return w.storeErr(errs.Errorf("value too large"))
}

func (w *Writer) Finish() error {
	if w.err != nil {
		return w.err
	}

	if !w.first {
		offset, length, err := w.vw.FinishSpan()
		if err != nil {
			return w.storeErr(err)
		}

		var ent kwEntry
		ent.Set(w.key, offset, length)

		if w.kw.CanAppendFast() {
			w.kw.AppendFast(ent)
		} else if err := w.kw.AppendSlow(ent); err != nil {
			return w.storeErr(err)
		}
	}
	if err := w.kw.Finish(); err != nil {
		return w.storeErr(err)
	}
	return nil
}
