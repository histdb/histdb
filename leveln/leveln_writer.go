package leveln

import (
	"github.com/zeebo/errs/v2"

	"github.com/histdb/histdb"
	"github.com/histdb/histdb/filesystem"
)

// TODO: this api kinda sucks because names were bolted on. i think we want to
// have the writer auto-add/encode names into a memindex, but that makes testing
// super hard because the keys have to come in sorted order for it to be correct
// and they are hashes of the name. right now, we assume the caller has a memindex
// that it is encoding the names through, which is a bit weird.

type Writer struct {
	_ [0]func() // no equality

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

func (w *Writer) Append(key histdb.Key, name, value []byte) error {
	if w.err != nil {
		return w.err
	} else if len(name) >= 256 {
		return w.storeErr(errs.Errorf("name too large: %d", len(name)))
	}

	// if not first, we may either append or finish an old span
	if !w.first {
		if key.Hash() == w.key.Hash() {
			if buf := w.vw.CanAppend(value); buf != nil {
				w.vw.Append(buf, key.Timestamp(), value)
				return nil
			}
		}

		// either mismatch or span full, so finish and record voff
		voff, vlen, err := w.vw.FinishSpan()
		if err != nil {
			return w.storeErr(err)
		}

		var ent kwEntry
		ent.Set(w.key, voff, vlen)

		if err := w.kw.Append(ent); err != nil {
			return w.storeErr(err)
		}
	} else {
		// no longer first
		w.first = false
	}

	// start the new span, and since it's new, the value must fit
	if !w.vw.BeginSpan(key, name) {
		return w.storeErr(errs.Errorf("name too large: %d", len(name)))
	}
	w.key = key

	if buf := w.vw.CanAppend(value); buf != nil {
		w.vw.Append(buf, key.Timestamp(), value)
		return nil
	}

	return w.storeErr(errs.Errorf("value too large: %d", len(value)))
}

func (w *Writer) Finish() error {
	if w.err != nil {
		return w.err
	}

	if !w.first {
		voff, vlen, err := w.vw.FinishSpan()
		if err != nil {
			return w.storeErr(err)
		}

		var ent kwEntry
		ent.Set(w.key, voff, vlen)

		if err := w.kw.Append(ent); err != nil {
			return w.storeErr(err)
		}
	}
	if err := w.kw.Finish(); err != nil {
		return w.storeErr(err)
	}
	return nil
}
