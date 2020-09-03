package leveln

import (
	"github.com/zeebo/errs"
	"github.com/zeebo/lsm"
	"github.com/zeebo/lsm/filesystem"
)

type Writer struct {
	err   error
	key   lsm.Key
	first bool
	kw    keyWriter
	vw    valueWriter
}

func (w *Writer) Init(keys, values filesystem.File) {
	w.err = nil
	w.key = lsm.Key{}
	w.first = true
	w.kw.Init(keys)
	w.vw.Init(values)
}

func (w *Writer) storeErr(err error) error {
	w.err = errs.Wrap(err)
	return w.err
}

func (w *Writer) Append(key lsm.Key, value []byte) error {
	if w.err != nil {
		return w.err
	}
	if w.first {
		w.vw.BeginSpan(key)
		w.key = key
		w.first = false
	}
	if key.Hash() == w.key.Hash() {
		if buf := w.vw.CanAppend(value); buf != nil {
			w.vw.Append(buf, key, value)
			return nil
		}
	}
	if !w.first {
		offset, err := w.vw.FinishSpan()
		if err != nil {
			return w.storeErr(err)
		}
		if err := w.kw.Append(kwEncode(w.key, offset)); err != nil {
			return w.storeErr(err)
		}
	}
	w.vw.BeginSpan(key)
	w.key = key
	if buf := w.vw.CanAppend(value); buf != nil {
		w.vw.Append(buf, key, value)
		return nil
	}
	return w.storeErr(errs.New("value too large"))
}

func (w *Writer) Finish() error {
	if w.err != nil {
		return w.err
	}
	if !w.first {
		offset, err := w.vw.FinishSpan()
		if err != nil {
			return w.storeErr(err)
		}
		if err := w.kw.Append(kwEncode(w.key, offset)); err != nil {
			return w.storeErr(err)
		}
	}
	if err := w.kw.Finish(); err != nil {
		return w.storeErr(err)
	}
	return nil
}
