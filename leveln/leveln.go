package leveln

import (
	"github.com/zeebo/errs"
	"github.com/zeebo/lsm"
	"github.com/zeebo/lsm/filesystem"
)

type T struct {
	err   error
	key   lsm.Key
	first bool
	kw    keyWriter
	vw    valueWriter
}

func (t *T) Init(keys, values filesystem.File) {
	t.err = nil
	t.key = lsm.Key{}
	t.first = true
	t.kw.Init(keys)
	t.vw.Init(values)
}

func (t *T) storeErr(err error) error {
	t.err = errs.Wrap(err)
	return t.err
}

func (t *T) Append(key lsm.Key, value []byte) error {
	if t.err != nil {
		return t.err
	}
	if t.first {
		t.vw.BeginSpan(key)
		t.key = key
		t.first = false
	}
	if key.Hash() == t.key.Hash() {
		if buf := t.vw.CanAppend(value); buf != nil {
			t.vw.Append(buf, key, value)
			return nil
		}
	}
	if !t.first {
		offset, err := t.vw.FinishSpan()
		if err != nil {
			return t.storeErr(err)
		}
		if err := t.kw.Append(kwEncode(t.key, offset)); err != nil {
			return t.storeErr(err)
		}
	}
	t.vw.BeginSpan(key)
	t.key = key
	if buf := t.vw.CanAppend(value); buf != nil {
		t.vw.Append(buf, key, value)
		return nil
	}
	return t.storeErr(errs.New("value too large"))
}

func (t *T) Finish() error {
	if t.err != nil {
		return t.err
	}
	if !t.first {
		offset, err := t.vw.FinishSpan()
		if err != nil {
			return t.storeErr(err)
		}
		if err := t.kw.Append(kwEncode(t.key, offset)); err != nil {
			return t.storeErr(err)
		}
	}
	if err := t.kw.Finish(); err != nil {
		return t.storeErr(err)
	}
	return nil
}
