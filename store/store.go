package store

import (
	"errors"
	"io"
	"sync"
	"sync/atomic"

	"github.com/zeebo/errs/v2"

	"github.com/histdb/histdb"
	"github.com/histdb/histdb/buffer"
	"github.com/histdb/histdb/card"
	"github.com/histdb/histdb/filesystem"
	"github.com/histdb/histdb/flathist"
	"github.com/histdb/histdb/leveln"
	"github.com/histdb/histdb/memindex"
	"github.com/histdb/histdb/pdqsort"
	"github.com/histdb/histdb/query"
	"github.com/histdb/histdb/rwutils"
)

type Config struct {
	_ [0]func() // no equality

	CardFix *card.Fixer
}

type T struct {
	_ [0]func() // no equality

	cfg Config
	fs  *filesystem.T
	ms  atomic.Pointer[memStore]

	mu   sync.Mutex
	rwmu sync.RWMutex
	lns  []levelN
	gen  uint32
	qst  *flathist.S
}

type memStore struct {
	idx memindex.T
	st  flathist.S
}

func (t *T) Close() (err error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	var eg errs.Group
	for _, ln := range t.lns {
		eg.Add(ln.Close())
	}

	// free up memory
	t.lns = nil
	t.ms.Store(nil)

	return eg.Err()
}

func (t *T) Init(fs *filesystem.T, cfg Config) (err error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	for _, ln := range t.lns {
		_ = ln.Close()
	}
	clear(t.lns)

	t.cfg = cfg
	t.fs = fs
	t.lns = t.lns[:0]
	t.gen = 0
	t.ms.Store(new(memStore))

	fh, err := fs.OpenRead(".")
	if err != nil {
		return errs.Errorf("unable to read store directory: %w", err)
	}
	defer fh.Close()

	type openFile struct {
		f  filesystem.File
		fh filesystem.H
	}

	var files []openFile
	defer func() {
		if err == nil {
			return
		}
		for _, file := range files {
			_ = file.fh.Close()
		}
	}()

	for {
		names, err := fh.Readdirnames(24)
		for _, name := range names {
			f, ok := filesystem.ParseFile(name)
			if !ok {
				continue
			}

			fh, err := fs.OpenRead(name)
			if err != nil {
				return errs.Errorf("unable to open file %q: %w", f, err)
			}

			files = append(files, openFile{f: f, fh: fh})
		}

		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return errs.Errorf("problem reading directory names: %w", err)
		}
	}

	pdqsort.Less(files, func(i, j int) bool {
		return files[i].f.String() < files[j].f.String()
	})

	for len(files) > 0 {
		if len(files) < 3 {
			return errs.Errorf("incomplete leveln files")
		}

		// TODO: check for contiguous non-overlapping range of generations
		// TODO: all of these files should have some suffix block that the
		// serialization code adds and skips during deserialization so that we
		// can do a quick sanity check that it was fully written.

		indx, keys, vals := files[0], files[1], files[2]
		if indx.f.Low != keys.f.Low || indx.f.Low != vals.f.Low {
			return errs.Errorf("leveln files have different low")
		} else if indx.f.High != keys.f.High || indx.f.High != vals.f.High {
			return errs.Errorf("leveln files have different high")
		} else if indx.f.Kind != filesystem.KindIndx {
			return errs.Errorf("leveln missing index")
		} else if keys.f.Kind != filesystem.KindKeys {
			return errs.Errorf("leveln missing keys")
		} else if vals.f.Kind != filesystem.KindVals {
			return errs.Errorf("leveln missing values")
		}

		t.lns = append(t.lns, levelN{
			low:  indx.f.Low,
			high: indx.f.High,
			indx: indx.fh,
			keys: keys.fh,
			vals: vals.fh,
		})

		t.gen = indx.f.High + 1
	}

	return nil
}

func (t *T) Query(q *query.Q, after uint32, cb func(key histdb.Key, name []byte, st *flathist.S, h flathist.H) bool) (bool, error) {
	t.mu.Lock()

	t.rwmu.RLock()
	defer t.rwmu.RUnlock()

	lns := t.lns
	if t.qst == nil || t.qst.Count() > 1000 {
		t.qst = new(flathist.S)
	}

	t.mu.Unlock()

	var name []byte

	for _, ln := range lns {
		// TODO: this could all be done in parallel
		idx, err := ln.Index()
		if err != nil {
			return false, errs.Wrap(err)
		}

		ok := memindex.Iter(q.Eval(idx), func(id memindex.Id) bool {
			hash, ok := idx.GetHashById(id)
			if !ok {
				return false
			}
			name, ok := idx.AppendNameById(id, name[:0])
			if !ok {
				return false
			}

			ok, err = ln.Query(hash, after, func(key histdb.Key, val []byte) (bool, error) {
				var r rwutils.R
				r.Init(buffer.OfLen(val))

				h := t.qst.New()
				flathist.ReadFrom(t.qst, h, &r)
				if _, err := r.Done(); err != nil {
					return false, err
				}

				return cb(key, name, t.qst, h), nil
			})
			return ok && err == nil
		})
		if !ok || err != nil {
			return ok, err
		}
	}

	return true, nil
}

func (t *T) Latest(q *query.Q, cb func(name []byte, st *flathist.S, h flathist.H) bool) bool {
	ms := t.ms.Load()

	var name []byte
	return memindex.Iter(q.Eval(&ms.idx), func(id memindex.Id) (ok bool) {
		name, ok = ms.idx.AppendNameById(id, name[:0])
		return ok && cb(name, &ms.st, flathist.UnsafeRawH(id+1))
	})
}

func (t *T) Observe(metric []byte, val float32) {
	ms := t.ms.Load()

	var h flathist.H
	_, id, _, ok := ms.idx.Add(metric, nil, t.cfg.CardFix)
	if ok {
		h = ms.st.New()
	} else {
		h = flathist.UnsafeRawH(id + 1)
	}
	ms.st.Observe(h, val)
}

func (t *T) WriteLevel(ts, dur uint32) (err error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	ms := t.ms.Load()
	if !t.ms.CompareAndSwap(ms, new(memStore)) {
		return errs.Errorf("impossible compare and swap failed")
	}
	ms.st.Finalize()

	type idHash struct {
		id   memindex.Id
		hash histdb.Hash
	}

	metrics := make([]idHash, 0, ms.idx.Cardinality())
	if !ms.idx.Iterate(func(id memindex.Id) bool {
		hash, ok := ms.idx.GetHashById(id)
		metrics = append(metrics, idHash{id: id, hash: hash})
		return ok
	}) {
		return errs.Errorf("memindex inconsistent")
	}
	pdqsort.Less(metrics, func(i, j int) bool {
		return string(metrics[i].hash[:]) < string(metrics[j].hash[:])
	})

	ln, err := newLevelN(t.fs, t.gen, t.gen)
	if err != nil {
		return errs.Errorf("unable to create leveln: %w", err)
	}
	defer func() {
		if err != nil {
			_ = ln.Remove()
		}
	}()

	var lnw leveln.Writer
	var w rwutils.W
	var key histdb.Key

	lnw.Init(ln.keys, ln.vals)
	key.SetTimestamp(ts)
	key.SetDuration(dur)

	for _, metric := range metrics {
		*key.HashPtr() = metric.hash
		w.Reset()
		flathist.AppendTo(&ms.st, flathist.UnsafeRawH(metric.id+1), &w)
		if err := lnw.Append(key, w.Done().Prefix()); err != nil {
			return errs.Errorf("unable to append value: %w", err)
		}
	}
	if err := lnw.Finish(); err != nil {
		return errs.Errorf("unable to finish leveln: %w", err)
	}

	w.Reset()
	memindex.AppendTo(&ms.idx, &w)
	if _, err := ln.indx.Write(w.Done().Prefix()); err != nil {
		return errs.Errorf("unable to write memindex: %w", err)
	}

	if err := ln.Sync(); err != nil {
		return errs.Errorf("unable to sync leveln: %w", err)
	}

	t.lns = append(t.lns, ln)
	t.gen++

	return nil
}
