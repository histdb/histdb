package store

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/zeebo/errs/v2"

	"github.com/histdb/histdb"
	"github.com/histdb/histdb/buffer"
	"github.com/histdb/histdb/card"
	"github.com/histdb/histdb/filesystem"
	"github.com/histdb/histdb/flathist"
	"github.com/histdb/histdb/hashtbl"
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
	ms  atomic.Pointer[MemStore]

	imu sync.Mutex   // protects ms.idx
	wmu sync.Mutex   // protects WriteLevel
	cmu sync.Mutex   // protects Compact
	lmu sync.Mutex   // protects access to lns/gen/qst
	qmu sync.RWMutex // protects Query

	lns []*levelN
	qst *flathist.S
}

type MemStore struct {
	I memindex.T
	S flathist.S
}

func (t *T) DebugMemStore() *MemStore { return t.ms.Load() }

// Close cannot be called concurrently with any other method.
func (t *T) Close() (err error) {
	var eg errs.Group

	for _, ln := range t.lns {
		eg.Add(ln.Close())
	}

	// free up memory
	t.lns = nil
	t.ms.Store(nil)
	t.qst = nil

	return eg.Err()
}

// Init cannot be called concurrently with any other method.
func (t *T) Init(fs *filesystem.T, cfg Config) (err error) {
	for _, ln := range t.lns {
		_ = ln.Close()
	}
	clear(t.lns)

	t.cfg = cfg
	t.fs = fs
	t.lns = t.lns[:0]
	t.ms.Store(new(MemStore))

	fh, err := fs.OpenRead(".")
	if err != nil {
		return errs.Errorf("unable to read store directory: %w", err)
	}
	defer fh.Close()

	var files []filesystem.File

	for {
		names, err := fh.Readdirnames(24)
		for _, name := range names {
			file, ok := filesystem.ParseFile(name)
			if !ok {
				continue
			}
			files = append(files, file)
		}

		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return errs.Errorf("problem reading directory names: %w", err)
		}
	}

	pdqsort.Less(files, func(i, j int) bool {
		return files[i].String() < files[j].String()
	})

	var nlow uint32
	for len(files) > 0 {
		if len(files) < 3 {
			return errs.Errorf("incomplete leveln files")
		}

		indx, keys, vals := files[0], files[1], files[2]
		if indx.Low != keys.Low || indx.Low != vals.Low {
			return errs.Errorf("leveln files have different low")
		} else if indx.High != keys.High || indx.High != vals.High {
			return errs.Errorf("leveln files have different high")
		} else if indx.High <= indx.Low {
			return errs.Errorf("leveln files have invalid range: %d <= %d", indx.High, indx.Low)
		} else if indx.Kind != filesystem.KindIndx {
			return errs.Errorf("leveln missing index")
		} else if keys.Kind != filesystem.KindKeys {
			return errs.Errorf("leveln missing keys")
		} else if vals.Kind != filesystem.KindVals {
			return errs.Errorf("leveln missing values")
		} else if indx.Low != nlow {
			return errs.Errorf("invalid next low gen: expect %d to be %d", indx.Low, nlow)
		}
		nlow = indx.High

		ln, err := openLevelN(fs, indx.Low, indx.High)
		if err != nil {
			return errs.Wrap(err)
		}
		t.lns = append(t.lns, ln)

		files = files[3:]
	}

	return nil
}

func (t *T) QueryMetrics(q *query.Q, cb func(hash histdb.Hash, name []byte) bool) bool {
	t.qmu.RLock()
	defer t.qmu.RUnlock()

	t.lmu.Lock()

	// SAFETY: t.lns is only either appended to in WriteLevel or fully replaced
	// in CompactSuffix, so taking a shallow snapshot of the slice is safe.
	lns := t.lns

	t.lmu.Unlock()

	var set hashtbl.T[histdb.Hash, int]

	for i, ln := range lns {
		ok := memindex.Iter(q.Eval(&ln.idx), func(id memindex.Id) bool {
			hash, ok := ln.idx.GetHashById(id)
			if !ok {
				return false
			}
			set.Insert(hash, i)
			return true
		})
		if !ok {
			return false
		}
	}

	var name []byte
	return set.Iterate(func(k histdb.Hash, v int) bool {
		ln := lns[v]
		name, ok := ln.idx.AppendNameByHash(k, name[:0])
		if !ok {
			return false
		}
		if !cb(k, name) {
			return false
		}
		return true
	})
}

func (t *T) QueryData(q *query.Q, after uint32, cb func(key histdb.Key, name []byte, st *flathist.S, h flathist.H) bool) (bool, error) {
	t.qmu.RLock()
	defer t.qmu.RUnlock()

	t.lmu.Lock()

	// SAFETY: t.lns is only either appended to in WriteLevel or fully replaced
	// in CompactSuffix, so taking a shallow snapshot of the slice is safe.
	lns := t.lns

	if t.qst == nil || t.qst.Count() > 8000 {
		t.qst = new(flathist.S)
	}
	h := t.qst.New()

	t.lmu.Unlock()

	var name []byte
	var err error
	var it leveln.Iterator

	for _, ln := range lns {
		// TODO: this could all be done in parallel

		it.Init(ln.fh.keys, ln.fh.vals)

		ok := memindex.Iter(q.Eval(&ln.idx), func(id memindex.Id) bool {
			hash, ok := ln.idx.GetHashById(id)
			if !ok {
				return false
			}
			name, ok = ln.idx.AppendNameById(id, name[:0])
			if !ok {
				return false
			}

			var key histdb.Key
			*key.HashPtr() = hash
			key.SetTimestamp(after)

			// this maybe skips seeks but is maybe an invalid optimization have to think about it.
			// if something goes wrong, try removing this if statement first lol.
			if k := it.Key(); string(k[:]) < string(key[:]) {
				it.Seek(key)
			}

			for it.Err() == nil {
				if it.Key().Hash() != hash {
					break
				}

				var r rwutils.R
				r.Init(buffer.OfLen(it.Value()))

				t.qst.Reset(h)
				flathist.ReadFrom(t.qst, h, &r)
				if _, err = r.Done(); err != nil {
					return false
				}

				if !cb(key, name, t.qst, h) {
					return false
				}

				if !it.Next() {
					break
				}
			}
			err = it.Err()

			return ok && err == nil
		})
		if !ok || err != nil {
			return ok, err
		}
	}

	return true, nil
}

func (t *T) Observe(metric []byte, val float32) {
	ms := t.ms.Load()
	if ms == nil {
		return
	}

	// TODO: this mutex is going to hurt scalability of writes in the case that
	// we're not adding a new value to the index but i'm not sure of how to
	// avoid it because in the case there is a new value it has to coodinate
	// with all of the people not adding new values in some way that causes
	// those to not contend with each other. meh. maybe the index can
	// periodically publish a read-only set of hash->id pairs?
	t.imu.Lock()
	_, id, _, ok := ms.I.Add(metric, nil, t.cfg.CardFix)
	t.imu.Unlock()

	var h flathist.H
	if ok {
		h = ms.S.New()
	} else {
		h = flathist.UnsafeRawH(id + 1)
	}

	ms.S.Observe(h, val)
}

func (t *T) WriteLevel(ts, dur uint32) (err error) {
	t.wmu.Lock()
	defer t.wmu.Unlock()

	ms := t.ms.Load()
	if ms == nil {
		return errs.Errorf("memstore is nil (store closed or not initialized)")
	}
	if !t.ms.CompareAndSwap(ms, new(MemStore)) {
		return errs.Errorf("impossible compare and swap failed")
	}
	ms.S.Finalize()

	type idHash struct {
		id   memindex.Id
		hash histdb.Hash
	}

	metrics := make([]idHash, 0, ms.I.Cardinality())
	if !ms.I.Iterate(func(id memindex.Id) bool {
		hash, ok := ms.I.GetHashById(id)
		metrics = append(metrics, idHash{id: id, hash: hash})
		return ok
	}) {
		return errs.Errorf("memindex inconsistent")
	}
	pdqsort.Less(metrics, func(i, j int) bool {
		return string(metrics[i].hash[:]) < string(metrics[j].hash[:])
	})

	var gen uint32

	// SAFETY: the largest generation is the high of the last leveln, if it
	// exists, and 0 otherwise. it's possible that t.lns is modified immediately
	// after this mutex by CompactSuffix, but compaction does not change the max
	// generation: the only function that modifies the last entry's high is
	// WriteLevel, and it has a mutex.
	t.lmu.Lock()
	if len(t.lns) > 0 {
		gen = t.lns[len(t.lns)-1].high
	}
	t.lmu.Unlock()

	ln, err := newLevelN(t.fs, gen, gen+1)
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
	var name []byte
	var ok bool

	lnw.Init(ln.fh.keys, ln.fh.vals)
	key.SetTimestamp(ts)
	key.SetDuration(dur)

	// TODO: we should reinsert into a new memindex with the sorted values
	// so that the iteration order is good for leveln

	for _, metric := range metrics {
		name, ok = ms.I.AppendNameById(metric.id, name[:0])
		if !ok {
			return errs.Errorf("unable to append name")
		}
		*key.HashPtr() = metric.hash

		w.Reset()
		flathist.AppendTo(&ms.S, flathist.UnsafeRawH(metric.id+1), &w)
		if err := lnw.Append(key, w.Done().Prefix()); err != nil {
			return errs.Errorf("unable to append value: %w", err)
		}

		_, _, _, ok := ln.idx.Add(name, nil, t.cfg.CardFix)
		if !ok {
			return errs.Errorf("did not create new memindex entry")
		}
	}
	if err := lnw.Finish(); err != nil {
		return errs.Errorf("unable to finish leveln: %w", err)
	}

	w.Reset()
	memindex.AppendTo(&ln.idx, &w)
	if _, err := ln.fh.indx.Write(w.Done().Prefix()); err != nil {
		return errs.Errorf("unable to write memindex: %w", err)
	}

	if err := ln.Sync(); err != nil {
		return errs.Errorf("unable to sync leveln: %w", err)
	}

	// SAFETY: other functions assume that WriteLevel will only ever append to
	// the end and not modify the slice in other ways.
	t.lmu.Lock()
	t.lns = append(t.lns, ln)
	t.lmu.Unlock()

	return nil
}

func (t *T) CompactSuffix() (err error) {
	t.cmu.Lock()
	defer t.cmu.Unlock()

	// SAFETY: t.lns is only either appended to in WriteLevel or fully replaced
	// in CompactSuffix, so taking a shallow snapshot of the slice is safe.
	t.lmu.Lock()
	lns := t.lns
	t.lmu.Unlock()

	if len(lns) == 0 {
		return nil
	}

	high, j := lns[len(lns)-1].high, len(lns)-1
	for i := len(lns) - 2; i >= 0; i-- {
		if depth(lns[i].low, high) == lns[i].Depth() {
			break
		}
		j = i
	}

	// we need at least 2 levels to compact
	clns := lns[j:]
	if len(clns) < 2 {
		return nil
	}

	ln, err := compact(t.fs, clns)
	if err != nil {
		return errs.Errorf("unable to compact: %w", err)
	}

	t.lmu.Lock()

	var nlns []*levelN
	nlns = append(nlns, t.lns[:j]...)        // the uncompacted files
	nlns = append(nlns, ln)                  // the new file
	nlns = append(nlns, t.lns[len(lns):]...) // files written during compact
	t.lns = nlns

	t.lmu.Unlock()

	// grab the query write lock temporarily so we know that no queries can
	// possibly be running holding on to any files compacted away.
	t.qmu.Lock()
	_ = 0 // staticcheck incorrectly complains about empty critical section
	t.qmu.Unlock()

	for _, ln := range clns {
		_ = ln.Remove()
	}

	return nil
}

func stringLevel(ln *levelN) string {
	return fmt.Sprintf("(ln %d %d %d)", ln.low, ln.high, ln.Depth())
}

func stringLevels(lns []*levelN) string {
	var out strings.Builder
	for i, ln := range lns {
		if i > 0 {
			out.WriteByte(' ')
		}
		out.WriteString(stringLevel(ln))
	}
	return out.String()
}
