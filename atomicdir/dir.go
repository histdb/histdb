package atomicdir

import (
	"sort"

	"github.com/zeebo/errs/v2"

	"github.com/histdb/histdb/filesystem"
)

const (
	KindLevel0       = 0
	KindLevelNKeys   = 1
	KindLevelNValues = 2
	KindMemindex     = 3
)

type Dir struct {
	_ [0]func() // no equality

	id uint32

	fhs []FH
	l0s []FH
	lns []LN
}

type LN struct {
	_ [0]func() // no equality

	Generation uint32
	Level      uint8

	Keys     FH
	Values   FH
	Memindex FH
}

type FH struct {
	_ [0]func() // no equality

	File

	filesystem.Handle
}

func (d *Dir) MaxGen() uint32  { return d.fhs[0].Generation }
func (d *Dir) MaxLevel() uint8 { return d.fhs[0].Level }

func (d *Dir) FHs() []FH { return d.fhs }
func (d *Dir) L0s() []FH { return d.l0s }
func (d *Dir) LNs() []LN { return d.lns }

func (d *Dir) reset(id uint32) {
	*d = Dir{
		id:  id,
		fhs: d.fhs[:0],
		l0s: d.l0s[:0],
		lns: d.lns[:0],
	}
}

func (d *Dir) include(f File, fh filesystem.Handle) {
	d.fhs = append(d.fhs, FH{
		File:   f,
		Handle: fh,
	})
}

func (d *Dir) sort() {
	sort.Slice(d.fhs, func(i, j int) bool {
		// newest data is in later generations and lower levels
		// so we want to sort by largest generation first, then
		// by smallest level, then by smallest kind.
		switch nhi, nhj := d.fhs[i], d.fhs[j]; {
		case nhi.Generation < nhj.Generation:
			return false
		case nhi.Generation > nhj.Generation:
			return true

		case nhi.Level < nhj.Level:
			return true
		case nhi.Level > nhj.Level:
			return false

		case nhi.Kind < nhj.Kind:
			return true
		case nhi.Kind > nhj.Kind:
			return false

		default:
			return false
		}
	})
}

func (d *Dir) validate() error {
	d.sort()

	handles := d.fhs
	gen := uint32(0)

	for len(handles) > 0 && handles[0].Kind == KindLevel0 {
		l0 := handles[0]
		if len(d.l0s) > 0 && l0.Generation == gen {
			return errs.Errorf("duplicate generation in l0 (how? lol)")
		}
		gen = l0.Generation

		d.l0s = append(d.l0s, l0)
		handles = handles[1:]
	}

	for len(handles) > 0 {
		if len(handles) < 3 {
			return errs.Errorf("leveln is missing some files")
		}

		keys, values, memindex := handles[0], handles[1], handles[2]
		if keys.Generation != values.Generation || values.Generation != memindex.Generation {
			return errs.Errorf("inconsistent generation for leveln")
		} else if keys.Level != values.Level || values.Level != memindex.Level {
			return errs.Errorf("inconsistent levels for leveln")
		} else if keys.Generation == gen {
			return errs.Errorf("duplicate generation in leveln")
		}
		gen = keys.Generation

		d.lns = append(d.lns, LN{
			Generation: keys.Generation,
			Level:      keys.Level,

			Keys:     keys,
			Values:   values,
			Memindex: memindex,
		})
		handles = handles[3:]
	}

	return nil
}

func (d *Dir) Close() error {
	var eg errs.Group
	for _, fh := range d.fhs {
		eg.Add(fh.Handle.Close())
	}

	clear(d.fhs)
	clear(d.l0s)
	clear(d.lns)

	d.fhs = d.fhs[:0]
	d.l0s = d.l0s[:0]
	d.lns = d.lns[:0]

	return eg.Err()
}
