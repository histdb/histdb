package mergeiter

import (
	"bytes"
	"math/bits"

	"github.com/histdb/histdb"
)

type Iterator interface {
	Next() bool
	Key() histdb.Key
	Name() []byte
	Value() []byte
	Err() error
}

type T struct {
	_ [0]func() // no equality

	iters []Iterator
	trn   []int
	win   int
	first bool
	err   error
}

func compare(i, j histdb.Key) int { return bytes.Compare(i[:], j[:]) }

func (m *T) Init(iters []Iterator) {
	leaves := 1 << uint(bits.Len(uint(len(iters)-1)))
	trn := append(m.trn[:0], make([]int, leaves-1)...)
	wins := make([]int, 2*leaves-1)

	for i := range wins {
		wins[i] = i

		if uint(i) < uint(len(iters)) {
			if iter := iters[i]; !iter.Next() {
				if m.err = iter.Err(); m.err != nil {
					return
				}
				iters[i] = nil
			}
		}
	}

	for i := range trn {
		l, r := wins[2*i], wins[2*i+1]

		if uint(l) >= uint(len(iters)) || iters[l] == nil {
			goto noSwap
		} else if uint(r) >= uint(len(iters)) || iters[r] == nil {
			// swap
		} else if cmp := compare(iters[l].Key(), iters[r].Key()); cmp < 0 || (cmp == 0 && l < r) {
			// swap
		} else {
			goto noSwap
		}

		r, l = l, r

	noSwap:
		trn[i] = l
		wins[leaves+i] = r
	}

	*m = T{
		iters: iters,
		trn:   trn,
		win:   wins[len(wins)-1],
	}
}

func (m *T) Err() error { return m.err }

func (m *T) Key() (k histdb.Key) {
	if uint(m.win) < uint(len(m.iters)) {
		k = m.iters[m.win].Key()
	}
	return
}

func (m *T) Name() (v []byte) {
	if uint(m.win) < uint(len(m.iters)) {
		v = m.iters[m.win].Name()
	}
	return
}

func (m *T) Value() (v []byte) {
	if uint(m.win) < uint(len(m.iters)) {
		v = m.iters[m.win].Value()
	}
	return
}

func (m *T) Next() bool {
	if m.err != nil {
		return false
	}

	iters, trn, win := m.iters, m.trn, m.win

	if uint(win) >= uint(len(iters)) || iters[win] == nil {
		return false
	}

	if !m.first {
		m.first = true
		return true
	}

	var wkey histdb.Key

	if iter := iters[win]; !iter.Next() {
		if m.err = iter.Err(); m.err != nil {
			return false
		}
		iters[win] = nil
	} else {
		wkey = iter.Key()
	}

	offset := (len(trn) + 1) / 2
	for idx := win / 2; uint(idx) < uint(len(trn)); idx = offset + idx/2 {
		var ckey histdb.Key
		chal := trn[idx]

		if uint(chal) >= uint(len(iters)) || iters[chal] == nil {
			goto noSwap
		} else if ckey = iters[chal].Key(); uint(win) >= uint(len(iters)) || iters[win] == nil {
			// swap
		} else if cmp := compare(ckey, wkey); cmp == -1 || (cmp == 0 && chal < win) {
			// swap
		} else {
			goto noSwap
		}

		trn[idx], win, wkey = win, chal, ckey

	noSwap:
	}

	m.win = win

	if uint(win) >= uint(len(iters)) || iters[win] == nil {
		return false
	}

	return true
}
