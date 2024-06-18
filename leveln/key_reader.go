package leveln

import (
	"github.com/zeebo/errs/v2"

	"github.com/histdb/histdb"
	"github.com/histdb/histdb/filesystem"
)

type krPage struct {
	_ [0]func() // no equality

	page *kwPage
	id   uint32
}

type keyReader struct {
	_ [0]func() // no equality

	stats struct {
		reads int64
	}

	fh    filesystem.H
	root  uint32
	cache []krPage
}

func (k *keyReader) Init(fh filesystem.H) {
	k.stats.reads = 0
	k.fh = fh
	k.root = ^uint32(0)
	k.cache = k.cache[:0]
}

func (k *keyReader) cachePage(depth uint, id uint32) (*kwPage, error) {
	// the root is always depth 0, so we ignore the id and load the root page
	if depth == 0 && k.root == ^uint32(0) {
		size, err := k.fh.Size()
		if err != nil {
			return nil, errs.Wrap(err)
		}
		k.root = uint32(size/kwPageSize) - 1
		id = k.root
	}

	p := krPage{id: id}
	if depth < uint(len(k.cache)) {
		p.page = k.cache[depth].page
	} else {
		p.page = new(kwPage)
	}

	k.stats.reads++
	_, err := k.fh.ReadAt(p.page.Buf()[:], int64(id)*kwPageSize)
	if err != nil {
		p.page = nil
	}

	if depth < uint(len(k.cache)) {
		k.cache[depth] = p
	} else {
		k.cache = append(k.cache, p)
	}

	return p.page, err
}

func (k *keyReader) Search(key histdb.Key) (ent kwEntry, ok bool, err error) {
	keyp := be.Uint64(key[0:8])
	id := k.root

	for depth := uint(0); ; depth++ {
		var page *kwPage
		if depth < uint(len(k.cache)) && k.cache[depth].id == id {
			page = k.cache[depth].page
		} else {
			page, err = k.cachePage(depth, id)
			if err != nil {
				return kwEntry{}, false, err
			}
		}

		count := page.hdr.Count()
		i, j := -1, int(count)
		for j-i > 1 {
			h := int(uint(i+j) >> 1)
			if uint(h) >= uint(len(page.ents)) {
				goto corrupt
			}
			ent := &page.ents[h]

			if keyhp := be.Uint64(ent[0:8]); keyhp < keyp {
				i = h
			} else if keyhp > keyp {
				j = h
			} else if string(key[:]) >= string(ent.Key()[:]) {
				i = h
			} else {
				j = h
			}
		}

		if page.hdr.Leaf() {
			if i == -1 {
				prev := page.hdr.Prev()
				if prev == ^uint32(0) {
					return kwEntry{}, false, nil
				}
				page, err = k.cachePage(depth, prev)
				if err != nil {
					return kwEntry{}, false, err
				}
				count := page.hdr.Count()
				if count == 0 {
					return kwEntry{}, false, err
				}
				if uint(count-1) < uint(len(page.ents)) {
					return page.ents[count-1], true, nil
				} else {
					goto corrupt
				}
			}
			if uint(i) < uint(len(page.ents)) {
				return page.ents[i], true, nil
			} else {
				goto corrupt
			}
		}

		i++
		if i >= int(count) || i >= kwEntries {
			id = page.hdr.Next()
		} else if uint(i) < uint(len(page.ents)) {
			id = page.ents[i].Child()
		} else {
			goto corrupt
		}
	}

corrupt:
	return kwEntry{}, false, errs.Errorf("corrupt key reader")
}
