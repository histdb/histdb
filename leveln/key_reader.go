package leveln

import (
	"encoding/binary"

	"github.com/zeebo/errs/v2"

	"github.com/histdb/histdb"
	"github.com/histdb/histdb/filesystem"
)

type krPage struct {
	page *kwPage
	id   uint32
}

type keyReader struct {
	fh    filesystem.Handle
	root  uint32
	cache []krPage

	stats struct {
		reads int64
	}
}

func (k *keyReader) Init(fh filesystem.Handle) {
	*k = keyReader{
		fh:   fh,
		root: ^uint32(0),
	}
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

func (k *keyReader) Search(key *histdb.Key) (offset uint32, ok bool, err error) {
	keyp := binary.BigEndian.Uint64(key[0:8])
	id := k.root

	for depth := uint(0); ; depth++ {
		var page *kwPage
		if depth < uint(len(k.cache)) && k.cache[depth].id == id {
			page = k.cache[depth].page
		} else {
			page, err = k.cachePage(depth, id)
			if err != nil {
				return 0, false, err
			}
		}

		count := page.hdr.Count()
		i, j := -1, int(count)
		for j-i > 1 {
			h := int(uint(i+j) >> 1)
			ent := &page.ents[h]

			// LessPtr does a call into the runtime when the first 8 bytes will
			// frequently be enough to compare unequal, so try that first.
			if keyhp := binary.BigEndian.Uint64(ent[0:8]); keyhp < keyp {
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
					return 0, false, nil
				}
				page, err = k.cachePage(depth, prev)
				if err != nil {
					return 0, false, err
				}
				count := page.hdr.Count()
				if count == 0 {
					return 0, false, err
				}
				ent := &page.ents[count-1]
				return ent.Offset(), true, nil
			}
			ent := &page.ents[i]
			return ent.Offset(), true, nil
		}

		i++
		if i >= int(count) || i >= kwEntries {
			id = page.hdr.Next()
		} else {
			id = page.ents[i].Child()
		}
	}
}
