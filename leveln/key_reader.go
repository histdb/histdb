package leveln

import (
	"encoding/binary"

	"github.com/zeebo/errs"
	"github.com/zeebo/lsm"
	"github.com/zeebo/lsm/filesystem"
)

type krPage struct {
	page *kwPage
	id   uint32
}

type keyReader struct {
	fh    filesystem.File
	root  uint32
	cache []krPage

	stats struct {
		reads int64
	}
}

func (k *keyReader) Init(fh filesystem.File) {
	*k = keyReader{
		fh: fh,
	}
}

func (k *keyReader) cachePage(depth uint, id uint32) (*kwPage, error) {
	// the root is always depth 0, so we ignore the id and load the root page
	if depth == 0 {
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

func (k *keyReader) Seek(key lsm.Key) (_ lsm.Key, _ uint32, _ bool, err error) {
	keyp := binary.BigEndian.Uint64(key[0:8])
	id := k.root

	for depth := uint(0); ; depth++ {
		var page *kwPage
		if depth < uint(len(k.cache)) && k.cache[depth].id == id {
			page = k.cache[depth].page
		} else {
			page, err = k.cachePage(depth, id)
			if err != nil {
				return
			}
		}

		count := page.hdr.Count()
		i, j := uint16(0), count
		for i < j {
			h := (i + j) >> 1
			ent := &page.ents[h]

			// LessPtr does a call into the runtime when the first 8 bytes will
			// frequently be enough to compare unequal, so try that first.
			if keyhp := binary.BigEndian.Uint64(ent[0:8]); keyhp < keyp {
				i = h + 1
			} else if keyhp > keyp {
				j = h
			} else if lsm.KeyCmp.LessPtr(ent.Key(), &key) {
				i = h + 1
			} else {
				j = h
			}
		}

		if page.hdr.Leaf() {
			if i >= count || i >= kwEntries {
				return lsm.Key{}, 0, false, nil
			}
			ent := &page.ents[i]
			return *ent.Key(), ent.Offset(), true, nil
		}

		if i >= count || i >= kwEntries {
			id = page.hdr.Next()
		} else {
			id = page.ents[i].Offset()
		}
	}
}
