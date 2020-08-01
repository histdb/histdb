package btree

import (
	"github.com/zeebo/lsm"
)

// Iterator walks over the entries in a btree.
type Iterator struct {
	b *T
	n *node
	i uint16
}

func (i *Iterator) Next() bool {
	if i.n == nil {
		return false
	}
	i.i++
next:
	if i.i < i.n.count {
		return true
	} else if i.n.next == invalidNode {
		i.n = nil
		return false
	}
	i.n = i.b.nodes[i.n.next]
	i.i = 0
	goto next
}

func (i *Iterator) Key() lsm.Key {
	return i.n.payload[i.i].key
}

func (i *Iterator) Value() uint32 {
	return i.n.payload[i.i].value
}
