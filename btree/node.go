package btree

import (
	"math"

	"github.com/zeebo/lsm"
)

const (
	invalidNode    = math.MaxUint32
	payloadEntries = 170
	payloadSplit   = payloadEntries / 2
)

// node are nodes in the btree.
type node struct {
	next    uint32 // pointer to the next node (or if not leaf, the rightmost edge)
	prev    uint32 // backpointer from next node (unused if not leaf)
	parent  uint32 // set to invalidNode on the root node
	count   uint16 // used values in payload
	leaf    bool   // set if is a leaf
	ok      bool
	payload [payloadEntries]entry
}

func (b *node) reset() {
	if b != nil {
		b.next = 0
		b.prev = 0
		b.parent = 0
		b.count = 0
		b.leaf = false
		b.ok = false
	}
}

// insertEntry inserts the entry into the node. it should never be called
// on a node that would have to split.
func (n *node) insertEntry(ent entry) {
	i, j := uint16(0), n.count
	for i < j {
		h := (i + j) >> 1
		if lsm.KeyCmp.Less(ent.key, n.payload[h].key) {
			j = h
		} else {
			i = h + 1
		}
	}
	copy(n.payload[i+1:], n.payload[i:n.count])
	n.payload[i] = ent
	n.count++
}

// appendEntry appends the entry into the node. it must compare greater than any
// element inside of the node, already, and should never be called on a node that
// would have to split.
func (n *node) appendEntry(ent entry) {
	n.payload[n.count] = ent
	n.count++
}
