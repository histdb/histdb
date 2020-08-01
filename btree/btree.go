package btree

import (
	"math"

	"github.com/zeebo/lsm"
)

type entry struct {
	key   lsm.Key
	value uint32
}

const (
	invalidNode    = math.MaxUint32
	payloadEntries = 170
)

// node are nodes in the btree.
type node struct {
	next    uint32 // pointer to the next node (or if not leaf, the rightmost edge)
	prev    uint32 // backpointer from next node (unused if not leaf)
	parent  uint32 // set to invalidNode on the root node
	count   uint16 // used values in payload
	leaf    bool   // set if is a leaf
	payload [payloadEntries]entry
}

// appendEntry appends the entry into the node. it must compare greater than any
// element inside of the node, already, and should never be called on a node that
// would have to split.
func (n *node) appendEntry(ent entry) {
	n.payload[0] = ent
	n.count++
}

// T is an in memory B+ tree tuned to store fixed size keys and values.
type T struct {
	root    *node
	right   *node
	nodes   []*node
	rootid  uint32
	rightid uint32
}

// alloc creates a fresh node.
func (b *T) alloc(leaf bool) (n *node, id uint32) {
	n = &node{
		next:   invalidNode,
		prev:   invalidNode,
		parent: invalidNode,
		leaf:   leaf,
	}

	b.nodes = append(b.nodes, n)
	return n, uint32(len(b.nodes) - 1)
}

// Append adds the key and value to the tree. The key must be greater than
// or equal to any previously appended keys.
func (b *T) Append(key lsm.Key, value uint32) {
	ent := entry{key: key, value: value}

	// allocate a root entry if necessary
	if b.root == nil {
		b.root, b.rootid = b.alloc(true)
		b.right, b.rightid = b.root, b.rootid
		b.root.appendEntry(ent)
		return
	}

	// use our reference to the rightmost node
	n, nid := b.right, b.rightid
	if n.count < payloadEntries {
		n.appendEntry(ent)
		return
	}

	// it's full. allocate a new leaf.
	s, sid := b.alloc(true)
	n.next = sid
	s.prev = nid
	b.right, b.rightid = s, sid

	// we can insert into the leaf
	s.appendEntry(ent)

	// we now have to insert a pointer to a parent, so walk up
	// allocating new parents until we find one with a slot or
	// we find that we need a new root.
	for {
		var p *node
		var pid uint32

		if n.parent == invalidNode {
			p, pid = b.alloc(false)
			b.root, b.rootid = p, pid
		} else {
			p, pid = b.nodes[n.parent], n.parent
		}

		// if the parent has room, insert it and set the next pointer
		if p.count < payloadEntries {
			ent.value = nid
			p.appendEntry(ent)
			s.parent = pid
			p.next = sid
			return
		}

		// allocate a new node and traverse upwards
		q, qid := b.alloc(false)
		q.next = sid
		s.parent = qid

		n, nid = p, pid
		s, sid = q, qid
	}
}

func (b *T) Iterator() Iterator {
	// find the deepest leftmost node
	n := b.root
	if n == nil {
		return Iterator{}
	}

next:
	if !n.leaf {
		n = b.nodes[n.payload[0].value]
		goto next
	}

	return Iterator{
		b: b,
		n: n,
		i: uint16(1<<16 - 1), // overflow hack. this is -1
	}
}
