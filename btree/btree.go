package btree

import (
	"github.com/zeebo/lsm"
)

type entry struct {
	key   lsm.Key
	value uint32
}

// T is an in memory B+ tree tuned to store entries
type T struct {
	root  *node
	rid   uint32
	right uint32
	nodes []*node
}

// Reset clears the btree back to an empty state
func (b *T) Reset() {
	b.root.reset()
	b.rid = 0
	b.right = 0
	b.nodes = b.nodes[:0]
}

// search returns the leaf node that should contain the key.
func (b *T) search(ent entry) (*node, uint32) {
	n, nid := b.root, b.rid

	for !n.leaf {
		i, j := uint16(0), n.count
		for i < j {
			h := (i + j) >> 1
			if lsm.KeyCmp.Less(ent.key, n.payload[h].key) {
				j = h
			} else {
				i = h + 1
			}
		}
		if i == n.count {
			nid = n.next
		} else {
			nid = n.payload[i].value
		}
		n = b.nodes[nid]
	}

	return n, nid
}

// alloc creates a fresh node.
func (b *T) alloc(leaf bool) (n *node, id uint32) {
	if len(b.nodes) < cap(b.nodes) {
		n = b.nodes[:len(b.nodes)+1][len(b.nodes)]
		n.reset()
	}
	if n == nil {
		n = new(node)
	}

	n.next = invalidNode
	n.prev = invalidNode
	n.parent = invalidNode
	n.leaf = leaf
	n.ok = true
	b.nodes = append(b.nodes, n)

	return n, uint32(len(b.nodes) - 1)
}

// split the node in half, returning a new node containing the
// smaller half of the keys.
func (b *T) split(n *node, nid uint32) (*node, uint32) {
	s, sid := b.alloc(n.leaf)
	s.parent = n.parent

	// split the entries between the two nodes
	s.count = uint16(copy(s.payload[:], n.payload[:payloadSplit]))

	copyAt := payloadSplit
	if !n.leaf {
		// if it's not a leaf, we don't want to include the split btreeEntry
		copyAt++

		// additionally, the next pointer should be what the split btreeEntry
		// points at.
		s.next = n.payload[payloadSplit].value

		// additionally, every element that it points at needs to have
		// their parent updated
		b.nodes[s.next].parent = sid
		for i := uint16(0); i < s.count; i++ {
			b.nodes[s.payload[i].value].parent = sid
		}
	} else {
		// if it is a leaf, fix up the next and previous pointers
		s.next = nid
		if n.prev != invalidNode {
			s.prev = n.prev
			b.nodes[s.prev].next = sid
		}
		n.prev = sid
	}
	n.count = uint16(copy(n.payload[:], n.payload[copyAt:]))

	return s, sid
}

// Insert puts the btreeEntry into the btree, using the buf to read keys
// to determine the position. It returns true if the insert created
// a new btreeEntry.
func (b *T) Insert(key lsm.Key, value uint32) {
	ent := entry{key: key, value: value}

	// easy case: if we have no root, we can just allocate it
	// and insert the btreeEntry.
	if b.root == nil || !b.root.ok {
		b.root, b.rid = b.alloc(true)
		b.right = b.rid
		b.root.insertEntry(ent)
		return
	}

	// search for the leaf that should contain the node
	n, nid := b.search(ent)
	for {
		n.insertEntry(ent)

		// easy case: if the node still has enough room, we're done.
		if n.count < payloadEntries {
			return
		}

		// update the btreeEntry we're going to insert to be the btreeEntry we're
		// splitting the node on.
		ent = n.payload[payloadSplit]

		// split the node. s is a new node that contains keys
		// smaller than the splitbtreeEntry.
		s, sid := b.split(n, nid)

		// find the parent, allocating a new node if we're looking
		// at the root, and set the parent of the split node.
		var p *node
		var pid uint32
		if n.parent != invalidNode {
			p, pid = b.nodes[n.parent], n.parent
		} else {
			// create a new parent node, and make it point at the
			// larger side of the split node for it's next pointer.
			p, pid = b.alloc(false)
			p.next = nid
			n.parent = pid
			s.parent = pid

			// store it as the root
			b.root, b.rid = p, pid
		}

		// make a pointer out of the split btreeEntry to point at the
		// newly split node, and try to insert it.
		ent.value = sid
		n, nid = p, pid
	}
}

// append adds the entry to the node, splitting if necessary. the entry must
// be greater than any entry already in the node. n remains to the right of
// and at least as low than any newly created nodes.
func (b *T) Append(key lsm.Key, value uint32) {
	ent := entry{
		key:   key,
		value: value,
	}

	// allocate a root entry if necessary
	if b.root == nil || !b.root.ok {
		b.root, b.rid = b.alloc(true)
		b.right = b.rid
		b.root.appendEntry(ent)
		return
	}

	// use our reference to the rightmost node
	n, nid := b.nodes[b.right], b.right

	// easy case: if the node still has enough room, we're done.
	if n.count < payloadEntries {
		n.appendEntry(ent)
		return
	}

	// allocate a new leaf node on the right
	s, sid := b.alloc(true)
	s.parent = n.parent
	s.prev, n.next = nid, sid
	b.right = sid

	// append the entry into the new leaf
	s.appendEntry(ent)

	// find the parent, allocating a new node if we're looking
	// at the root, and set the parent of the split node.
	var p *node
	var pid uint32
	if n.parent != invalidNode {
		p, pid = b.nodes[n.parent], n.parent
		p.next = sid
	} else {
		// create a new parent node, and make it point at the
		// larger side of the split node for it's next pointer.
		p, pid = b.alloc(false)
		p.next = sid
		n.parent = pid
		s.parent = pid

		// store it as the root
		b.root, b.rid = p, pid
	}

	// update the entry to point at the left leaf and walk to the parent
	ent.value = nid
	n, nid = p, pid

	for {
		// we're always appending
		n.appendEntry(ent)

		// easy case: if the node still has enough room, we're done.
		if n.count < payloadEntries {
			return
		}

		// update the btreeEntry we're going to insert to be the btreeEntry we're
		// splitting the node on.
		ent = n.payload[payloadSplit]

		// split the node. s is a new node that contains keys
		// smaller than the splitbtreeEntry.
		s, sid := b.split(n, nid)

		// find the parent, allocating a new node if we're looking
		// at the root, and set the parent of the split node.
		var p *node
		var pid uint32
		if n.parent != invalidNode {
			p, pid = b.nodes[n.parent], n.parent
		} else {
			// create a new parent node, and make it point at the
			// larger side of the split node for it's next pointer.
			p, pid = b.alloc(false)
			p.next = nid
			n.parent = pid
			s.parent = pid

			// store it as the root
			b.root, b.rid = p, pid
		}

		// make a pointer out of the split btreeEntry to point at the
		// newly split node, and try to insert it.
		ent.value = sid
		n, nid = p, pid
	}
}

func (b *T) Iterator() Iterator {
	// find the deepest leftmost node
	n := b.root
	if n == nil {
		return Iterator{}
	}

	for !n.leaf {
		nid := n.payload[0].value
		if n.count == 0 {
			nid = n.next
		}
		n = b.nodes[nid]
	}

	return Iterator{
		b: b,
		n: n,
		i: uint16(1<<16 - 1), // overflow hack. this is -1
	}
}
