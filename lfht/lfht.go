package lfht

import (
	"fmt"
	"math/bits"
	"sync/atomic"
	"unsafe"
)

// https://repositorio.inesctec.pt/bitstream/123456789/5465/1/P-00F-YAG.pdf

//
// parameters for the table
//

const (
	_width    = 3
	_entries  = 1 << _width
	_mask     = _entries - 1
	_bits     = bits.UintSize
	_depth    = 3
	_maxLevel = _bits / _width
)

//
// shorten some common phrases
//

type ptr = unsafe.Pointer

func cas(addr *ptr, old, new ptr) bool { return atomic.CompareAndSwapPointer(addr, old, new) }
func load(addr *ptr) ptr               { return atomic.LoadPointer(addr) }
func store(addr *ptr, val ptr)         { atomic.StorePointer(addr, val) }

func tag[K comparable, V any](b *T[K, V]) ptr   { return ptr(uintptr(ptr(b)) + 1) }
func tagged(p ptr) bool                         { return uintptr(p)&1 > 0 }
func untag[K comparable, V any](p ptr) *T[K, V] { return (*T[K, V])(ptr(uintptr(p) - 1)) }

//
// helper data types
//

type lazyValue[V any] struct {
	_ [0]func() // no equality

	value V
	fn    func() V
}

func (lv *lazyValue[V]) get() V {
	if lv.fn != nil {
		lv.value = lv.fn()
		lv.fn = nil
	}
	return lv.value
}

//
// data structrue
//

type T[K comparable, V any] struct {
	_ [0]func() // no equality

	header[K, V]
	_       [64 - unsafe.Sizeof(header[K, V]{})]byte // pad to cache line
	buckets [_entries]ptr
}

// N.B. this must be defined after T (see golang.org/issue/14620)
type header[K comparable, V any] struct {
	_ [0]func() // no equality

	level uint
	prev  *T[K, V]
	bmap  bmap
}

func (t *T[K, V]) getHashBucket(hash uint64) (*ptr, uint) {
	idx := uint(hash>>((t.level*_width)&(_bits-1))) & _mask
	return &t.buckets[idx], idx
}

type node[K comparable, V any] struct {
	_ [0]func() // no equality

	key   K
	hash  uint64
	value V
	next  ptr
}

func (n *node[K, V]) getNextRef() *ptr { return &n.next }

// Insert inserts a key-value pair into the map.
//
// If the key already exists in the map, the associated value is returned.
//
// Parameters:
//
//	k: The key to insert.
//	h: The hash of the key.
//	vf: A function that evaluates to the value to insert. The function is only
//	    evaluated if the key is not already present.
//
// Returns the value associated with the key.
func (t *T[K, V]) Insert(k K, h uint64, vf func() V) V {
	return t.insert(k, h, lazyValue[V]{fn: vf}).value
}

func (t *T[K, V]) insert(k K, h uint64, lv lazyValue[V]) *node[K, V] {
	bucket, idx := t.getHashBucket(h)
	entryRef := load(bucket)
	if entryRef == nil {
		newNode := &node[K, V]{key: k, hash: h, value: lv.get(), next: tag(t)}
		if cas(bucket, nil, ptr(newNode)) {
			t.bmap.AtomicSetIdx(idx)
			return newNode
		}
		entryRef = load(bucket)
	}

	if tagged(entryRef) {
		return untag[K, V](entryRef).insert(k, h, lv)
	}
	return (*node[K, V])(entryRef).insert(k, h, lv, t, 1)
}

func (n *node[K, V]) insert(k K, h uint64, lv lazyValue[V], t *T[K, V], count int) *node[K, V] {
	if n.key == k {
		return n
	}

	next := n.getNextRef()
	nextRef := load(next)
	if nextRef == tag(t) {
		if count == _depth && t.level+1 < _maxLevel {
			newTable := &T[K, V]{header: header[K, V]{
				level: t.level + 1,
				prev:  t,
			}}
			if cas(next, tag(t), tag(newTable)) {
				bucket, _ := t.getHashBucket(h)
				adjustChainNodes((*node[K, V])(load(bucket)), newTable)
				store(bucket, tag(newTable))
				return newTable.insert(k, h, lv)
			}
		} else {
			newNode := &node[K, V]{key: k, hash: h, value: lv.get(), next: tag(t)}
			if cas(next, tag(t), ptr(newNode)) {
				return newNode
			}
		}
		nextRef = load(next)
	}

	if tagged(nextRef) {
		prevTable := untag[K, V](nextRef)
		for prevTable.prev != nil && prevTable.prev != t {
			prevTable = prevTable.prev
		}
		return prevTable.insert(k, h, lv)
	}
	return (*node[K, V])(nextRef).insert(k, h, lv, t, count+1)
}

//
// adjust
//

func adjustChainNodes[K comparable, V any](r *node[K, V], t *T[K, V]) {
	next := r.getNextRef()
	nextRef := load(next)
	if nextRef != tag(t) {
		adjustChainNodes((*node[K, V])(nextRef), t)
	}
	t.adjustNode(r)
}

func (t *T[K, V]) adjustNode(n *node[K, V]) {
	next := n.getNextRef()
	store(next, tag(t))

	bucket, idx := t.getHashBucket(n.hash)
	entryRef := load(bucket)
	if entryRef == nil {
		if cas(bucket, nil, ptr(n)) {
			t.bmap.AtomicSetIdx(idx)
			return
		}
		entryRef = load(bucket)
	}

	if tagged(entryRef) {
		untag[K, V](entryRef).adjustNode(n)
		return
	}
	n.adjustNode(t, (*node[K, V])(entryRef), 1)
}

func (n *node[K, V]) adjustNode(t *T[K, V], r *node[K, V], count int) {
	next := r.getNextRef()
	nextRef := load(next)
	if nextRef == tag(t) {
		if count == _depth && t.level+1 < _maxLevel {
			newTable := &T[K, V]{header: header[K, V]{
				level: t.level + 1,
				prev:  t,
			}}
			if cas(next, tag(t), tag(newTable)) {
				bucket, _ := t.getHashBucket(n.hash)
				adjustChainNodes((*node[K, V])(load(bucket)), newTable)
				store(bucket, tag(newTable))
				newTable.adjustNode(n)
				return
			}
		} else if cas(next, tag(t), ptr(n)) {
			return
		}
		nextRef = load(next)
	}

	if tagged(nextRef) {
		prevTable := untag[K, V](nextRef)
		for prevTable.prev != nil && prevTable.prev != t {
			prevTable = prevTable.prev
		}
		prevTable.adjustNode(n)
		return
	}
	n.adjustNode(t, (*node[K, V])(nextRef), count+1)
}

//
// find
//

func (t *T[K, V]) Find(k K, h uint64) (V, bool) {
	// if lookup misses are frequent, it may be worthwhile to check
	// the bitmap to avoid a cache miss loading the bucket.
	bucket, _ := t.getHashBucket(h)
	entryRef := load(bucket)
	if entryRef == nil {
		return *new(V), false
	}
	if tagged(entryRef) {
		return untag[K, V](entryRef).Find(k, h)
	}
	return (*node[K, V])(entryRef).find(k, h, t)
}

func (n *node[K, V]) find(k K, h uint64, t *T[K, V]) (V, bool) {
	if n.key == k {
		return n.value, true
	}

	next := n.getNextRef()
	nextRef := load(next)
	if tagged(nextRef) {
		prevTable := untag[K, V](nextRef)
		for prevTable.prev != nil && prevTable.prev != t {
			prevTable = prevTable.prev
		}
		return prevTable.Find(k, h)
	}
	return (*node[K, V])(nextRef).find(k, h, t)
}

//
// iterator
//

type Iterator[K comparable, V any] struct {
	_ [0]func() // no equality

	n     *node[K, V]
	top   int
	stack [_maxLevel]struct {
		table *T[K, V]
		pos   bmap
	}
}

func (t *T[K, V]) Iterator() (itr Iterator[K, V]) {
	itr.stack[0].table = t
	itr.stack[0].pos = t.bmap.AtomicClone()
	return itr
}

func (i *Iterator[K, V]) Next() bool {
next:
	// if the stack is empty, we're done
	if i.top < 0 {
		return false
	}
	is := &i.stack[i.top]

	// if we don't have a node, load it from the top of the stack
	var nextTable *T[K, V]
	if i.n == nil {
		if is.pos.Empty() {
			// if we've walked the whole table, pop it and try again
			i.top--
			goto next
		}
		idx := is.pos.Lowest()
		is.pos.ClearLowest()

		bucket := &is.table.buckets[idx&127]
		entryRef := load(bucket)

		// if it's a node, set it and continue
		if !tagged(entryRef) {
			i.n = (*node[K, V])(entryRef)
			return true
		}

		// otherwise, we need to walk to a new table.
		nextTable = untag[K, V](entryRef)
	} else {
		// if we have a node, try to walk to the next entry.
		nextRef := load(i.n.getNextRef())

		// if it's a node, set it and continue
		if !tagged(nextRef) {
			i.n = (*node[K, V])(nextRef)
			return true
		}

		// otherwise, we need to walk to a new table
		nextTable = untag[K, V](nextRef)
	}

	// if we're on the same table, just go to the next entry
	if nextTable == is.table {
		i.n = nil
		goto next
	}

	// walk nextTable backwards as much as possible.
	for nextTable.prev != nil && nextTable.prev != is.table {
		nextTable = nextTable.prev
	}

	// if it's a different table, push it on to the stack.
	if nextTable != is.table {
		i.top++
		i.stack[i.top].table = nextTable
		i.stack[i.top].pos = nextTable.bmap.AtomicClone()
	}

	// walk to the next entry in the top of the stack table
	i.n = nil
	goto next
}

func (i *Iterator[K, V]) Key() K       { return i.n.key }
func (i *Iterator[K, V]) Hash() uint64 { return i.n.hash }
func (i *Iterator[K, V]) Value() V     { return i.n.value }

//
// dumping code
//

const dumpIndent = "|    "

func dumpPointer[K comparable, V any](indent string, p ptr) {
	if tagged(p) {
		table := untag[K, V](p)
		fmt.Printf("%stable[%p]:\n", indent, table)
		for i := range &table.buckets {
			dumpPointer[K, V](indent+dumpIndent, load(&table.buckets[i]))
		}
	} else if p != nil {
		n := (*node[K, V])(p)
		p := load(&n.next)
		fmt.Printf("%snode[%p](key:%v, hash:%v, value:%v, next:%p):\n", indent, n, n.key, n.hash, n.value, p)
		if !tagged(p) {
			dumpPointer[K, V](indent+dumpIndent, load(&n.next))
		}
	}
}

func (t *T[K, V]) dump() { dumpPointer[K, V]("", tag(t)) }
