package leveln

import (
	"math"
	"unsafe"

	"github.com/zeebo/errs/v2"

	"github.com/histdb/histdb"
	"github.com/histdb/histdb/filesystem"
)

// constants for the page
const (
	// The page size is the most important thing to tune here. Be sure that the value
	// makes kwEntries an integer value.
	//
	// The smaller the page size, the more frequently we have to write and the more nodes
	// there are in the index, decreasing write performance and potentially causing more
	// reads. It also reduces the amount of memory used in caches and potentially makes
	// reads have lower latencies.
	//
	// On the other hand, larger pages increase write performance, make the index have
	// a smaller depth, and potentially require less reads. It also increases the amount
	// of memory used in caches and potentially increases read latencies.
	//
	// The memory usage is because we keep a page per depth in both the reader and writer.
	kwPageSize   = 4096 * 4
	kwEntrySize  = histdb.KeySize + 4 + 1
	kwHeaderSize = 30 // 11 used
	kwEntries    = (kwPageSize - kwHeaderSize) / kwEntrySize

	_ uintptr = (kwHeaderSize + kwEntries*kwEntrySize) - kwPageSize
	_ uintptr = kwPageSize - (kwHeaderSize + kwEntries*kwEntrySize)

	valOffsetStart = histdb.KeySize
	valOffsetend   = valOffsetStart + 4

	valLengthStart = valOffsetend
	valLengthEnd   = valLengthStart + 1

	nameOffsetStart = valLengthEnd
	nameOffsetEnd   = nameOffsetStart + 4

	nextStart = 0
	nextEnd   = nextStart + 4

	prevStart = nextEnd
	prevEnd   = prevStart + 4

	countStart = prevEnd
	countEnd   = countStart + 2

	leafStart = countEnd
	leafEnd   = leafStart + 1
)

// kwEntry is the byte representation of an entry in the index.
type kwEntry [kwEntrySize]byte

func (k *kwEntry) Child() uint32        { return le.Uint32(k[valOffsetStart:valOffsetend]) }
func (k *kwEntry) SetChild(next uint32) { le.PutUint32(k[valOffsetStart:valOffsetend], next) }

// Key returns a pointer to the key portion of the entry.
func (k *kwEntry) Key() *histdb.Key { return (*histdb.Key)(k[0:histdb.KeySize]) }

// TODO: value files can only be up to 1TB? that doesn't seem like enough. we
// need more. maybe 6 bytes instead of 4?

// ValOffset returns the value offset encoded into the entry.
func (k *kwEntry) ValOffset() uint32 { return le.Uint32(k[valOffsetStart:valOffsetend]) }

// ValLength returns the value length encoded into the entry.
func (k *kwEntry) ValLength() uint8 { return k[valLengthStart] }

// Set sets all of the fields of the entry.
func (k *kwEntry) Set(key histdb.Key, voff uint32, vlen uint8) {
	copy(k[0:histdb.KeySize], key[0:histdb.KeySize])
	le.PutUint32(k[valOffsetStart:valOffsetend], voff)
	k[valLengthStart] = vlen
}

// kwPageHeader is the header starting every page.
type kwPageHeader [kwHeaderSize]byte

func (k *kwPageHeader) Next() uint32        { return be.Uint32(k[nextStart:nextEnd]) }
func (k *kwPageHeader) SetNext(next uint32) { be.PutUint32(k[nextStart:nextEnd], next) }

func (k *kwPageHeader) Prev() uint32        { return be.Uint32(k[prevStart:prevEnd]) }
func (k *kwPageHeader) SetPrev(prev uint32) { be.PutUint32(k[prevStart:prevEnd], prev) }

func (k *kwPageHeader) Count() uint16         { return be.Uint16(k[countStart:countEnd]) }
func (k *kwPageHeader) SetCount(count uint16) { be.PutUint16(k[countStart:countEnd], count) }

func (k *kwPageHeader) Leaf() bool { return k[leafStart] > 0 }
func (k *kwPageHeader) SetLeaf(leaf bool) {
	if leaf {
		k[leafStart] = 1
	} else {
		k[leafStart] = 0
	}
}

// kwPage is a struct with the same layout as a page.
type kwPage struct {
	_ [0]func() // no equality

	hdr  kwPageHeader
	ents [kwEntries]kwEntry
}

// Ensure the offsets of the page fields are what we expect and the full size
// is exactly kwPageSize.
const (
	_ uintptr = unsafe.Offsetof(kwPage{}.hdr) - 0
	_ uintptr = 0 - unsafe.Offsetof(kwPage{}.hdr)

	_ uintptr = unsafe.Offsetof(kwPage{}.ents) - kwHeaderSize
	_ uintptr = kwHeaderSize - unsafe.Offsetof(kwPage{}.ents)

	_ uintptr = unsafe.Sizeof(kwPage{}) - kwPageSize
	_ uintptr = kwPageSize - unsafe.Sizeof(kwPage{})
)

// Buf returns an unsafe pointer to the memory backing the page.
func (k *kwPage) Buf() *[kwPageSize]byte { return (*[kwPageSize]byte)(unsafe.Pointer(k)) }

// keyWriter allows one to append sorted keys and writes out a static b+ tree
// using log(n) in memory space.
type keyWriter struct {
	_ [0]func() // no equality

	fh    filesystem.H
	pages []*kwPage
	id    uint32
	count uint16
	page  kwPage
}

// Init resets the keyWriter to write to the provided file handle.
func (k *keyWriter) Init(fh filesystem.H) {
	k.fh = fh
	k.pages = nil
	k.id = 0
	k.count = 0
	k.page.hdr = kwPageHeader{}
	k.page.hdr.SetPrev(^uint32(0))
}

func (k *keyWriter) Append(ent kwEntry) error {
	if k.count < kwEntries-1 {
		k.page.ents[k.count] = ent
		k.count++
		return nil
	}
	return k.appendSlow(ent)
}

func (k *keyWriter) appendSlow(ent kwEntry) error {
	// add the value as the last entry in the leaf
	k.page.ents[kwEntries-1] = ent
	k.count++

	// we have to flush the leaf first, and then start flushing
	// the parents. that way the root node is always the last node
	// written. but in order to flush the leaf, we need the pointer
	// to the next node. in order to get that, we have to count up
	// the number of parents we will flush.
	next := k.id + 1
	for _, page := range k.pages {
		if page.hdr.Count() < kwEntries {
			break
		}
		next++
	}

	// now we can flush the newly filled leaf node and reset it
	k.page.hdr.SetNext(next)
	k.page.hdr.SetCount(k.count)
	k.page.hdr.SetLeaf(true)
	if err := k.writePage(&k.page); err != nil {
		return errs.Wrap(err)
	}
	k.page.hdr.SetPrev(k.id) // set prev pointer for next leaf
	k.id++
	k.count = 0

	// now, insert pointers into the parent nodes, allocating
	// a new root node if necessary. flush any parent nodes that
	// become full from the insertion.
	for _, page := range k.pages {
		// if we had room left over, then we're done after the insertion.
		if count := page.hdr.Count(); count < kwEntries {
			ent.SetChild(k.id - 1)
			page.ents[count] = ent
			page.hdr.SetCount(count + 1)
			return nil
		}

		// the page is already full: flush it and reset it.
		page.hdr.SetNext(k.id - 1)
		if err := k.writePage(page); err != nil {
			return errs.Wrap(err)
		}
		k.id++
		page.hdr.SetCount(0)
	}

	// all the pages were full. allocate a new one to hold the entry.
	p := new(kwPage)
	ent.SetChild(k.id - 1)
	p.ents[0] = ent
	p.hdr.SetNext(next)
	p.hdr.SetCount(1)
	k.pages = append(k.pages, p)

	return nil
}

// Finish flushes any partial pages. No more Append or Finish calls should be
// made after a call to Finish. No calls to Append or Finish should be made
// after either returns an error.
func (k *keyWriter) Finish() error {
	// write the leaf
	k.page.hdr.SetNext(math.MaxUint32)
	k.page.hdr.SetCount(k.count)
	k.page.hdr.SetLeaf(true)
	if err := k.writePage(&k.page); err != nil {
		return errs.Wrap(err)
	}
	k.id++

	// write all the parents
	for _, page := range k.pages {
		page.hdr.SetNext(k.id - 1)
		if err := k.writePage(page); err != nil {
			return errs.Wrap(err)
		}
		k.id++
	}

	return nil
}

func (k *keyWriter) writePage(page *kwPage) error {
	_, err := k.fh.Write(page.Buf()[:])
	return err
}
