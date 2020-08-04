package leveln

import (
	"encoding/binary"
	"math"
	"unsafe"

	"github.com/zeebo/errs"
	"github.com/zeebo/lsm"
	"github.com/zeebo/lsm/filesystem"
)

// constants for the page
const (
	// The page size is the most important thing to tune here. Be sure that the value
	// makes kwEntries an integer value. Powers of 4 are useful values to ensure that.
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
	kwEntrySize  = 24
	kwHeaderSize = 16
	kwEntries    = (kwPageSize - kwHeaderSize) / kwEntrySize
)

// kwEntry is the byte representation of an entry in the index.
type kwEntry [kwEntrySize]byte

// Key returns a pointer to the key portion of the entry.
func (k *kwEntry) Key() *lsm.Key { return (*lsm.Key)(unsafe.Pointer(k)) }

// Offset returns the offset encoded into the entry
func (k *kwEntry) Offset() uint32 { return binary.BigEndian.Uint32(k[20:24]) }

// kwEncode is used to encode a key and offset into a 24 byte entry. it is an
// external function so that the append call can be outlined.
func kwEncode(key lsm.Key, offset uint32) (ent kwEntry) { // nolint
	copy(ent[0:20], key[0:20])
	binary.BigEndian.PutUint32(ent[20:24], offset)
	return ent
}

// kwPageHeader is the header starting every 4096 byte index page.
type kwPageHeader [16]byte

func (k *kwPageHeader) Next() uint32        { return binary.BigEndian.Uint32(k[0:4]) }
func (k *kwPageHeader) SetNext(next uint32) { binary.BigEndian.PutUint32(k[0:4], next) }

func (k *kwPageHeader) Count() uint16         { return binary.BigEndian.Uint16(k[4:6]) }
func (k *kwPageHeader) SetCount(count uint16) { binary.BigEndian.PutUint16(k[4:6], count) }

func (k *kwPageHeader) Leaf() bool { return k[6] > 0 }
func (k *kwPageHeader) SetLeaf(leaf bool) {
	if leaf {
		k[6] = 1
	} else {
		k[6] = 0
	}
}

// kwPage is a struct with the same layout as a page.
type kwPage struct {
	hdr  kwPageHeader
	ents [kwEntries]kwEntry
}

// Ensure the offsets of the page fields are what we expect
const (
	_ uintptr = unsafe.Offsetof(kwPage{}.hdr) - 0
	_ uintptr = 0 - unsafe.Offsetof(kwPage{}.hdr)

	_ uintptr = unsafe.Offsetof(kwPage{}.ents) - kwHeaderSize
	_ uintptr = kwHeaderSize - unsafe.Offsetof(kwPage{}.ents)
)

// Buf returns an unsafe pointer to the memory backing the page.
func (k *kwPage) Buf() *[kwPageSize]byte { return (*[kwPageSize]byte)(unsafe.Pointer(k)) }

// keyWriter allows one to append sorted keys and writes out a static b+ tree
// using log(n) in memory space.
type keyWriter struct {
	fh    filesystem.File
	pages []*kwPage
	id    uint32

	count uint16       // duplicated from hdr so append can be outlined
	_     [2]byte      // padding so the hdr field is aligned appropriately
	hdr   kwPageHeader // inlined kwPage so append can be outlined
	ents  [kwEntries]kwEntry
}

// Ensure the inlined kwPage is at the offsets we expect (a page from the end)
const (
	kwEndSize = unsafe.Sizeof(keyWriter{}) - unsafe.Offsetof(keyWriter{}.hdr)

	_ uintptr = kwEndSize - kwPageSize
	_ uintptr = kwPageSize - kwEndSize
)

// Init resets the keyWriter to write to the provided file handle.
func (k *keyWriter) Init(fh filesystem.File) {
	k.fh = fh
	k.pages = nil
	k.id = 0
	k.count = 0
	k.hdr = kwPageHeader{}
}

func (k *keyWriter) page() *kwPage {
	return (*kwPage)(unsafe.Pointer(&k.hdr))
}

// Append adds the encoded entry to the writer. No calls to Append or Finish
// should be made after either returns an error.
func (k *keyWriter) Append(ent kwEntry) error {
	if k.count < kwEntries-1 {
		k.ents[k.count] = ent
		k.count++
		return nil
	}
	return k.appendSlow(ent)
}

func (k *keyWriter) appendSlow(ent kwEntry) error {
	// add the value as the last entry in the leaf
	k.ents[kwEntries-1] = ent
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
	k.hdr.SetNext(next)
	k.hdr.SetCount(k.count)
	k.hdr.SetLeaf(true)
	if err := k.writePage(k.page()); err != nil {
		return errs.Wrap(err)
	}
	k.id++
	k.count = 0

	// now, insert pointers into the parent nodes, allocating
	// a new root node if necessary. flush any parent nodes that
	// become full from the insertion.
	for _, page := range k.pages {
		// if we had room left over, then we're done after the insertion.
		if count := page.hdr.Count(); count < kwEntries {
			binary.BigEndian.PutUint32(ent[20:24], k.id-1)
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
	binary.BigEndian.PutUint32(ent[20:24], k.id-1)
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
	k.hdr.SetNext(math.MaxUint32)
	k.hdr.SetCount(k.count)
	k.hdr.SetLeaf(true)
	if err := k.writePage(k.page()); err != nil {
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
