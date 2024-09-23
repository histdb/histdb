package arena

import (
	"math/bits"
	"sync"
	"sync/atomic"
	"unsafe"
)

const (
	lBatch = 1024
	lAlloc = 8

	ptrSize = unsafe.Sizeof(unsafe.Pointer(nil))
)

type T[V any] struct {
	_ [0]func() // no equality

	s atomic.Pointer[*[lBatch]V]
	p atomic.Uint32
	t atomic.Uint32

	mu sync.Mutex // protects realloc
}

func (t *T[V]) Size() uint64 {
	return 0 +
		/* buf */ uint64(((t.t.Load()+lBatch-1)/lBatch)*lBatch)*uint64(unsafe.Sizeof(*new(V))) +
		/* s   */ 8 +
		/* p   */ 4 +
		/* t   */ 4 +
		/* mu  */ uint64(unsafe.Sizeof(sync.Mutex{})) +
		0
}

func (t *T[V]) Allocated() uint32 { return t.p.Load() }

type tag[V any] struct{}

type P[V any] struct {
	_ tag[V]
	v uint32
}

func Raw[V any](v uint32) P[V] { return P[V]{v: v} }
func (p P[V]) Raw() uint32     { return p.v }

func (l *T[V]) Get(p P[V]) *V {
	b := unsafe.Add(unsafe.Pointer(l.s.Load()), uintptr(p.v/lBatch)*ptrSize)
	return &(*(**[lBatch]V)(b))[p.v%lBatch]
}

func (l *T[V]) New() (p P[V]) {
	p.v = l.p.Add(1)
	if p.v >= l.t.Load() {
		l.realloc(p.v)
	}
	return
}

func (l *T[V]) realloc(v uint32) {
	l.mu.Lock()
	defer l.mu.Unlock()

	for t := l.t.Load(); v >= t; t += lBatch {
		var arr []*[lBatch]V

		switch {
		// first time through we initally alloc lAlloc
		case t == 0:
			arr = make([]*[lBatch]V, lAlloc)
			l.s.Store(&arr[0])

		// we need to reallocate if we're at at least lBatch*lAlloc and we've
		// just hit a new power of 2
		case t >= lBatch*lAlloc && bits.OnesCount32(t) == 1:
			arr = make([]*[lBatch]V, t/(lBatch/2))
			copy(arr, unsafe.Slice(l.s.Load(), t/lBatch))
			l.s.Store(&arr[0])

		// otherwise, load arr so we can allocate a new batch
		default:
			arr = unsafe.Slice(l.s.Load(), t/lBatch+1)
		}

		arr[t/lBatch] = new([lBatch]V)
		l.t.Add(lBatch) // synchronizes arr reads
	}
}
