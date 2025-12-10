package arena1

import (
	"sync"
	"sync/atomic"
	"unsafe"
)

var _ T[int]

type T[V any] struct {
	_ [0]func() // no equality

	s atomic.Pointer[*V]
	p atomic.Uint32
	t atomic.Uint32

	mu sync.Mutex // protects realloc
}

func (a *T[V]) Size() uint64 {
	return 0 +
		/* buf */ uint64(a.t.Load())*uint64(unsafe.Sizeof(*new(V))+8) +
		/* s   */ 8 +
		/* p   */ 4 +
		/* t   */ 4 +
		/* mu  */ uint64(unsafe.Sizeof(sync.Mutex{})) +
		0
}

func (a *T[V]) Allocated() uint32 { return a.p.Load() }

type tag[V any] struct{}

type P[V any] struct {
	_ tag[V]
	v uint32
}

func Raw[V any](v uint32) P[V] { return P[V]{v: v} }
func (p P[V]) Raw() uint32     { return p.v }

func (a *T[V]) New() (p P[V]) {
	if p.v = a.p.Add(1); p.v >= a.t.Load() {
		a.realloc(p.v)
	}
	return
}

func (a *T[V]) Get(p P[V]) *V {
	return unsafe.Slice(a.s.Load(), a.t.Load())[p.v]
}

//go:noinline
func (a *T[V]) realloc(v uint32) {
	a.mu.Lock()
	defer a.mu.Unlock()

	switch t := a.t.Load(); {
	case v < t:
		// another goroutine already did the realloc

	case t == 0:
		next := []*V{1: new(V)}
		a.s.Store(unsafe.SliceData(next))
		a.t.Store(uint32(len(next)))

	default:
		next := make([]*V, t*2)
		fill(next[copy(next, unsafe.Slice(a.s.Load(), t)):])
		a.s.Store(unsafe.SliceData(next))
		a.t.Store(uint32(len(next)))
	}
}

func fill[V any](x []*V) {
	for i := range x {
		x[i] = new(V)
	}
}
