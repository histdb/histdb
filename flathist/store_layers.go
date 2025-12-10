package flathist

import (
	"github.com/histdb/histdb/arena"
	"github.com/histdb/histdb/arena1"
)

type tag[V any] struct{}

type pointer[V any] struct {
	_ tag[V]
	v uint32
}

type storeLayers interface {
	Size() uint64
	Allocated() [4]uint32

	New0() pointer[layer0]
	New1() pointer[layer1]
	New2S() pointer[layer2Small]
	New2L() pointer[layer2Large]

	Get0(pointer[layer0]) *layer0
	Get1(pointer[layer1]) *layer1
	Get2S(pointer[layer2Small]) *layer2Small
	Get2L(pointer[layer2Large]) *layer2Large
}

func newDefaultStoreLayers() storeLayers {
	return new(batchArenaLayers)
}

func raw[V any](v uint32) pointer[V] { return pointer[V]{v: v} }
func (p pointer[V]) Raw() uint32     { return p.v }

//
// arenaLayers uses arenas for layer storage which is fast but has more wasted space but less
// pointers for the gc to track and does not require a mutex.
//

type batchArenaLayers struct {
	l0  arena.T[layer0]
	l1  arena.T[layer1]
	l2s arena.T[layer2Small]
	l2l arena.T[layer2Large]
}

func (a *batchArenaLayers) Size() uint64 {
	return 0 +
		/* l0  */ a.l0.Size() +
		/* l1  */ a.l1.Size() +
		/* l2s */ a.l2s.Size() +
		/* l2l */ a.l2l.Size() +
		0
}

func (a *batchArenaLayers) Allocated() [4]uint32 {
	return [4]uint32{
		a.l0.Allocated(),
		a.l1.Allocated(),
		a.l2s.Allocated(),
		a.l2l.Allocated(),
	}
}

func (a *batchArenaLayers) New0() pointer[layer0]       { return raw[layer0](a.l0.New().Raw()) }
func (a *batchArenaLayers) New1() pointer[layer1]       { return raw[layer1](a.l1.New().Raw()) }
func (a *batchArenaLayers) New2S() pointer[layer2Small] { return raw[layer2Small](a.l2s.New().Raw()) }
func (a *batchArenaLayers) New2L() pointer[layer2Large] { return raw[layer2Large](a.l2l.New().Raw()) }

func (a *batchArenaLayers) Get0(p pointer[layer0]) *layer0 {
	return a.l0.Get(arena.Raw[layer0](p.Raw()))
}
func (a *batchArenaLayers) Get1(p pointer[layer1]) *layer1 {
	return a.l1.Get(arena.Raw[layer1](p.Raw()))
}
func (a *batchArenaLayers) Get2S(p pointer[layer2Small]) *layer2Small {
	return a.l2s.Get(arena.Raw[layer2Small](p.Raw()))
}
func (a *batchArenaLayers) Get2L(p pointer[layer2Large]) *layer2Large {
	return a.l2l.Get(arena.Raw[layer2Large](p.Raw()))
}

//
// sliceLayers does smaller allocations and has less overhead for small amounts of data but has more
// pointers for the gc to track and requires a mutex.
//

type singleArenaLayers struct {
	l0  arena1.T[layer0]
	l1  arena1.T[layer1]
	l2s arena1.T[layer2Small]
	l2l arena1.T[layer2Large]
}

func (s *singleArenaLayers) Size() uint64 {
	return 0 +
		/* l0  */ s.l0.Size() +
		/* l1  */ s.l1.Size() +
		/* l2s */ s.l2s.Size() +
		/* l2l */ s.l2l.Size() +
		0
}

func (s *singleArenaLayers) Allocated() [4]uint32 {
	return [4]uint32{
		s.l0.Allocated(),
		s.l1.Allocated(),
		s.l2s.Allocated(),
		s.l2l.Allocated(),
	}
}

func (a *singleArenaLayers) New0() pointer[layer0]       { return raw[layer0](a.l0.New().Raw()) }
func (a *singleArenaLayers) New1() pointer[layer1]       { return raw[layer1](a.l1.New().Raw()) }
func (a *singleArenaLayers) New2S() pointer[layer2Small] { return raw[layer2Small](a.l2s.New().Raw()) }
func (a *singleArenaLayers) New2L() pointer[layer2Large] { return raw[layer2Large](a.l2l.New().Raw()) }

func (a *singleArenaLayers) Get0(p pointer[layer0]) *layer0 {
	return a.l0.Get(arena1.Raw[layer0](p.Raw()))
}
func (a *singleArenaLayers) Get1(p pointer[layer1]) *layer1 {
	return a.l1.Get(arena1.Raw[layer1](p.Raw()))
}
func (a *singleArenaLayers) Get2S(p pointer[layer2Small]) *layer2Small {
	return a.l2s.Get(arena1.Raw[layer2Small](p.Raw()))
}
func (a *singleArenaLayers) Get2L(p pointer[layer2Large]) *layer2Large {
	return a.l2l.Get(arena1.Raw[layer2Large](p.Raw()))
}
