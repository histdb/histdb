//go:build (amd64 || amd64p32) && gc
// +build amd64 amd64p32
// +build gc

package floathist

// Used to sum a layer2Small with no assumptions about the size
// of the individual counters contained.
//
//go:noescape
func sumLayer2SmallAVX2(layer2) uint64

// Used to sum a layer2Large with no assumptions about the size
// of the individual counters contained.
//
//go:noescape
func sumLayer2LargeAVX2(layer2) uint64
