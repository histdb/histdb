//go:build amd64 && gc
// +build amd64,gc

package flathist

// Used to sum a layer2Small with no assumptions about the size
// of the individual counters contained.
//
//go:noescape
func sumLayer2SmallAVX2(data *layer2Small) uint64

// Used to sum a layer2Large with no assumptions about the size
// of the individual counters contained.
//
//go:noescape
func sumLayer2LargeAVX2(data *layer2Large) uint64

func sumLayer2Small(l *layer2Small) uint64 { return sumLayer2SmallAVX2(l) }
func sumLayer2Large(l *layer2Large) uint64 { return sumLayer2LargeAVX2(l) }
