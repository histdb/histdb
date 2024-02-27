//go:build !(amd64 && gc)
// +build !amd64 !gc

package flathist

func sumLayer2SmallAVX2(l *layer2Small) uint64 { return sumLayer2SmallFallback(l) }
func sumLayer2LargeAVX2(l *layer2Large) uint64 { return sumLayer2LargeFallback(l) }

func sumLayer2Small(l *layer2Small) uint64 { return sumLayer2SmallFallback(l) }
func sumLayer2Large(l *layer2Large) uint64 { return sumLayer2LargeFallback(l) }
