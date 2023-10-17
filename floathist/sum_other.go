//go:build !((amd64 || amd64p32) && gc)
// +build !amd64,!amd64p32 !gc

package floathist

func sumLayer2SmallAVX2(l2 layer2) uint64 { return sumLayer2SmallFallback(l2) }
func sumLayer2LargeAVX2(l2 layer2) uint64 { return sumLayer2LargeFallback(l2) }
