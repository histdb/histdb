//go:build !amd64 || !gc
// +build !amd64 !gc

package flathist

func bitmask(data *[16]uint32) uint32 { return bitmaskFallback(data) }
