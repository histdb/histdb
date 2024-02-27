//go:build amd64 && gc
// +build amd64,gc

package flathist

//go:noescape
func bitmaskAVX(data *[16]uint32) uint32

func bitmask(data *[16]uint32) uint32 { return bitmaskAVX(data) }
