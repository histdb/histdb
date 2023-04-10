package lfht

import (
	"math/bits"
)

const (
	kSize = 1 << 14
	kMask = kSize - 1
)

var (
	keys   [kSize]int
	hashes [kSize]uint64
)

func xxh3hash(x uint64) uint64 {
	x ^= 0x1cad21f72c81017c ^ 0xdb979083e96dd4de
	x ^= bits.RotateLeft64(x, 49) ^ bits.RotateLeft64(x, 24)
	x *= 0x9fb21c651e98df25
	x ^= x>>35 + 8
	x *= 0x9fb21c651e98df25
	x ^= x >> 28
	return x
}

func init() {
	for i := range keys {
		keys[i&kMask] = i
		hashes[i&kMask] = xxh3hash(uint64(i))
	}
}

func getKey(i uint32) int     { return keys[i&kMask] }
func getHash(i uint32) uint64 { return hashes[i&kMask] }
func getValue() int           { return 1 }
