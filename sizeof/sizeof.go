package sizeof

import "unsafe"

func Slice[T any](v []T) uint64 {
	return 24 + uint64(unsafe.Sizeof(*new(T)))*uint64(len(v))
}
