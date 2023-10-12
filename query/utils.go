package query

func appendTag(buf, tkey []byte, tval string) []byte {
	buf = append(buf, tkey...)
	buf = append(buf, '=')
	buf = append(buf, tval...)
	return buf
}

type bytesSet struct {
	set  map[string]int64
	list [][]byte
}

func newBytesSet(cap int) bytesSet {
	return bytesSet{list: make([][]byte, 0, cap)}
}

func (s *bytesSet) lookup(x []byte) (n int64, ok bool) {
	if s.set != nil {
		n, ok = s.set[string(x)]
		return n, ok
	}
	for n, u := range s.list {
		if string(x) == string(u) {
			return int64(n), true
		}
	}
	return 0, false
}

func (s *bytesSet) add(x []byte) (n int64) {
	if s.set != nil {
		if n, ok := s.set[string(x)]; ok {
			return n
		}
		n := int64(len(s.list))
		s.list = append(s.list, x)
		s.set[string(x)] = n
		return n
	}

	for n, u := range s.list {
		if string(x) == string(u) {
			return int64(n)
		}
	}

	n = int64(len(s.list))
	s.list = append(s.list, x)
	if len(s.list) == cap(s.list) {
		s.set = make(map[string]int64)
		for n, u := range s.list {
			s.set[string(u)] = int64(n)
		}
	}

	return n
}

type anySet[T comparable] struct {
	set  map[T]int64
	list []T
}

func newAnySet[T comparable](cap int) anySet[T] {
	return anySet[T]{list: make([]T, 0, cap)}
}

func (s *anySet[T]) add(x T) (n int64) {
	if s.set != nil {
		if n, ok := s.set[x]; ok {
			return n
		}
		n := int64(len(s.list))
		s.list = append(s.list, x)
		s.set[x] = n
		return n
	}

	for n, u := range s.list {
		if x == u {
			return int64(n)
		}
	}

	n = int64(len(s.list))
	s.list = append(s.list, x)
	if len(s.list) == cap(s.list) {
		s.set = make(map[T]int64)
		for n, u := range s.list {
			s.set[u] = int64(n)
		}
	}

	return n
}

type valueSet = anySet[value]

func newValueSet(cap int) valueSet { return newAnySet[value](cap) }
