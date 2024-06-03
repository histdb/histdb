package query

import "fmt"

func appendTag(buf, tkey []byte, tval string) []byte {
	buf = append(buf, tkey...)
	buf = append(buf, '=')
	buf = append(buf, tval...)
	return buf
}

type matcher struct {
	_ [0]func() // no equality

	fn func([]byte) bool
	k  string
	q  string
}

func (m matcher) String() string { return fmt.Sprintf("%s(%q)", m.k, m.q) }

type bytesSet struct {
	_ [0]func() // no equality

	set  map[string]int16
	list [][]byte
}

func newBytesSet() bytesSet {
	return bytesSet{}
}

func (s *bytesSet) reset() {
	clear(s.set)
	clear(s.list)
	s.list = s.list[:0]
}

func (s *bytesSet) lookup(x []byte) (n int16, ok bool) {
	if s.set != nil {
		n, ok = s.set[string(x)]
		return n, ok
	}
	for n, u := range s.list {
		if string(x) == string(u) {
			return int16(n), true
		}
	}
	return 0, false
}

func (s *bytesSet) add(x []byte) (n int16) {
	if s.set != nil {
		if n, ok := s.set[string(x)]; ok {
			return n
		}
		n := int16(len(s.list))
		s.list = append(s.list, x)
		s.set[string(x)] = n
		return n
	}

	for n, u := range s.list {
		if string(x) == string(u) {
			return int16(n)
		}
	}

	n = int16(len(s.list))
	if cap(s.list) == 0 {
		s.list = make([][]byte, 0, 8)
	}
	s.list = append(s.list, x)
	if len(s.list) == 8 {
		s.set = make(map[string]int16)
		for n, u := range s.list {
			s.set[string(u)] = int16(n)
		}
	}

	return n
}

type valueSet struct {
	_ [0]func() // no equality

	set  map[value]int16
	list []value
}

func newValueSet(cap int) valueSet {
	return valueSet{list: make([]value, 0, cap)}
}

func (s *valueSet) reset() {
	clear(s.set)
	clear(s.list)
	s.list = s.list[:0]
}

func (s *valueSet) add(x value) (n int16) {
	if s.set != nil {
		if n, ok := s.set[x]; ok {
			return n
		}
		n := int16(len(s.list))
		s.list = append(s.list, x)
		s.set[x] = n
		return n
	}

	for n, u := range s.list {
		if x == u {
			return int16(n)
		}
	}

	n = int16(len(s.list))
	if cap(s.list) == 0 {
		s.list = make([]value, 0, 8)
	}
	s.list = append(s.list, x)
	if len(s.list) == 8 {
		s.set = make(map[value]int16)
		for n, u := range s.list {
			s.set[u] = int16(n)
		}
	}

	return n
}
