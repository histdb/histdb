package memindex

func addUint32Set(l []uint32, s map[uint32]struct{}, v uint32) ([]uint32, map[uint32]struct{}, bool) {
	if s != nil {
		if _, ok := s[v]; ok {
			return l, s, false
		}
		l = append(l, v)
		s[v] = struct{}{}
		return l, s, true
	}

	for _, u := range l {
		if u == v {
			return l, s, false
		}
	}

	l = append(l, v)
	if len(l) == cap(l) {
		s = make(map[uint32]struct{})
		for _, u := range l {
			s[u] = struct{}{}
		}
	}

	return l, s, true
}
