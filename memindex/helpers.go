package memindex

import "strings"

func popTag(tags string) (tkey, tag string, isKey bool, rest string) {
	// find the first unescaped ','
	for j := uint(0); j < uint(len(tags)); {
		i := strings.IndexByte(tags[j:], ',')
		if i < 0 {
			break
		}
		ui := uint(i)

		if ui > 0 && ui-1 < uint(len(tags)) && tags[ui-1] == '\\' {
			j = ui + 1
			continue
		}

		idx := ui + j
		tags, rest = tags[:idx], tags[idx+1:]
		break
	}

	// if there's no =, then the tag key is the tag
	tkey, isKey = tags, true

	// find the first unescaped '='
	for j := uint(0); j < uint(len(tkey)); {
		i := strings.IndexByte(tkey[j:], '=')
		if i < 0 {
			break
		}
		ui := uint(i)

		if ui > 0 && ui-1 < uint(len(tkey)) && tkey[ui-1] == '\\' {
			j = ui + 1
			continue
		}

		tkey, isKey = tkey[:ui+j], false
		break
	}

	// if the tag has an empty string value, then drop the trailing =
	if len(tags) == len(tkey)+1 && tags[len(tags)-1] == '=' {
		tags, isKey = tags[:len(tags)-1], false
	}

	return tkey, tags, isKey, rest
}

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
