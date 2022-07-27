package metrics

import "strings"

func PopTag(tags string) (tkey, tag string, isKey bool, rest string) {
	// find the first unescaped ','
	for j := uint(0); j < uint(len(tags)); {
		i := strings.IndexByte(tags[j:], ',')
		if i < 0 {
			break
		}

		// walk backwards counting the number of \
		ui := uint(i)
		for ui-1 < uint(len(tags)) && tags[ui-1] == '\\' {
			ui--
		}

		// an odd number of \ means it is escaped
		if (uint(i)-ui)%2 == 1 {
			j = uint(i) + 1
			continue
		}

		idx := uint(i) + j
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

		// walk backwards counting the number of \
		ui := uint(i)
		for ui-1 < uint(len(tkey)) && tkey[ui-1] == '\\' {
			ui--
		}

		// an odd number of \ means it is escaped
		if (uint(i)-ui)%2 == 1 {
			j = uint(i) + 1
			continue
		}

		tkey, isKey = tkey[:uint(i)+j], false
		break
	}

	// if the tag has an empty string value, then drop the trailing =
	// this is so that `foo=` and `foo` are the same.
	if len(tags) == len(tkey)+1 && tags[len(tags)-1] == '=' {
		tags, isKey = tags[:len(tags)-1], false
	}

	return tkey, tags, isKey, rest
}

func addSet[T comparable](l []T, s map[T]struct{}, v T) ([]T, map[T]struct{}, bool) {
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
		s = make(map[T]struct{})
		for _, u := range l {
			s[u] = struct{}{}
		}
	}

	return l, s, true
}
