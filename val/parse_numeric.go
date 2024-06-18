package val

func ParseInt(s []byte) (v int64, ok bool) {
	if len(s) == 0 {
		return 0, false
	}

	const cutoff = 1 << 63

	neg := false
	if s[0] == '-' {
		neg, s = true, s[1:]
	}

	uv, ok := ParseUint(s)
	if !ok {
		return 0, false
	}

	if !neg && uv >= cutoff {
		return 0, false
	} else if neg && uv > cutoff {
		return 0, false
	}

	v = int64(uv)
	if neg {
		v = -v
	}

	return v, true
}

func ParseUint(s []byte) (v uint64, ok bool) {
	if len(s) == 0 {
		return 0, false
	}

	const (
		maxVal uint64 = 1<<64 - 1
		cutoff uint64 = maxVal/10 + 1
	)

	for _, c := range s {
		d := c - '0'
		if d >= 10 {
			return 0, false
		} else if v >= cutoff {
			return 0, false
		}
		v *= 10
		n1 := v + uint64(d)
		if n1 < v || n1 > maxVal {
			return 0, false
		}
		v = n1
	}
	return v, true
}
