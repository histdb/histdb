package query

import (
	"bytes"
	"strings"
)

func makeGlob(pattern string) (func([]byte) bool, bool) {
	// if we have no special characters, optimize to bytes.Contains
	if !strings.ContainsAny(pattern, `*?^$`) {
		return func(scrut []byte) bool {
			return bytes.Contains(scrut, []byte(pattern))
		}, true
	}

	// TODO: this is a hacky way to handle rooting glob matches. it should
	// definitely be able to be put into the algorithm directly when i
	// have more brain power.
	if len(pattern) > 0 {
		if pattern[0] == '^' {
			pattern = pattern[1:]
		} else {
			pattern = "*" + pattern
		}
		if pattern[len(pattern)-1] == '$' {
			pattern = pattern[:len(pattern)-1]
		} else {
			pattern = pattern + "*"
		}
	}

	// check for well formed escapes
	for i := uint(0); i < uint(len(pattern)); i++ {
		if pattern[i] != '\\' {
			continue
		}
		i++
		if i >= uint(len(pattern)) {
			return nil, false
		}
		// check for invalid escapes
		if pattern[i] != '*' && pattern[i] != '?' && pattern[i] != '\\' {
			return nil, false
		}
	}

	return func(scrut []byte) (match bool) {
		nx, px := uint(0), uint(0)
		npx, nnx := uint(0), uint(0)

		for px < uint(len(pattern)) || nx < uint(len(scrut)) {
			if px < uint(len(pattern)) {
				switch c := pattern[px]; c {
				case '?': // single-character wildcard
					if nx < uint(len(scrut)) {
						px++
						nx++
						continue
					}

				case '*': // zero-or-more-character wildcard

					// trailing wildcard optimization
					if px+1 >= uint(len(pattern)) {
						return true
					}

					// set the reset point if a match fails
					npx, nnx = px, nx+1

					px++
					continue

				case '\\': // escape character
					px++

					if px >= uint(len(pattern)) {
						// invalid pattern caught above so this should never happen.
						return false
					}
					c = pattern[px]

					fallthrough

				default: // ordinary character
					if nx < uint(len(scrut)) && scrut[nx] == c {
						px++
						nx++
						continue
					}
				}
			}

			// restart if possible
			if 0 < nnx && nnx <= uint(len(scrut)) {
				px, nx = npx, nnx
				continue
			}

			return false
		}

		return true
	}, true
}
