package query

import (
	"fmt"
	"testing"

	"github.com/zeebo/assert"
)

var tokenCases = []struct {
	in  string
	out []string
}{
	0:  {`{foo, bar | foo == bar}` /**/, []string{`{`, `foo`, `,`, `bar`, `|`, `foo`, `==`, `bar`, `}`}},
	1:  {`{foo\} | 2 == foo\}}` /*   */, []string{`{`, `foo\}`, `|`, `2`, `==`, `foo\}`, `}`}},
	2:  {`"foo"` /*                  */, []string{`foo`}},
	3:  {`"foo'"` /*                 */, []string{`foo'`}},
	4:  {`"foo\""` /*                */, []string{`foo\"`}},
	5:  {`"foo\\"` /*                */, []string{`foo\\`}},
	6:  {`'foo'` /*                  */, []string{`foo`}},
	7:  {`'foo"'` /*                 */, []string{`foo"`}},
	8:  {`'foo\''` /*                */, []string{`foo\'`}},
	9:  {`'foo\\'` /*                */, []string{`foo\\`}},
	10: {`  foo="foo"  ` /*          */, []string{`foo`, `=`, `foo`}},
}

func TestToken(t *testing.T) {
	collect := func(t *testing.T, x string) (out []string) {
		bx := b(x)
		assert.NoError(t, tokens(bx, func(tk token) {
			t.Log(x, "=>", tk)
			if tk.isLiteral() {
				out = append(out, string(tk.literal(bx)))
			} else {
				out = append(out, tk.String())
			}
		}))
		return out
	}

	for i, c := range tokenCases {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			assert.Equal(t, collect(t, c.in), c.out)
		})
	}
}
