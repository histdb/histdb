package query

import (
	"testing"

	"github.com/zeebo/assert"

	"github.com/histdb/histdb/memindex"
)

func FuzzParseQuery(f *testing.F) {
	f.Add(b(`(foo=foo & bar=wif) | (baz=baz & bar=baz)`))
	f.Add(b(`{foo=foo & bar=wif} | ({baz,bar | baz=baz} & {baz,bar | bar=baz})`))
	f.Add(b(`|`))
	f.Fuzz(func(t *testing.T, query []byte) {
		q, err := Parse(query)
		if err == nil {
			var idx memindex.T
			_, err := q.Eval(&idx)
			assert.NoError(t, err)
		}
	})
}
