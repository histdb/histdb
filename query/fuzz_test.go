package query

import (
	"testing"

	"github.com/histdb/histdb/memindex"
)

func FuzzParseQuery(f *testing.F) {
	f.Add(b(`(foo=foo & bar=wif) | (baz=baz & bar=baz)`))
	f.Add(b(`{foo=foo & bar=wif} | ({baz,bar | baz=baz} & {baz,bar | bar=baz})`))
	f.Add(b(`|`))

	var idx memindex.T
	var q Q

	f.Fuzz(func(t *testing.T, query []byte) {
		err := Parse(query, &q)
		if err == nil {
			_ = q.Eval(&idx)
		}
	})
}
