package query

import (
	"fmt"
	"testing"

	"github.com/zeebo/assert"
)

// const parseQuery = `{foo=foo,bar=wif} | {(baz=baz & bar=baz) | whatever =* foo}`
const parseQuery = `inst !* 12z & name='(*Dir).Commit' & field=successes`

func TestParse(t *testing.T) {
	q := new(Query)
	err := Parse(b(parseQuery), q)
	assert.NoError(t, err)
	fmt.Printf("prog: %v\n", q.prog)
	fmt.Printf("strs: %q\n", q.strs.list)
	fmt.Printf("vals: %v\n", q.vals.list)
	fmt.Printf("mchs: %v\n", q.mchs)
}

func BenchmarkParse(b *testing.B) {
	query := []byte(parseQuery)
	into := new(Query)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = Parse(query, into)
	}
}
