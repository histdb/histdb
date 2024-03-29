package query

import (
	"fmt"
	"testing"

	"github.com/zeebo/assert"
)

// const parseQuery = `{foo=foo,bar=wif} | {(baz=baz & bar=baz) | whatever =* foo}`
const parseQuery = `inst !* 12z & name='(*Dir).Commit' & field=successes`

func TestParse(t *testing.T) {
	e, err := Parse(b(parseQuery))
	assert.NoError(t, err)
	fmt.Printf("prog: %v\n", e.prog)
	fmt.Printf("strs: %q\n", e.strs)
	fmt.Printf("vals: %v\n", e.vals)
	fmt.Printf("mchs: %v\n", e.mchs)
}

func BenchmarkParse(b *testing.B) {
	query := []byte(parseQuery)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Parse(query)
	}
}
