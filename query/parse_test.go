package query

import (
	"testing"

	"github.com/zeebo/assert"
)

// const parseQuery = `{foo=foo,bar=wif} | {(baz=baz & bar=baz) | whatever =* foo}`
const parseQuery = `inst !* 12z & name='(*Dir).Commit' & field=successes`

func TestParse(t *testing.T) {
	q := new(Q)
	err := Parse(b(parseQuery), q)
	assert.NoError(t, err)
	t.Logf("prog: %v\n", q.prog)
	t.Logf("strs: %q\n", q.strs.list)
	t.Logf("mchs: %v\n", q.mchs)
}

func BenchmarkParse(b *testing.B) {
	query := []byte(parseQuery)
	into := new(Q)

	b.ReportAllocs()

	for b.Loop() {
		_ = Parse(query, into)
	}
}
