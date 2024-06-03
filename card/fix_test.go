package card

import (
	"testing"

	"github.com/zeebo/assert"
)

func bs(s string) []byte { return []byte(s) }

func TestFixer(t *testing.T) {
	var cf Fixer

	cf.DropTagKey(bs(`interface`))
	cf.RewriteTag(bs(`error_name`), bs(`Node\ ID:`), bs(`error_name=fixed`))

	{
		res := cf.Fix(bs(`error_name`), bs(`error_name=Node\ ID: foo`))
		assert.Equal(t, string(res), `error_name=fixed`)
	}

	{
		res := cf.Fix(bs(`error_name`), bs(`error_name=something`))
		assert.Equal(t, string(res), `error_name=something`)
	}

	{
		res := cf.Fix(bs(`interface`), bs(`interface=foo`))
		assert.Equal(t, string(res), ``)
	}
}
