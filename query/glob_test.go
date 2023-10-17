package query

import (
	"testing"

	"github.com/zeebo/assert"
)

func TestGlob(t *testing.T) {
	matches := func(pattern, scrut string) bool {
		glob, ok := makeGlob(pattern)
		assert.That(t, ok)
		return glob(b(scrut))
	}

	assert.That(t, !matches(`12X\*`, "12Xa"))
	assert.That(t, matches(`12X\*`, "12X*"))

	assert.That(t, matches(`^12X`, "12Xyab"))
	assert.That(t, !matches(`^12X`, "a12Xyab"))

	assert.That(t, matches(`^`, "foo"))
	assert.That(t, matches(`$`, "foo"))
}
