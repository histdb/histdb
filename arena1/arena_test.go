package arena1

import (
	"testing"

	"github.com/zeebo/assert"
)

func TestArena(t *testing.T) {
	const (
		lBatch = 1024
		lAlloc = 8
	)

	var s T[int64]

	p1 := s.New()
	p2 := s.New()

	*s.Get(p1) = 5
	*s.Get(p2) = 6

	assert.Equal(t, *s.Get(p1), 5)
	assert.Equal(t, *s.Get(p2), 6)

	for range 70 * lAlloc * lBatch {
		*s.Get(s.New()) = 1
	}

	p3 := s.New()
	p4 := s.New()

	*s.Get(p3) = 7
	*s.Get(p4) = 8

	assert.Equal(t, *s.Get(p1), 5)
	assert.Equal(t, *s.Get(p2), 6)
	assert.Equal(t, *s.Get(p3), 7)
	assert.Equal(t, *s.Get(p4), 8)

	assert.Equal(t, s.Allocated(), 70*lAlloc*lBatch+4)
}
