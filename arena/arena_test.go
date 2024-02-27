package arena

import (
	"testing"

	"github.com/zeebo/assert"
)

func TestArena(t *testing.T) {
	var s T[int]

	p1 := s.New()
	p2 := s.New()

	*s.Get(p1) = 5
	*s.Get(p2) = 6

	assert.Equal(t, *s.Get(p1), 5)
	assert.Equal(t, *s.Get(p2), 6)

	for i := 0; i < 70*lAlloc*lBatch; i++ {
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
}
