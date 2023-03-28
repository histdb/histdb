package rwutils

import (
	"testing"

	"github.com/histdb/histdb/buffer"
	"github.com/zeebo/assert"
	"github.com/zeebo/mwc"
)

func testRoundTrip[T any](
	t *testing.T,
	write func(*W, T),
	read func(*R) T,
	gen func(*mwc.T) T,
) {
	var (
		rng = mwc.Rand()
		w   W
		r   R
		vs  []T
	)

	w.Init(buffer.T{})
	for i := 0; i < 100; i++ {
		v := gen(rng)
		write(&w, v)
		vs = append(vs, v)
	}

	r.Init(w.Done().Reset())
	for _, v := range vs {
		assert.Equal(t, read(&r), v)
	}
	_, err := r.Done()
	assert.NoError(t, err)
}

func TestReadWriter(t *testing.T) {
	t.Run("Varint", func(t *testing.T) {
		testRoundTrip(t, (*W).Varint, (*R).Varint, func(rng *mwc.T) uint64 {
			return rng.Uint64n(1 << rng.Uint64n(64))
		})
	})

	t.Run("Uint64", func(t *testing.T) {
		testRoundTrip(t, (*W).Uint64, (*R).Uint64, (*mwc.T).Uint64)
	})

	t.Run("Uint32", func(t *testing.T) {
		testRoundTrip(t, (*W).Uint32, (*R).Uint32, (*mwc.T).Uint32)
	})

	t.Run("Uint16", func(t *testing.T) {
		testRoundTrip(t, (*W).Uint16, (*R).Uint16, func(rng *mwc.T) uint16 {
			return uint16(rng.Uint32())
		})
	})

	t.Run("Uint8", func(t *testing.T) {
		testRoundTrip(t, (*W).Uint8, (*R).Uint8, func(rng *mwc.T) uint8 {
			return uint8(rng.Uint32())
		})
	})
}
