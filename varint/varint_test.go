package varint

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/zeebo/assert"
	"github.com/zeebo/mwc"

	"github.com/histdb/histdb/buffer"
)

func TestVarint(t *testing.T) {
	t.Run("Safe", func(t *testing.T) {
		for i := uint(0); i <= 64; i++ {
			buf := buffer.Of(make([]byte, 9))

			nbytes := Append(buf.Front9(), 1<<i-1)
			buf = buf.Advance(nbytes)
			dec, _, ok := SafeConsume(buf.Reset())

			t.Logf("%-2d %064b %08b\n", i, dec, buf.Prefix())

			assert.That(t, ok)
			assert.Equal(t, uint64(1<<i-1), dec)
		}
	})

	t.Run("Fast", func(t *testing.T) {
		for i := uint(0); i <= 64; i++ {
			buf := buffer.Of(make([]byte, 9))

			nbytes := Append(buf.Front9(), 1<<i-1)
			buf = buf.Advance(nbytes)
			_, dec := FastConsume(buf.Reset().Front9())

			t.Logf("%-2d %064b %08b\n", i, dec, buf.Prefix())

			assert.Equal(t, uint64(1<<i-1), dec)
		}
	})

	t.Run("FastDirty", func(t *testing.T) {
		rng := mwc.Rand()

		for i := uint(0); i <= 64; i++ {
			buf := buffer.Of(make([]byte, 9))

			nbytes := Append(buf.Front9(), 1<<i-1)
			for i := nbytes; i < 9; i++ {
				*buf.Index(uintptr(i)) = uint8(rng.Uint64())
			}

			buf = buf.Advance(nbytes)
			_, dec := FastConsume(buf.Reset().Front9())

			t.Logf("%-2d %064b %08b\n", i, dec, buf.Prefix())

			assert.Equal(t, uint64(1<<i-1), dec)
		}
	})

	t.Run("RandomSafe", func(t *testing.T) {
		rng := mwc.Rand()

		for nb := 1; nb <= 9; nb++ {
			mask := uint64(1)<<(7*nb) - 1
			if nb == 9 {
				mask = 1<<64 - 1
			}

			for i := 0; i < 10; i++ {
				exp := rng.Uint64() & mask
				buf := buffer.Of(make([]byte, 9))

				nbytes := Append(buf.Front9(), exp)
				buf = buf.Advance(nbytes)
				dec, _, ok := SafeConsume(buf.Reset())

				t.Logf("%-2d %064b %08b\n", i, dec, buf.Prefix())

				assert.That(t, ok)
				assert.Equal(t, exp, dec)
			}
		}
	})

	t.Run("RandomFast", func(t *testing.T) {
		rng := mwc.Rand()

		for nb := 1; nb <= 9; nb++ {
			mask := uint64(1)<<(7*nb) - 1
			if nb == 9 {
				mask = 1<<64 - 1
			}

			for i := 0; i < 10; i++ {
				exp := rng.Uint64() & mask
				buf := buffer.Of(make([]byte, 9))

				nbytes := Append(buf.Front9(), exp)
				buf = buf.Advance(nbytes)
				_, dec := FastConsume(buf.Reset().Front9())

				t.Logf("%-2d %064b %08b\n", i, dec, buf.Prefix())

				assert.Equal(t, exp, dec)
			}
		}
	})
}

func BenchmarkVarint(b *testing.B) {
	rng := mwc.Rand()

	randVals := make([]uint64, 1024*1024)
	for i := range randVals {
		randVals[i] = uint64(1<<rng.Uint32n(65) - 1)
	}
	randBuf := buffer.Of(make([]byte, 16))
	for _, val := range randVals {
		randBuf = randBuf.Grow()
		nbytes := Append(randBuf.Front9(), val)
		randBuf = randBuf.Advance(nbytes)
	}
	randBuf = randBuf.Reset()

	b.Run("Append", func(b *testing.B) {
		for _, i := range []uint{1, 64} {
			b.Run(fmt.Sprint(i), func(b *testing.B) {
				n := uint64(1<<i - 1)
				buf := buffer.Of(make([]byte, 16))

				for i := 0; i < b.N; i++ {
					buf = buf.Grow()
					Append(buf.Front9(), n)
				}
			})
		}

		b.Run("Rand", func(b *testing.B) {
			buf := buffer.Of(make([]byte, 16))

			for i := 0; i < b.N; i++ {
				buf = buf.Grow()
				Append(buf.Front9(), randVals[i%(1024*1024)])
			}
		})
	})

	b.Run("Consume", func(b *testing.B) {
		for _, i := range []uint{1, 64} {
			b.Run(fmt.Sprint(i), func(b *testing.B) {
				n := uint64(1<<i - 1)
				buf := buffer.Of(make([]byte, 9))
				nbytes := Append(buf.Front9(), n)
				buf = buf.Advance(nbytes)

				for i := 0; i < b.N; i++ {
					SafeConsume(buf)
				}
			})
		}

		b.Run("Rand", func(b *testing.B) {
			buf := randBuf.Reset()
			for i := 0; i < b.N; i++ {
				if buf.Remaining() == 0 {
					buf = buf.Reset()
				}
				_, buf, _ = SafeConsume(buf)
			}
		})
	})

	b.Run("FastConsume", func(b *testing.B) {
		for _, i := range []uint{1, 64} {
			b.Run(fmt.Sprint(i), func(b *testing.B) {
				n := uint64(1<<i - 1)
				buf := buffer.Of(make([]byte, 9))
				nbytes := Append(buf.Front9(), n)
				buf = buf.Advance(nbytes)

				var dec uint64
				for i := 0; i < b.N; i++ {
					_, dec = FastConsume(buf.Front9())
				}
				runtime.KeepAlive(dec)
			})
		}

		b.Run("Rand", func(b *testing.B) {
			var nbytes uintptr
			var dec uint64

			buf := randBuf.Reset()
			for i := 0; i < b.N; i++ {
				if buf.Remaining() < 9 {
					buf = buf.Reset()
				}
				nbytes, dec = FastConsume(buf.Front9())
				buf = buf.Advance(nbytes)
			}

			runtime.KeepAlive(nbytes)
			runtime.KeepAlive(dec)
		})
	})
}
