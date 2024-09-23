package hashany

import "testing"

func TestHash(t *testing.T) {
	_ = Hash("")
	_ = Hash(0)
}

func BenchmarkHash(b *testing.B) {
	b.Run("string", func(b *testing.B) {
		b.ReportAllocs()

		for range b.N {
			_ = Hash("")
		}
	})

	b.Run("int", func(b *testing.B) {
		b.ReportAllocs()

		for range b.N {
			_ = Hash(0)
		}
	})
}
