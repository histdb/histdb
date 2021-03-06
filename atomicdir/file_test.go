package atomicdir

import (
	"testing"

	"github.com/zeebo/assert"
)

func TestFileName(t *testing.T) {
	cases := []struct {
		Expect string
		File   File
	}{
		{"L00-K00-00000000", File{}},
		{"L01-K00-00000000", File{Level: 1}},
		{"LFF-K00-00000000", File{Level: 255}},
		{"L00-K01-00000000", File{Kind: 1}},
		{"L00-KFF-00000000", File{Kind: 255}},
		{"L00-K00-00000001", File{Generation: 1}},
		{"L00-K00-FFFFFFFF", File{Generation: ^uint32(0)}},
	}

	for _, tc := range cases {
		assert.Equal(t, tc.Expect, tc.File.String())
		f, ok := parseFile(tc.Expect)
		assert.That(t, ok)
		assert.Equal(t, tc.File, f)
	}
}

func BenchmarkFileName(b *testing.B) {
	b.Run("Name", func(b *testing.B) {
		b.Run("Easy", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = File{}.String()
			}
		})

		b.Run("Hard", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = File{
					Level:      ^uint8(0),
					Kind:       ^uint8(0),
					Generation: ^uint32(0),
				}.String()
			}
		})
	})

	b.Run("Parse", func(b *testing.B) {
		b.Run("Easy", func(b *testing.B) {
			name := File{}.String()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, _ = parseFile(name)
			}
		})

		b.Run("Hard", func(b *testing.B) {
			name := File{
				Level:      ^uint8(0),
				Kind:       ^uint8(0),
				Generation: ^uint32(0),
			}.String()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, _ = parseFile(name)
			}
		})
	})
}
