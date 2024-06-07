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
		{"L00K00G00000001-00000000", File{GenLow: 1}},
		{"L00K00GFFFFFFFF-00000000", File{GenLow: ^uint32(0)}},

		{"L00K00G00000000-00000001", File{GenHigh: 1}},
		{"L00K00G00000000-FFFFFFFF", File{GenHigh: ^uint32(0)}},

		{"L01K00G00000000-00000000", File{Level: 1}},
		{"LFFK00G00000000-00000000", File{Level: ^uint8(0)}},

		{"L00K01G00000000-00000000", File{Kind: 1}},
		{"L00KFFG00000000-00000000", File{Kind: ^uint8(0)}},

		{"L00K00G00000000-00000000", File{}},
		{"LFFKFFGFFFFFFFF-FFFFFFFF", File{
			GenLow:  ^uint32(0),
			GenHigh: ^uint32(0),
			Level:   ^uint8(0),
			Kind:    ^uint8(0),
		}},
	}

	for _, tc := range cases {
		assert.Equal(t, tc.Expect, tc.File.String())
		f, ok := ParseFile(tc.Expect)
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
					GenLow:  ^uint32(0),
					GenHigh: ^uint32(0),
					Level:   ^uint8(0),
					Kind:    ^uint8(0),
				}.String()
			}
		})
	})

	b.Run("Parse", func(b *testing.B) {
		b.Run("Easy", func(b *testing.B) {
			name := File{}.String()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, _ = ParseFile(name)
			}
		})

		b.Run("Hard", func(b *testing.B) {
			name := File{
				GenLow:  ^uint32(0),
				GenHigh: ^uint32(0),
				Level:   ^uint8(0),
				Kind:    ^uint8(0),
			}.String()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, _ = ParseFile(name)
			}
		})
	})
}
