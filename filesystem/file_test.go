package filesystem

import (
	"testing"

	"github.com/zeebo/assert"
)

func TestFileName(t *testing.T) {
	cases := []struct {
		Expect string
		File   File
	}{
		{"00000001-00000000.xxxx", File{Low: 1}},
		{"FFFFFFFF-00000000.xxxx", File{Low: ^uint32(0)}},

		{"00000000-00000001.xxxx", File{High: 1}},
		{"00000000-FFFFFFFF.xxxx", File{High: ^uint32(0)}},

		{"00000000-00000000.indx", File{Kind: KindIndx}},
		{"00000000-00000000.keys", File{Kind: KindKeys}},
		{"00000000-00000000.vals", File{Kind: KindVals}},

		{"00000000-00000000.xxxx", File{}},
		{"FFFFFFFF-FFFFFFFF.vals", File{
			Low:  ^uint32(0),
			High: ^uint32(0),
			Kind: KindVals,
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
			for b.Loop() {
				_ = File{}.String()
			}
		})

		b.Run("Hard", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_ = File{
					Low:  ^uint32(0),
					High: ^uint32(0),
					Kind: 3,
				}.String()
			}
		})
	})

	b.Run("Parse", func(b *testing.B) {
		b.Run("Easy", func(b *testing.B) {
			name := File{}.String()
			b.ReportAllocs()
			for b.Loop() {
				_, _ = ParseFile(name)
			}
		})

		b.Run("Hard", func(b *testing.B) {
			name := File{
				Low:  ^uint32(0),
				High: ^uint32(0),
				Kind: 3,
			}.String()
			b.ReportAllocs()
			for b.Loop() {
				_, _ = ParseFile(name)
			}
		})
	})
}
