package atomicdir

import (
	"testing"

	"github.com/zeebo/assert"
	"github.com/zeebo/pcg"
)

func TestFileName(t *testing.T) {
	cases := []struct {
		Name string
		File File
	}{
		{"tempdata/L00-00000000", File{}},
		{"GEN-0001/L00-00000000", File{Transaction: 1}},
		{"GEN-FFFF/L00-00000000", File{Transaction: ^uint16(0)}},
		{"tempdata/WAL-00000000", File{Level: -1}},
		{"tempdata/L01-00000000", File{Level: 1}},
		{"tempdata/L7F-00000000", File{Level: 127}},
		{"tempdata/L00-00000001", File{Generation: 1}},
		{"tempdata/L00-FFFFFFFF", File{Generation: ^uint32(0)}},
	}

	for _, tc := range cases {
		assert.Equal(t, tc.Name, tc.File.Name())
		f, ok := parseFile(tc.Name)
		assert.That(t, ok)
		assert.Equal(t, tc.File, f)
	}
}

func TestReadWriteUint(t *testing.T) {
	var rng pcg.T
	for i := 0; i < 10000; i++ {
		buf := []byte("00000000")
		v1 := rng.Uint32()
		writeUint(buf, v1)
		v2, ok := readUint(string(buf), true)
		assert.That(t, ok)
		assert.Equal(t, v1, v2)
	}
}

func BenchmarkFileName(b *testing.B) {
	b.Run("Name", func(b *testing.B) {
		b.Run("Easy", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = File{Level: -1}.Name()
			}
		})

		b.Run("Hard", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = File{Level: 99, Generation: ^uint32(0), Transaction: ^uint16(0)}.Name()
			}
		})
	})

	b.Run("Parse", func(b *testing.B) {
		b.Run("Easy", func(b *testing.B) {
			name := File{Level: -1}.Name()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, _ = parseFile(name)
			}
		})

		b.Run("Hard", func(b *testing.B) {
			name := File{Level: 99, Generation: ^uint32(0), Transaction: ^uint16(0)}.Name()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, _ = parseFile(name)
			}
		})
	})
}
