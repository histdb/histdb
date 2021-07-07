package leveln

import (
	"encoding/binary"
	"testing"

	"github.com/zeebo/assert"
	"github.com/zeebo/pcg"

	"github.com/histdb/histdb"
	"github.com/histdb/histdb/testhelp"
)

func TestKeyReader(t *testing.T) {
	const count = 1e5

	fs, cleanup := testhelp.FS(t)
	defer cleanup()

	fh, cleanup := testhelp.Tempfile(t, fs)
	defer cleanup()

	var kw keyWriter
	kw.Init(fh)

	for i := 0; i < count; i++ {
		var key histdb.Key
		binary.BigEndian.PutUint32(key[16:20], uint32(i*2+1))
		var ent kwEntry
		ent.Set(key, uint32(i), uint32(i))
		kw.Append(ent)
	}
	assert.NoError(t, kw.Finish())

	var kr keyReader
	kr.Init(fh)

	check := func(i int, key histdb.Key) func(uint32, uint32, bool, error) {
		return func(offset, length uint32, ok bool, err error) {
			t.Helper()
			assert.NoError(t, err)
			assert.Equal(t, i, offset)
			assert.Equal(t, i, length)
			assert.That(t, ok)
		}
	}

	for i := 0; i < count; i++ {
		var key histdb.Key
		binary.BigEndian.PutUint32(key[16:20], uint32(i*2+1))
		check(i, key)(kr.Search(key))
		binary.BigEndian.PutUint32(key[16:20], uint32(i*2+2))
		check(i, key)(kr.Search(key))
	}

	var key histdb.Key
	_, _, ok, err := kr.Search(key)
	assert.NoError(t, err)
	assert.That(t, !ok)
}

func BenchmarkKeyReader(b *testing.B) {
	run := func(b *testing.B, n int) {
		fs, cleanup := testhelp.FS(b)
		defer cleanup()

		fh, cleanup := testhelp.Tempfile(b, fs)
		defer cleanup()

		var rng pcg.T

		var kw keyWriter
		kw.Init(fh)

		for i := 0; i < n; i++ {
			var ent kwEntry
			ent.Set(testhelp.KeyN(uint32(i)), uint32(i), uint32(i))
			kw.Append(ent)
		}
		assert.NoError(b, kw.Finish())

		var key histdb.Key
		var kr keyReader
		kr.Init(fh)

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			binary.BigEndian.PutUint64(key[0:8], rng.Uint64())
			_, _, _, _ = kr.Search(key)
		}

		b.ReportMetric(float64(kr.stats.reads)/float64(b.N), "reads/op")
		b.ReportMetric(float64(kr.stats.reads)/float64(b.N)*kwPageSize, "bytes/op")
	}

	b.Run("1e2", func(b *testing.B) { run(b, 1e2) })
	b.Run("1e3", func(b *testing.B) { run(b, 1e3) })
	b.Run("1e4", func(b *testing.B) { run(b, 1e4) })
	b.Run("1e5", func(b *testing.B) { run(b, 1e5) })
	b.Run("1e6", func(b *testing.B) { run(b, 1e6) })
	b.Run("1e7", func(b *testing.B) { run(b, 1e7) })
}
