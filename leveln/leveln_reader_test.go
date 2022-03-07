package leveln

import (
	"testing"

	"github.com/zeebo/assert"
	"github.com/zeebo/mwc"

	"github.com/histdb/histdb"
	"github.com/histdb/histdb/testhelp"
)

func TestLevelNSeek(t *testing.T) {
	const count = 1e4

	fs, cleanup := testhelp.FS(t)
	defer cleanup()

	keys, cleanup := testhelp.Tempfile(t, fs)
	defer cleanup()

	values, cleanup := testhelp.Tempfile(t, fs)
	defer cleanup()

	var lnw Writer
	lnw.Init(keys, values)

	for i := 0; i < count; i++ {
		key := testhelp.KeyFrom(uint64(i)/8, 0, uint32(i))
		assert.NoError(t, lnw.Append(key, []byte{byte(i >> 8), byte(i)}))
	}
	assert.NoError(t, lnw.Finish())

	var lnr Reader
	lnr.Init(keys, values)

	var it Iterator
	lnr.InitIterator(&it)

	for i := 0; i < count; i++ {
		key := testhelp.KeyFrom(uint64(i)/8, 0, uint32(i))

		assert.That(t, it.Seek(key))
		assert.Equal(t, key, it.Key())
		assert.Equal(t, it.Value()[0], byte(i>>8))
		assert.Equal(t, it.Value()[1], byte(i))

		if i%8 == 7 && i != count-1 {
			assert.That(t, it.Seek(testhelp.KeyFrom(uint64(i)/8, 0, uint32(i+1))))
			key := testhelp.KeyFrom(uint64(i+1)/8, 0, uint32(i+1))
			assert.Equal(t, key.String(), it.Key().String())
			assert.Equal(t, it.Value()[0], byte((i+1)>>8))
			assert.Equal(t, it.Value()[1], byte(i+1))
		}
	}
	assert.NoError(t, it.Err())
}

func TestLevelNSeekBoundaries(t *testing.T) {
	fs, cleanup := testhelp.FS(t)
	defer cleanup()

	keys, cleanup := testhelp.Tempfile(t, fs)
	defer cleanup()

	values, cleanup := testhelp.Tempfile(t, fs)
	defer cleanup()

	var lnw Writer
	lnw.Init(keys, values)

	assert.NoError(t, lnw.Append(histdb.Key{0: 0x10, 16: 0x30}, nil))
	assert.NoError(t, lnw.Append(histdb.Key{0: 0x10, 16: 0x40}, nil))
	assert.NoError(t, lnw.Append(histdb.Key{0: 0x20, 16: 0x30}, nil))
	assert.NoError(t, lnw.Append(histdb.Key{0: 0x20, 16: 0x40}, nil))
	assert.NoError(t, lnw.Finish())

	var lnr Reader
	lnr.Init(keys, values)

	var it Iterator
	lnr.InitIterator(&it)

	assert.NoError(t, it.Err())

	assert.That(t, it.Seek(histdb.Key{0: 0x00, 16: 0x00}))
	assert.Equal(t, histdb.Key{0: 0x10, 16: 0x30}.String(), it.Key().String())

	assert.That(t, it.Seek(histdb.Key{0: 0x10, 16: 0x00}))
	assert.Equal(t, histdb.Key{0: 0x10, 16: 0x30}.String(), it.Key().String())

	assert.That(t, it.Seek(histdb.Key{0: 0x10, 16: 0x35}))
	assert.Equal(t, histdb.Key{0: 0x10, 16: 0x40}.String(), it.Key().String())

	assert.That(t, it.Seek(histdb.Key{0: 0x10, 16: 0x45}))
	assert.Equal(t, histdb.Key{0: 0x20, 16: 0x30}.String(), it.Key().String())

	assert.That(t, it.Seek(histdb.Key{0: 0x20, 16: 0x00}))
	assert.Equal(t, histdb.Key{0: 0x20, 16: 0x30}.String(), it.Key().String())

	assert.That(t, it.Seek(histdb.Key{0: 0x20, 16: 0x35}))
	assert.Equal(t, histdb.Key{0: 0x20, 16: 0x40}.String(), it.Key().String())

	assert.That(t, !it.Seek(histdb.Key{0: 0x20, 16: 0x45}))
}

func BenchmarkLevelNReader(b *testing.B) {
	run := func(b *testing.B, n uint64) {
		fs, cleanup := testhelp.FS(b)
		defer cleanup()

		keys, cleanup := testhelp.Tempfile(b, fs)
		defer cleanup()

		values, cleanup := testhelp.Tempfile(b, fs)
		defer cleanup()

		rng := mwc.Rand()

		var lnw Writer
		lnw.Init(keys, values)

		for i := uint64(0); i < n; i++ {
			key := testhelp.KeyFrom(i/512, 0, uint32(i))
			assert.NoError(b, lnw.Append(key, nil))
		}
		assert.NoError(b, lnw.Finish())

		var lnr Reader
		lnr.Init(keys, values)

		var it Iterator
		lnr.InitIterator(&it)

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			key := testhelp.KeyFrom(rng.Uint64n(n)/512, 0, uint32(n))
			it.Seek(key)
		}
	}

	b.Run("1e2", func(b *testing.B) { run(b, 1e2) })
	b.Run("1e3", func(b *testing.B) { run(b, 1e3) })
	b.Run("1e4", func(b *testing.B) { run(b, 1e4) })
	b.Run("1e5", func(b *testing.B) { run(b, 1e5) })
	b.Run("1e6", func(b *testing.B) { run(b, 1e6) })
	b.Run("1e7", func(b *testing.B) { run(b, 1e7) })
}
