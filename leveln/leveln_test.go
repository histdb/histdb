package leveln

import (
	"encoding/binary"
	"testing"

	"github.com/zeebo/assert"
	"github.com/zeebo/lsm"
	"github.com/zeebo/lsm/filesystem"
	"github.com/zeebo/lsm/testhelp"
)

func TestLevelNWriterReader(t *testing.T) {
	keys, cleanup := testhelp.Tempfile(t, new(filesystem.T))
	defer cleanup()

	values, cleanup := testhelp.Tempfile(t, new(filesystem.T))
	defer cleanup()

	var lnw Writer
	lnw.Init(keys, values)

	for i := 0; i < 1000; i++ {
		var key lsm.Key
		binary.BigEndian.PutUint64(key[0:8], uint64(i)/8)
		binary.BigEndian.PutUint32(key[16:20], uint32(i))
		assert.NoError(t, lnw.Append(key, []byte{byte(i >> 8), byte(i)}))
	}
	assert.NoError(t, lnw.Finish())

	var lnr Reader
	lnr.Init(keys, values)

	it, i := lnr.Iterator(), 0
	for ; it.Next(); i++ {
		var key lsm.Key
		binary.BigEndian.PutUint64(key[0:8], uint64(i)/8)
		binary.BigEndian.PutUint32(key[16:20], uint32(i))

		assert.Equal(t, key, it.Key())
		assert.Equal(t, it.Value()[0], i/256)
		assert.Equal(t, it.Value()[1], i%256)
	}
	assert.NoError(t, it.Err())
	assert.Equal(t, i, 1000)
}

func TestLevelNSeek(t *testing.T) {
	const count = 1e4

	keys, cleanup := testhelp.Tempfile(t, new(filesystem.T))
	defer cleanup()

	values, cleanup := testhelp.Tempfile(t, new(filesystem.T))
	defer cleanup()

	var lnw Writer
	lnw.Init(keys, values)

	for i := 0; i < count; i++ {
		var key lsm.Key
		binary.BigEndian.PutUint64(key[0:8], uint64(i)/8)
		binary.BigEndian.PutUint32(key[16:20], uint32(i))
		assert.NoError(t, lnw.Append(key, []byte{byte(i >> 8), byte(i)}))
	}
	assert.NoError(t, lnw.Finish())

	var lnr Reader
	lnr.Init(keys, values)

	it := lnr.Iterator()
	for i := 0; i < count; i++ {
		var key lsm.Key
		binary.BigEndian.PutUint64(key[0:8], uint64(i)/8)
		binary.BigEndian.PutUint32(key[16:20], uint32(i))

		assert.That(t, it.Seek(key))
		assert.Equal(t, key, it.Key())
		assert.Equal(t, it.Value()[0], byte(i>>8))
		assert.Equal(t, it.Value()[1], byte(i))

		if i%8 == 7 && i != count-1 {
			binary.BigEndian.PutUint32(key[16:20], uint32(i+1))
			assert.That(t, it.Seek(key))
			binary.BigEndian.PutUint64(key[0:8], uint64(i+1)/8)
			assert.Equal(t, key.String(), it.Key().String())
			assert.Equal(t, it.Value()[0], byte((i+1)>>8))
			assert.Equal(t, it.Value()[1], byte(i+1))
		}
	}
	assert.NoError(t, it.Err())
}

func TestLevelNSeekBoundaries(t *testing.T) {
	keys, cleanup := testhelp.Tempfile(t, new(filesystem.T))
	defer cleanup()

	values, cleanup := testhelp.Tempfile(t, new(filesystem.T))
	defer cleanup()

	var lnw Writer
	lnw.Init(keys, values)

	assert.NoError(t, lnw.Append(lsm.Key{0: 10, 16: 10}, nil))
	assert.NoError(t, lnw.Append(lsm.Key{0: 10, 16: 20}, nil))
	assert.NoError(t, lnw.Append(lsm.Key{0: 20, 16: 10}, nil))
	assert.NoError(t, lnw.Append(lsm.Key{0: 20, 16: 20}, nil))
	assert.NoError(t, lnw.Finish())

	var lnr Reader
	lnr.Init(keys, values)

	it := lnr.Iterator()
	assert.NoError(t, it.Err())

	assert.That(t, it.Seek(lsm.Key{0: 0, 16: 0}))
	assert.Equal(t, lsm.Key{0: 10, 16: 10}.String(), it.Key().String())

	assert.That(t, it.Seek(lsm.Key{0: 10, 16: 0}))
	assert.Equal(t, lsm.Key{0: 10, 16: 10}.String(), it.Key().String())

	assert.That(t, it.Seek(lsm.Key{0: 10, 16: 15}))
	assert.Equal(t, lsm.Key{0: 10, 16: 20}.String(), it.Key().String())

	assert.That(t, it.Seek(lsm.Key{0: 10, 16: 25}))
	assert.Equal(t, lsm.Key{0: 20, 16: 10}.String(), it.Key().String())

	assert.That(t, it.Seek(lsm.Key{0: 20, 16: 0}))
	assert.Equal(t, lsm.Key{0: 20, 16: 10}.String(), it.Key().String())

	assert.That(t, it.Seek(lsm.Key{0: 20, 16: 15}))
	assert.Equal(t, lsm.Key{0: 20, 16: 20}.String(), it.Key().String())

	assert.That(t, !it.Seek(lsm.Key{0: 20, 16: 25}))
}
