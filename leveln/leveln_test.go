package leveln

import (
	"testing"

	"github.com/zeebo/assert"

	"github.com/histdb/histdb"
	"github.com/histdb/histdb/testhelp"
)

func TestLevelNWriterReader(t *testing.T) {
	fs, cleanup := testhelp.FS(t)
	defer cleanup()

	keys, cleanup := testhelp.Tempfile(t, fs)
	defer cleanup()

	values, cleanup := testhelp.Tempfile(t, fs)
	defer cleanup()

	var lnw Writer
	lnw.Init(keys, values)

	for i := 0; i < 1000; i++ {
		key := testhelp.KeyFrom(uint32(i)/8, 0, uint32(i))
		assert.NoError(t, lnw.Append(key, key[:4], []byte{byte(i >> 8), byte(i)}))
	}
	assert.NoError(t, lnw.Finish())

	var it Iterator
	it.Init(keys, values, nil)

	i := 0
	for ; it.Next(); i++ {
		var key histdb.Key
		be.PutUint32(key.TagKeyHashPtr()[:], uint32(i)/8)
		key.SetTimestamp(uint32(i))

		assert.Equal(t, key, it.Key())
		assert.Equal(t, it.Name(), key[:4])
		assert.Equal(t, it.Value()[0], i/256)
		assert.Equal(t, it.Value()[1], i%256)
	}
	assert.NoError(t, it.Err())
	assert.Equal(t, i, 1000)
}
