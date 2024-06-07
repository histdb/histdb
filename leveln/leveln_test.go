package leveln

import (
	"testing"

	"github.com/zeebo/assert"

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

	const amount = 50000

	for i := 0; i < amount; i++ {
		key := testhelp.KeyFrom(uint64(i)/8, 0, uint32(i), 0)
		assert.NoError(t, lnw.Append(
			key,
			[]byte{byte(i) / 8},
			[]byte{byte(i >> 8), byte(i)},
		))
	}
	assert.NoError(t, lnw.Finish())

	var it Iterator
	it.Init(keys, values, nil)

	i := 0
	for ; it.Next(); i++ {
		key := testhelp.KeyFrom(uint64(i)/8, 0, uint32(i), 0)

		assert.Equal(t, it.Key(), key)
		assert.Equal(t, it.Name()[0], byte(i)/8)
		assert.Equal(t, it.Value()[0], i/256)
		assert.Equal(t, it.Value()[1], i%256)
	}
	assert.NoError(t, it.Err())
	assert.Equal(t, i, amount)
}
