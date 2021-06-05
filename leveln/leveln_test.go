package leveln

import (
	"encoding/binary"
	"testing"

	"github.com/zeebo/assert"

	"github.com/histdb/histdb"
	"github.com/histdb/histdb/filesystem"
	"github.com/histdb/histdb/testhelp"
)

func TestLevelNWriterReader(t *testing.T) {
	keys, cleanup := testhelp.Tempfile(t, filesystem.Temp)
	defer cleanup()

	values, cleanup := testhelp.Tempfile(t, filesystem.Temp)
	defer cleanup()

	var lnw Writer
	lnw.Init(keys, values)

	for i := 0; i < 1000; i++ {
		var key histdb.Key
		binary.BigEndian.PutUint64(key[0:8], uint64(i)/8)
		binary.BigEndian.PutUint32(key[16:20], uint32(i))
		assert.NoError(t, lnw.Append(key, nil, []byte{byte(i >> 8), byte(i)}))
	}
	assert.NoError(t, lnw.Finish())

	var lnr Reader
	lnr.Init(keys, values)

	it, i := lnr.Iterator(), 0
	for ; it.Next(); i++ {
		var key histdb.Key
		binary.BigEndian.PutUint64(key[0:8], uint64(i)/8)
		binary.BigEndian.PutUint32(key[16:20], uint32(i))

		assert.Equal(t, key, it.Key())
		assert.Equal(t, it.Value()[0], i/256)
		assert.Equal(t, it.Value()[1], i%256)
	}
	assert.NoError(t, it.Err())
	assert.Equal(t, i, 1000)
}
