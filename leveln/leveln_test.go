package leveln

import (
	"testing"

	"github.com/zeebo/assert"

	"github.com/histdb/histdb"
	"github.com/histdb/histdb/memindex"
	"github.com/histdb/histdb/testhelp"
)

func TestLevelNWriterReader(t *testing.T) {
	fs, cleanup := testhelp.FS(t)
	defer cleanup()

	keys, cleanup := testhelp.Tempfile(t, fs)
	defer cleanup()

	values, cleanup := testhelp.Tempfile(t, fs)
	defer cleanup()

	var idx memindex.T
	metrics := insertMetrics(&idx, 50000)

	var lnw Writer
	lnw.Init(keys, values)

	for _, metric := range metrics {
		var key histdb.Key
		*key.HashPtr() = metric.hash
		key.SetDuration(1)
		for i := range 8 {
			key.SetTimestamp(uint32(i))
			assert.NoError(t, lnw.Append(key, []byte{byte(i)}))
		}
	}
	assert.NoError(t, lnw.Finish())

	var it Iterator
	it.Init(keys, values, &idx)

	for _, metric := range metrics {
		for i := range 8 {
			assert.That(t, it.Next())
			assert.Equal(t, metric.hash, it.Key().Hash())
			assert.Equal(t, uint32(i), it.Key().Timestamp())
			assert.Equal(t, 1, it.Key().Duration())
			assert.Equal(t, []byte{byte(i)}, it.Value())
		}
	}
	assert.That(t, !it.Next())
}
