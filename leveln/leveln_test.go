package leveln

import (
	"testing"

	"github.com/zeebo/assert"
	"github.com/zeebo/mwc"

	"github.com/histdb/histdb"
	"github.com/histdb/histdb/testhelp"
)

func TestLevelNWriterReader(t *testing.T) {
	fs, cleanup := testhelp.FS(t)
	defer cleanup()

	kfh, cleanup := testhelp.Tempfile(t, fs)
	defer cleanup()

	vfh, cleanup := testhelp.Tempfile(t, fs)
	defer cleanup()

	metrics := createMetrics(5000)

	var lnw Writer
	lnw.Init(kfh, vfh)

	var values [][]byte
	for _, metric := range metrics {
		var key histdb.Key
		*key.HashPtr() = metric.hash
		key.SetDuration(1)
		for i := range 8 {
			val := testhelp.Value(mwc.Intn(32))
			values = append(values, val)
			key.SetTimestamp(uint32(i))
			assert.NoError(t, lnw.Append(key, val))
		}
	}
	assert.NoError(t, lnw.Finish())

	var it Iterator
	it.Init(kfh, vfh)

	for _, metric := range metrics {
		for i := range 8 {
			if !it.Next() {
				t.Fatalf("next failed: %+v", it.Err())
			}
			assert.Equal(t, metric.hash, it.Key().Hash())
			assert.Equal(t, uint32(i), it.Key().Timestamp())
			assert.Equal(t, 1, it.Key().Duration())
			assert.Equal(t, values[0], it.Value())
			values = values[1:]
		}
	}
	assert.That(t, !it.Next())

	for _, metric := range metrics {
		var key histdb.Key
		*key.HashPtr() = metric.hash
		it.Seek(key)

		var next bool
		for range 8 {
			assert.Equal(t, key.Hash(), it.Key().Hash())
			next = it.Next()
		}
		if next {
			assert.NotEqual(t, key.Hash(), it.Key().Hash())
		}
	}
}
