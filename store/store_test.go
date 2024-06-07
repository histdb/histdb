package store

import (
	"testing"
	"time"

	"github.com/zeebo/assert"

	"github.com/histdb/histdb/testhelp"
)

func BenchmarkStore(b *testing.B) {
	fs, cleanup := testhelp.FS(b)
	// defer cleanup()
	_ = cleanup

	names := make([][]byte, b.N)
	values := make([][]byte, b.N)
	for i := range b.N {
		names[i] = testhelp.Name(3)
		values[i] = testhelp.Value(256)
	}

	var s T
	assert.NoError(b, s.Init(fs))

	b.SetBytes(256)
	b.ReportAllocs()

	now := time.Now()
	b.ResetTimer()

	for i := range b.N {
		assert.NoError(b, s.Write(names[i], values[i], uint32(i), uint16(i)))
	}
	b.ReportMetric(float64(b.N)/time.Since(now).Seconds(), "keys/sec")
}
