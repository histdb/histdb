package store

import (
	"fmt"
	"testing"
	"time"

	"github.com/zeebo/assert"

	"github.com/histdb/histdb/testhelp"
)

func BenchmarkStore(b *testing.B) {
	fs, cleanup := testhelp.FS(b)
	// defer cleanup()
	_ = cleanup

	fmt.Println(fs.Base)

	names := make([][]byte, b.N)
	values := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		names[i] = testhelp.Name(3)
		values[i] = testhelp.Value(256)
	}

	var s T
	assert.NoError(b, s.Init(fs))

	b.SetBytes(256)
	b.ReportAllocs()

	now := time.Now()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		assert.NoError(b, s.Write(uint32(i), names[i], values[i]))
	}
	b.ReportMetric(float64(b.N)/time.Since(now).Seconds(), "keys/sec")
}
