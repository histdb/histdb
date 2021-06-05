package leveln

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/zeebo/pcg"

	"github.com/histdb/histdb"
	"github.com/histdb/histdb/filesystem"
	"github.com/histdb/histdb/testhelp"
)

func BenchmarkLevelNAppend(b *testing.B) {
	var rng pcg.T

	value := make([]byte, 512)

	run := func(b *testing.B, n int) {
		now := time.Now()

		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			func() {
				keys, cleanup := testhelp.Tempfile(b, filesystem.Temp)
				defer cleanup()

				values, cleanup := testhelp.Tempfile(b, filesystem.Temp)
				defer cleanup()

				var key histdb.Key
				var ln Writer
				ln.Init(keys, values)

				for j := 0; j < n; j++ {
					binary.BigEndian.PutUint64(key[0:8], uint64(j)/32)
					binary.BigEndian.PutUint32(key[16:20], uint32(j))
					_ = ln.Append(key, nil, value[0:256+rng.Uint32()%256])
				}
				_ = ln.Finish()

				if i == 0 {
					ksize, _ := keys.Size()
					vsize, _ := values.Size()
					b.SetBytes(ksize + vsize)
				}
			}()
		}

		b.ReportMetric(float64(time.Since(now))/float64(n)/float64(b.N), "ns/key")
		b.ReportMetric(float64(n)*float64(b.N)/time.Since(now).Seconds(), "keys/sec")
	}

	b.Run("1e2", func(b *testing.B) { run(b, 1e2) })
	b.Run("1e3", func(b *testing.B) { run(b, 1e3) })
	b.Run("1e4", func(b *testing.B) { run(b, 1e4) })
	b.Run("1e5", func(b *testing.B) { run(b, 1e5) })
	b.Run("1e6", func(b *testing.B) { run(b, 1e6) })
	b.Run("1e7", func(b *testing.B) { run(b, 1e7) })
}
