package level0

import (
	"io"
	"io/ioutil"
	"testing"
	"time"

	"github.com/zeebo/assert"
	"github.com/zeebo/lsm"
	"github.com/zeebo/lsm/filesystem"
)

func TestLevel0New(t *testing.T) {
	t.Run("Append", func(t *testing.T) {
		fh, cleanup := newTempfile(t, new(filesystem.T))
		defer cleanup()

		var l0 T2
		assert.NoError(t, l0.Init(fh))

		for {
			ok, err := l0.Append(newKey(t), 0x0a0b0c0d, []byte{0, 1, 2, 3, 4, 5, 6, 7})
			assert.NoError(t, err)
			if !ok {
				break
			}
		}

		_, err := fh.Seek(0, io.SeekStart)
		assert.NoError(t, err)

		data, err := ioutil.ReadAll(fh)
		assert.NoError(t, err)

		assert.Equal(t, len(data), l0DataSize+l0IndexSize)
	})
}

func BenchmarkLevel0New(b *testing.B) {
	b.Run("AppendAll", func(b *testing.B) {
		fh, cleanup := newTempfile(b, new(filesystem.T))
		defer cleanup()
		var l0 T2

		now := time.Now()
		value := make([]byte, 8)
		keys := make([]lsm.Key, 65535)
		for i := range keys {
			keys[i] = newKey(b)
		}

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			assert.NoError(b, l0.Init(fh))
			for _, key := range keys {
				_, _ = l0.Append(key, 0, value)
			}
		}

		b.ReportMetric(float64(len(keys)*b.N)/time.Since(now).Seconds(), "keys/sec")
		b.ReportMetric(float64(time.Since(now).Nanoseconds())/float64(len(keys)*b.N), "ns/key")
	})
}
