package leveln

import (
	"encoding/binary"
	"encoding/hex"
	"io"
	"testing"
	"time"

	"github.com/zeebo/assert"
	"github.com/zeebo/lsm"
	"github.com/zeebo/lsm/filesystem"
	"github.com/zeebo/lsm/testhelp"
	"github.com/zeebo/pcg"
)

func TestLevelN(t *testing.T) {
	keys, cleanup := testhelp.Tempfile(t, new(filesystem.T))
	defer cleanup()

	values, cleanup := testhelp.Tempfile(t, new(filesystem.T))
	defer cleanup()

	var ln T
	ln.Init(keys, values)

	for i := 0; i < 1000; i++ {
		var key lsm.Key
		binary.BigEndian.PutUint64(key[0:8], uint64(i)/8)
		binary.BigEndian.PutUint32(key[16:20], uint32(i))

		assert.NoError(t, ln.Append(key, []byte{byte(i >> 8), byte(i)}))
	}
	assert.NoError(t, ln.Finish())

	kd := testhelp.ReadFile(t, keys)
	vd := testhelp.ReadFile(t, values)

	t.Log("\n" + hex.Dump(kd))
	t.Log("\n" + hex.Dump(vd))
}

func BenchmarkLevelNAppend(b *testing.B) {
	var rng pcg.T

	keys, cleanup := testhelp.Tempfile(b, new(filesystem.T))
	defer cleanup()

	values, cleanup := testhelp.Tempfile(b, new(filesystem.T))
	defer cleanup()

	value := make([]byte, 512)

	run := func(b *testing.B, n int) {
		now := time.Now()

		for i := 0; i < b.N; i++ {
			_, _ = keys.Seek(0, io.SeekStart)
			_, _ = values.Seek(0, io.SeekStart)

			var key lsm.Key
			var ln T
			ln.Init(keys, values)

			for j := 0; j < n; j++ {
				binary.BigEndian.PutUint64(key[0:8], uint64(j)/32)
				binary.BigEndian.PutUint32(key[16:20], uint32(j))
				_ = ln.Append(key, value[0:256+rng.Uint32()%256])
			}
			_ = ln.Finish()
		}

		ksize, _ := keys.Size()
		vsize, _ := values.Size()
		b.SetBytes(ksize + vsize)

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
