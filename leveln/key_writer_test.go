package leveln

import (
	"io"
	"io/ioutil"
	"testing"
	"time"

	"github.com/zeebo/assert"
	"github.com/zeebo/lsm/filesystem"
	"github.com/zeebo/lsm/testhelp"
)

func TestKeyWriterPage(t *testing.T) {
	fh, cleanup := testhelp.Tempfile(t, new(filesystem.T))
	defer cleanup()

	var kw keyWriter
	kw.Init(fh)

	// build a page
	kw.hdr = [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	for i := range kw.ents {
		var val [kwEntrySize]byte
		for j := range val {
			val[j] = byte(i)
		}
		kw.ents[i] = val
	}

	// write it out
	assert.NoError(t, kw.writePage(kw.page()))

	// read it back
	_, err := fh.Seek(0, io.SeekStart)
	assert.NoError(t, err)
	data, err := ioutil.ReadAll(fh)
	assert.NoError(t, err)
	assert.Equal(t, len(data), kwPageSize)

	// check header bytes
	assert.DeepEqual(t, data[0:16], []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	data = data[16:]

	// check payload bytes
	for i := 0; len(data) > 0; i++ {
		for j := 0; j < kwEntrySize; j++ {
			assert.Equal(t, data[j], byte(i))
		}
		data = data[kwEntrySize:]
	}
}

func BenchmarkKeyWriterAppend(b *testing.B) {
	fh, cleanup := testhelp.Tempfile(b, new(filesystem.T))
	defer cleanup()

	run := func(b *testing.B, n int) {
		now := time.Now()

		for i := 0; i < b.N; i++ {
			_, _ = fh.Seek(0, io.SeekStart)

			kw := new(keyWriter)
			kw.Init(fh)

			var key [24]byte

			for j := 0; j < n; j++ {
				_ = kw.Append(key)
			}
			_ = kw.Finish()
		}

		size, _ := fh.Size()
		b.SetBytes(size)

		b.ReportMetric(float64(size)/kwPageSize, "pages")
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
