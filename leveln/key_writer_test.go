package leveln

import (
	"io"
	"testing"
	"time"

	"github.com/zeebo/assert"

	"github.com/histdb/histdb/testhelp"
)

func TestKeyWriterPage(t *testing.T) {
	fs, cleanup := testhelp.FS(t)
	defer cleanup()

	fh, cleanup := testhelp.Tempfile(t, fs)
	defer cleanup()

	var kw keyWriter
	kw.Init(fh)

	// build a page
	for i := range &kw.page.hdr {
		kw.page.hdr[i] = byte(i + 1)
	}
	for i := range kw.page.ents {
		var val [kwEntrySize]byte
		for j := range val {
			val[j] = byte(i)
		}
		kw.page.ents[i] = val
	}

	// write it out
	assert.NoError(t, kw.writePage(&kw.page))

	// read it back
	_, err := fh.Seek(0, io.SeekStart)
	assert.NoError(t, err)
	data, err := io.ReadAll(fh)
	assert.NoError(t, err)
	assert.Equal(t, len(data), kwPageSize)

	// check header bytes
	for i := range data[0:kwHeaderSize] {
		assert.Equal(t, data[i], i+1)
	}
	data = data[kwHeaderSize:]

	// check payload bytes
	for i := 0; len(data) > 0; i++ {
		for j := range kwEntrySize {
			assert.Equal(t, data[j], byte(i))
		}
		data = data[kwEntrySize:]
	}
}

func BenchmarkKeyWriterAppend(b *testing.B) {
	run := func(b *testing.B, n int) {
		fs, cleanup := testhelp.FS(b)
		defer cleanup()

		fh, cleanup := testhelp.Tempfile(b, fs)
		defer cleanup()

		now := time.Now()
		b.ReportAllocs()
		b.ResetTimer()

		for b.Loop() {
			_, _ = fh.Seek(0, io.SeekStart)
			fh.Truncate(0)

			kw := new(keyWriter)
			kw.Init(fh)

			for range n {
				_ = kw.Append(kwEntry{})
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
