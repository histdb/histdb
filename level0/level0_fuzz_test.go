package level0

import (
	"encoding/binary"
	"testing"

	"github.com/zeebo/assert"

	"github.com/histdb/histdb"
	"github.com/histdb/histdb/testhelp"
)

func FuzzLevel0(f *testing.F) {
	fs, cleanup := testhelp.FS(f)
	defer cleanup()

	nbuf := make([]byte, 0, 256)
	vbuf := make([]byte, 0, 256)
	var kbuf histdb.Key

	loadEntry := func(entry []byte) {
		kbuf.SetTimestamp(binary.BigEndian.Uint32(entry) | 1)
		nbuf = nbuf[:entry[0]]
		vbuf = vbuf[:entry[1]]
	}

	f.Fuzz(func(t *testing.T, entries []byte) {
		fh, cleanup := testhelp.Tempfile(t, fs)
		defer cleanup()

		var l0 T
		assert.NoError(t, l0.Init(fh, nil))

		if len(entries) < 4 {
			return
		}

		b := entries
		for {
			if len(b) < 4 {
				b = entries
			}
			loadEntry(b[:4])
			b = b[4:]

			ok, err := l0.Append(kbuf, nbuf, vbuf)
			assert.NoError(t, err)
			if !ok || l0.len > 2*l0BufferSize {
				break
			}
		}

		b = entries
		l0.Init(fh, func(key histdb.Key, name, value []byte) {
			if len(b) < 4 {
				b = entries
			}
			loadEntry(b[:4])
			b = b[4:]

			assert.Equal(t, key, kbuf)
			assert.DeepEqual(t, name, nbuf)
			assert.DeepEqual(t, value, vbuf)
		})
	})
}
