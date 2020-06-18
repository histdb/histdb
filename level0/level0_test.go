package level0

import (
	"encoding/binary"
	"io"
	"io/ioutil"
	"testing"

	"github.com/zeebo/assert"
	"github.com/zeebo/lsm/filesystem"
)

func TestLevel0(t *testing.T) {
	t.Run("Append", func(t *testing.T) {
		l0, _, cleanup := newLevel0(t, new(filesystem.T), 4096, 2<<20)
		defer cleanup()

		_, err := l0.fh.Seek(0, io.SeekStart)
		assert.NoError(t, err)

		data, err := ioutil.ReadAll(l0.fh)
		assert.NoError(t, err)

		assert.Equal(t, len(data), (2<<20)+8*((2<<20)/32)+4)
		assert.Equal(t, binary.BigEndian.Uint32(data[len(data)-4:]), 2<<20)
	})
}
