package leveln

import (
	"github.com/zeebo/lsm"
	"github.com/zeebo/lsm/filesystem"
	"github.com/zeebo/lsm/utils"
)

const (
	kwPageSize = 4096
)

type keyWriter struct {
	fh  filesystem.File
	buf utils.Buffer

	chunk struct {
		minTimestamp uint32
		startLen     int
	}

	prev struct {
		key    lsm.Key
		offset uint64
	}
}

func (k *keyWriter) Init(fh filesystem.File) {
	*k = keyWriter{
		fh:  fh,
		buf: utils.NewBuffer(kwPageSize),
	}
}

func (k *keyWriter) Append(key lsm.Key, offset uint64) error {

}

func (k *keyWriter) chunkStart(key lsm.Key) bool {
	startLen := k.buf.Len()
	if !k.buf.AppendUint32(0) { // length
		return false
	}
	if !k.buf.AppendUint32(key.Timestamp()) { // min timestamp
		return false
	}
	if !k.buf.AppendUint32(0) { // max timestamp
		return false
	}
	if !k.buf.Append(key[:]) {
		return false
	}
	k.chunk.minTimestamp = key.Timestamp()
	k.chunk.startLen = startLen
	return true
}

func (k *keyWriter) chunkEnd(ts uint32) {
	// TODO: write length: uint32(k.buf.Len() - k.chunk.startLen)
	// TODO: write max timestamp: ts
}
