package level0

import (
	"encoding/binary"

	"github.com/zeebo/lsm"
)

type entry [24]byte

func (e *entry) SetLength(x uint32) { binary.LittleEndian.PutUint32(e[0:4], x) }
func (e entry) Length() uint32      { return binary.LittleEndian.Uint32(e[0:4]) }

func (e *entry) SetTimestamp(x uint32) { binary.LittleEndian.PutUint32(e[20:24], x) }
func (e entry) Timestamp() uint32      { return binary.LittleEndian.Uint32(e[20:24]) }

func (e *entry) SetKey(k lsm.Key) { copy(e[4:20], k[0:16]) }
func (e entry) Key() (k lsm.Key) {
	copy(k[0:16], e[4:20])
	return k
}
