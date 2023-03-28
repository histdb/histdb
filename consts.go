package histdb

import "encoding/binary"

var le = binary.LittleEndian

const (
	TagKeyHashSize = 4
	TagHashSize    = 12
	HashSize       = TagKeyHashSize + TagHashSize
	TimestampSize  = 4

	KeySize = HashSize + TimestampSize

	tagHashStart = 0
	tagHashEnd   = tagHashStart + TagKeyHashSize

	metricHashStart = tagHashEnd
	metricHashEnd   = metricHashStart + TagHashSize

	hashStart = tagHashStart
	hashEnd   = metricHashEnd

	timestampStart = hashEnd
	timestampEnd   = timestampStart + TimestampSize
)
