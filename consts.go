package histdb

import "encoding/binary"

var le = binary.LittleEndian

const (
	TagKeyHashSize = 8
	TagHashSize    = 12
	HashSize       = TagKeyHashSize + TagHashSize
	TimestampSize  = 4

	KeySize = HashSize + TimestampSize

	TagHashStart = 0
	TagHashEnd   = TagHashStart + TagKeyHashSize

	MetricHashStart = TagHashEnd
	MetricHashEnd   = MetricHashStart + TagHashSize

	HashStart = TagHashStart
	HashEnd   = MetricHashEnd

	TimestampStart = HashEnd
	TimestampEnd   = TimestampStart + TimestampSize
)
