package histdb

import "encoding/binary"

var le = binary.LittleEndian

const (
	TagKeyHashSize = 8
	TagHashSize    = 16
	HashSize       = TagKeyHashSize + TagHashSize
	TimestampSize  = 4
	DurationSize   = 4

	KeySize = HashSize + TimestampSize + DurationSize

	TagKeyHashStart = 0
	TagKeyHashEnd   = TagKeyHashStart + TagKeyHashSize

	TagHashStart = TagKeyHashEnd
	TagHashEnd   = TagHashStart + TagHashSize

	HashStart = TagKeyHashStart
	HashEnd   = TagHashEnd

	TimestampStart = HashEnd
	TimestampEnd   = TimestampStart + TimestampSize

	DurationStart = TimestampEnd
	DurationEnd   = DurationStart + DurationSize
)
