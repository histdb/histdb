package histdb

import "encoding/binary"

var le = binary.LittleEndian

const (
	TagKeyHashSize = 8
	TagHashSize    = 10
	HashSize       = TagKeyHashSize + TagHashSize
	TimestampSize  = 4
	DurationSize   = 2

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
