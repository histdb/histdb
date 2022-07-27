package histdb

import "encoding/binary"

var le = binary.LittleEndian

const (
	TagHashSize    = 8
	MetricHashSize = 8
	HashSize       = TagHashSize + MetricHashSize
	TimestampSize  = 4

	KeySize = HashSize + TimestampSize

	tagHashStart = 0
	tagHashEnd   = tagHashStart + TagHashSize

	metricHashStart = tagHashEnd
	metricHashEnd   = metricHashStart + MetricHashSize

	hashStart = tagHashStart
	hashEnd   = metricHashEnd

	timestampStart = hashEnd
	timestampEnd   = timestampStart + TimestampSize
)
