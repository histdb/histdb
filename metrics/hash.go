package metrics

import (
	"encoding/binary"

	"github.com/zeebo/xxh3"

	"github.com/histdb/histdb"
)

var le = binary.LittleEndian

func Hash(metric string) histdb.Hash {
	tkeyis := make([]uint64, 0, 8)
	var tkeyus map[uint64]struct{}
	var hash histdb.Hash

	for rest := metric; len(rest) > 0; {
		var tag string
		var tkey string
		tkey, tag, _, rest = PopTag(rest)
		if len(tag) == 0 {
			continue
		}

		tkeyh := xxh3.HashString(tkey)

		var ok bool
		tkeyis, tkeyus, ok = addSet(tkeyis, tkeyus, tkeyh)

		if ok {
			th := le.Uint64(hash.TagHashPtr()[:])
			le.PutUint64(hash.TagHashPtr()[:], th+tkeyh)

			tagh := xxh3.HashString(tag)
			mh := le.Uint64(hash.MetricHashPtr()[:])
			le.PutUint64(hash.MetricHashPtr()[:], mh+tagh)
		}
	}

	return hash
}
