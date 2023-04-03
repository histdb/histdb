package metrics

import (
	"encoding/binary"

	"github.com/zeebo/xxh3"

	"github.com/histdb/histdb"
)

var le = binary.LittleEndian

func Hash(metric []byte) histdb.Hash {
	tkeyis := make([]uint64, 0, 8)
	var tkeyus map[uint64]struct{}
	var hash histdb.Hash

	mhp := hash.TagHashPtr()
	thp := hash.TagKeyHashPtr()

	for rest := metric; len(rest) > 0; {
		var tag []byte
		var tkey []byte
		tkey, tag, _, rest = PopTag(rest)
		if len(tag) == 0 {
			continue
		}

		tkeyh := xxh3.Hash(tkey)

		var ok bool
		tkeyis, tkeyus, ok = addSet(tkeyis, tkeyus, tkeyh)

		if ok {
			tagh := xxh3.Hash128(tag)

			le.PutUint32(thp[:], le.Uint32(thp[:])+uint32(tkeyh))
			le.PutUint64(mhp[0:8], le.Uint64(mhp[0:8])+tagh.Lo)
			le.PutUint32(mhp[8:12], le.Uint32(mhp[8:12])+uint32(tagh.Hi))
		}
	}

	return hash
}
