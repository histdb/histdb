package metrics

import (
	"github.com/histdb/histdb"
)

func Hash(metric []byte) histdb.Hash {
	tagis := make([]histdb.TagHash, 0, 8)
	var tagus map[histdb.TagHash]struct{}
	var hash histdb.Hash

	mhp := hash.TagHashPtr()
	thp := hash.TagKeyHashPtr()

	for rest := metric; len(rest) > 0; {
		var tag []byte
		var tkey []byte
		tkey, tag, rest = PopTag(rest)
		if len(tag) == 0 {
			continue
		}

		tkeyh := histdb.NewTagKeyHash(tkey)
		tagh := histdb.NewTagHash(tag)

		var ok bool
		tagis, tagus, ok = addSet(tagis, tagus, tagh)

		if ok {
			thp.Add(tkeyh)
			mhp.Add(tagh)
		}
	}

	return hash
}
