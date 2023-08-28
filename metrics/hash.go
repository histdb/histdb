package metrics

import (
	"github.com/histdb/histdb"
)

func Hash(metric []byte) histdb.Hash {
	tkeyis := make([]histdb.TagKeyHash, 0, 8)
	var tkeyus map[histdb.TagKeyHash]struct{}
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

		tkeyh := histdb.NewTagKeyHash(tkey)

		var ok bool
		tkeyis, tkeyus, ok = addSet(tkeyis, tkeyus, tkeyh)

		if ok {
			tagh := histdb.NewTagHash(tag)

			thp.Add(tkeyh)
			mhp.Add(tagh)
		}
	}

	return hash
}
