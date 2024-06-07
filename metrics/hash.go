package metrics

import (
	"bytes"

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

func PopTag(tags []byte) (tkey, tag []byte, rest []byte) {
	// find the first unescaped ','
	for j := uint(0); j < uint(len(tags)); {
		i := bytes.IndexByte(tags[j:], ',')
		if i < 0 {
			break
		}

		// walk backwards counting the number of \
		ui := uint(i) + j
		for ui-1 < uint(len(tags)) && tags[ui-1] == '\\' {
			ui--
		}

		// an odd number of \ means it is escaped
		if (uint(i)+j-ui)%2 == 1 {
			j += uint(i) + 1
			continue
		}

		idx := uint(i) + j
		tags, rest = tags[:idx], tags[idx+1:]
		break
	}

	// if there's no =, then the tag key is the tag
	tkey = tags

	// find the first unescaped '='
	for j := uint(0); j < uint(len(tkey)); {
		i := bytes.IndexByte(tkey[j:], '=')
		if i < 0 {
			break
		}

		// walk backwards counting the number of \
		ui := uint(i) + j
		for ui-1 < uint(len(tkey)) && tkey[ui-1] == '\\' {
			ui--
		}

		// an odd number of \ means it is escaped
		if (uint(i)+j-ui)%2 == 1 {
			j += uint(i) + 1
			continue
		}

		tkey = tkey[:uint(i)+j]
		break
	}

	// if the tag has an empty string value, then drop the trailing =
	// this is so that `foo=` and `foo` are the same.
	if len(tags) == len(tkey)+1 && tags[len(tags)-1] == '=' {
		tags = tags[:len(tags)-1]
	}

	return tkey, tags, rest
}
