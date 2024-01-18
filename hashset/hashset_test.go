package hashset

import (
	"testing"

	"github.com/zeebo/assert"

	"github.com/histdb/histdb"
	"github.com/histdb/histdb/hashtbl"
	"github.com/histdb/histdb/rwutils"
)

func TestHashSet(t *testing.T) {
	var hs T[histdb.Hash, hashtbl.U64]

	hs.Insert(histdb.Hash{1})
	hs.Insert(histdb.Hash{2})
	hs.Insert(histdb.Hash{3})

	assert.Equal(t, hs.Hash(0), histdb.Hash{1})
	assert.Equal(t, hs.Hash(1), histdb.Hash{2})
	assert.Equal(t, hs.Hash(2), histdb.Hash{3})

	var w rwutils.W
	AppendTo(&hs, &w)

	var r rwutils.R
	r.Init(w.Done().Reset())

	var ms2 T[histdb.Hash, hashtbl.U64]
	ReadFrom(&ms2, &r)

	assert.Equal(t, ms2.Hash(0), histdb.Hash{1})
	assert.Equal(t, ms2.Hash(1), histdb.Hash{2})
	assert.Equal(t, ms2.Hash(2), histdb.Hash{3})
}
