package memindex

import (
	"testing"

	"github.com/histdb/histdb"
	"github.com/histdb/histdb/rwutils"
	"github.com/zeebo/assert"
)

func TestHashSet(t *testing.T) {
	var hs hashSet

	hs.Insert(histdb.Hash{1})
	hs.Insert(histdb.Hash{2})
	hs.Insert(histdb.Hash{3})

	assert.Equal(t, hs.Hash(0), histdb.Hash{1})
	assert.Equal(t, hs.Hash(1), histdb.Hash{2})
	assert.Equal(t, hs.Hash(2), histdb.Hash{3})

	var w rwutils.W
	hs.AppendTo(&w)

	var r rwutils.R
	r.Init(w.Done().Reset())

	var ms2 hashSet
	ms2.ReadFrom(&r)

	assert.Equal(t, ms2.Hash(0), histdb.Hash{1})
	assert.Equal(t, ms2.Hash(1), histdb.Hash{2})
	assert.Equal(t, ms2.Hash(2), histdb.Hash{3})
}
