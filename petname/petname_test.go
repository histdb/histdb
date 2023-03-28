package petname

import (
	"testing"

	"github.com/histdb/histdb/hashtbl"
	"github.com/zeebo/assert"
)

func TestPetname(t *testing.T) {
	var pn T[hashtbl.U64, *hashtbl.U64]

	assert.Equal(t, pn.Len(), 0)
	assert.Equal(t, pn.Size(), 0x68)

	i0 := pn.Put(1, "value1")
	i1 := pn.Put(2, "value2")
	i2 := pn.Put(1, "value3")
	assert.Equal(t, i0, i2)

	f0, ok := pn.Find(1)
	assert.That(t, ok)
	assert.Equal(t, f0, i0)

	f1, ok := pn.Find(2)
	assert.That(t, ok)
	assert.Equal(t, f1, i1)

	_, ok = pn.Find(3)
	assert.That(t, !ok)

	assert.Equal(t, pn.Get(i0), "value1")
	assert.Equal(t, pn.Get(i1), "value2")

	var out string
	assert.Equal(t, testing.AllocsPerRun(100, func() {
		out = pn.Get(i0)
	}), 0.0)
	assert.Equal(t, out, "value1")
}
