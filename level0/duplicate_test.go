package level0

import (
	"testing"

	"github.com/zeebo/assert"
	"github.com/zeebo/lsm/filesystem"
	"github.com/zeebo/lsm/testhelp"
)

func TestDuplicate(t *testing.T) {
	fh, cleanup := testhelp.Tempfile(t, new(filesystem.T))
	defer cleanup()

	var l0 T
	assert.NoError(t, l0.Init(fh))

	key := testhelp.Key()

	// write duplicate keys with a non-duplicate in between
	ok, err := l0.Append(key, []byte{10}, []byte{0})
	assert.NoError(t, err)
	assert.That(t, ok)

	key[len(key)-1]++
	ok, err = l0.Append(key, []byte{11}, []byte{1})
	assert.NoError(t, err)
	assert.That(t, ok)

	key[len(key)-1]--
	ok, err = l0.Append(key, []byte{12}, []byte{2})
	assert.NoError(t, err)
	assert.That(t, ok)

	// finish early and write the index, allowing iteration
	assert.NoError(t, l0.finish())

	// iterate over the file and see the keys come in the right order
	it, err := l0.Iterator()
	assert.NoError(t, err)

	assert.That(t, it.Next())
	assert.Equal(t, it.Key().String(), key.String())
	assert.DeepEqual(t, it.Name(), []byte{10})
	assert.DeepEqual(t, it.Value(), []byte{0})

	assert.That(t, it.Next())
	assert.Equal(t, it.Key().String(), key.String())
	assert.DeepEqual(t, it.Name(), []byte{12})
	assert.DeepEqual(t, it.Value(), []byte{2})

	key[len(key)-1]++
	assert.That(t, it.Next())
	assert.Equal(t, it.Key().String(), key.String())
	assert.DeepEqual(t, it.Name(), []byte{11})
	assert.DeepEqual(t, it.Value(), []byte{1})

	assert.That(t, !it.Next())
	assert.NoError(t, it.Err())

	// check seeking
	t.Log(key)
	assert.That(t, it.Seek(key))
	assert.Equal(t, it.Key().String(), key.String())
	assert.DeepEqual(t, it.Name(), []byte{11})
	assert.DeepEqual(t, it.Value(), []byte{1})

	key[len(key)-1]--
	assert.That(t, it.Seek(key))
	assert.Equal(t, it.Key().String(), key.String())
	assert.DeepEqual(t, it.Name(), []byte{10})
	assert.DeepEqual(t, it.Value(), []byte{0})

	key[len(key)-1] += 2
	assert.That(t, !it.Seek(key))
	assert.NoError(t, it.Err())
}
