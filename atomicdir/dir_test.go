package atomicdir

import (
	"testing"

	"github.com/zeebo/assert"

	"github.com/histdb/histdb/testhelp"
)

func TestDir(t *testing.T) {
	fs, cleanup := testhelp.FS(t)
	defer cleanup()

	var dir T
	assert.NoError(t, dir.Init(fs))

	tx, err := dir.NewTransaction(func(ops *Operations) { ops.Allocate(File{}, 100) })
	assert.NoError(t, err)

	t.Log(tx.WAL().File)
}
