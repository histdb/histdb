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

	var txn Txn
	err := dir.InitTxn(&txn, func(ops Ops) Ops {
		ops.Allocate(File{Generation: 0, Kind: 0}, 100)
		return ops
	})
	assert.NoError(t, err)

	t.Log(txn.L0s()[0].File)
}
