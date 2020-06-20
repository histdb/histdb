package level0

import (
	"sort"
	"testing"

	"github.com/zeebo/assert"
	"github.com/zeebo/lsm"
	"github.com/zeebo/lsm/testhelp"
)

func TestKeyHeap(t *testing.T) {
	t.Run("Order", func(t *testing.T) {
		var kh keyHeap
		var keys []lsm.Key

		for i := 0; i < 100000; i++ {
			key := testhelp.Key()

			kh = kh.Push(key)
			keys = append(keys, key)
		}

		sort.Slice(keys, func(i, j int) bool {
			return lsm.KeyCmp.Less(keys[i], keys[j])
		})

		var key lsm.Key
		for len(kh) > 0 {
			kh, key = kh.Pop()
			assert.Equal(t, key.String(), keys[0].String())
			keys = keys[1:]
		}
	})
}
