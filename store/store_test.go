package store

import (
	"fmt"
	"testing"

	"github.com/zeebo/assert"
	"github.com/zeebo/mwc"

	"github.com/histdb/histdb"
	"github.com/histdb/histdb/flathist"
	"github.com/histdb/histdb/query"
	"github.com/histdb/histdb/testhelp"
)

func TestStore(t *testing.T) {
	fs, cleanup := testhelp.FS(t)
	defer cleanup()

	var st T
	var q query.Q
	var m []byte

	assert.NoError(t, st.Init(fs, Config{}))

	for range 1000 {
		m = testhelp.Metric(5)
		for range 100 {
			st.Observe(m, mwc.Float32())
		}

		for i, v := range m {
			if v == ',' {
				m[i] = '&'
			}
		}

		assert.NoError(t, query.Parse(m, &q))

		called := false
		st.Latest(&q, func(metric []byte, st *flathist.S, h flathist.H) bool {
			called = true
			return true
		})
		assert.That(t, called)
	}

	assert.NoError(t, st.WriteLevel(1000, 1))

	ok, err := st.Query(&q, 0, func(key histdb.Key, name []byte, st *flathist.S, h flathist.H) bool {
		total, sum, avg, vari := st.Summary(h)
		fmt.Println(key, string(name), total, sum, avg, vari)
		return true
	})
	assert.NoError(t, err)
	assert.That(t, ok)
}
