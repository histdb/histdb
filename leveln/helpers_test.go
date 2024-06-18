package leveln

import (
	"fmt"
	"sort"

	"github.com/histdb/histdb"
	"github.com/histdb/histdb/metrics"
	"github.com/histdb/histdb/testhelp"
)

type hashedMetric struct {
	hash histdb.Hash
	norm []byte
}

func (a hashedMetric) String() string {
	return fmt.Sprintf("%v %s", a.hash, a.norm)
}

func createMetrics(n uint64) []hashedMetric {
	var ms []hashedMetric
	for range n {
		norm := testhelp.Metric(5)
		hash := metrics.Hash(norm)
		ms = append(ms, hashedMetric{
			hash: hash,
			norm: norm,
		})
	}
	sort.Slice(ms, func(i, j int) bool {
		return string(ms[i].hash[:]) < string(ms[j].hash[:])
	})
	return ms
}
