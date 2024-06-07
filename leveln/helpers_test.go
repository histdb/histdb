package leveln

import (
	"sort"

	"github.com/histdb/histdb"
	"github.com/histdb/histdb/memindex"
	"github.com/histdb/histdb/testhelp"
)

type addedMetric struct {
	hash histdb.Hash
	norm []byte
}

func insertMetrics(idx *memindex.T, n uint64) []addedMetric {
	var metrics []addedMetric
	for range n {
		hash, _, norm, _ := idx.Add(testhelp.Metric(5), []byte{}, nil)
		metrics = append(metrics, addedMetric{
			hash: hash,
			norm: norm,
		})
	}
	sort.Slice(metrics, func(i, j int) bool {
		return string(metrics[i].hash[:]) < string(metrics[j].hash[:])
	})
	return metrics
}
