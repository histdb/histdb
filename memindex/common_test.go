package memindex

import (
	"bytes"
	"fmt"

	"github.com/zeebo/mwc"
)

func loadRandom(idx *T) {
	rng := mwc.Rand()

	const (
		nkeys = 20
		ntags = 1000
		nmets = 20000
	)

	var tags [][]byte
	for k := 0; k < nkeys; k++ {
		for t := 0; t < ntags/nkeys; t++ {
			tags = append(tags, []byte(fmt.Sprintf("k%d=v%d", k, t)))
		}
	}

	var mbuf [][]byte
	for m := 0; m < nmets; m++ {
		mbuf = mbuf[:0]
		for n := 0; n < 5; n++ {
			mbuf = append(mbuf, tags[rng.Uint32n(uint32(len(tags)))])
		}
		idx.Add(bytes.Join(mbuf, []byte(",")), nil, nil)
	}
}
