package memindex

import (
	"fmt"
	"strings"

	"github.com/zeebo/pcg"
)

func load(idx *T) {
	rng := pcg.New(1)

	const (
		nkeys = 200
		ntags = 10000
		nmets = 200000
	)

	var tags []string
	for k := 0; k < nkeys; k++ {
		for t := 0; t < ntags/nkeys; t++ {
			tags = append(tags, fmt.Sprintf("k%d=v%d", k, t))
		}
	}

	var mbuf []string
	for m := 0; m < nmets; m++ {
		mbuf = mbuf[:0]
		for n := 0; n < 5; n++ {
			mbuf = append(mbuf, tags[rng.Uint32n(uint32(len(tags)))])
		}
		idx.Add(strings.Join(mbuf, ","))
	}

	idx.Fix()
}
