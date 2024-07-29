package memindex

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/histdb/histdb/card"
	"github.com/histdb/histdb/rwutils"
)

var reload = flag.Bool("reload", false, "reload metrics.txt.gz into metrics.idx")

func TestMain(m *testing.M) {
	flag.Parse()
	if *reload {
		doReload()
	}
	os.Exit(m.Run())
}

func doReload() {
	var idx T

	fh, err := os.Open("metrics.txt.gz")
	if err != nil {
		panic(err)
	}
	defer fh.Close()

	gzfh, err := gzip.NewReader(fh)
	if err != nil {
		panic(err)
	}

	const statEvery = 100000
	start := time.Now()
	count := 0

	lstats := start
	lcard := 0
	lcount := 0

	stats := func() {
		size := float64(idx.Size())
		card := idx.Cardinality()

		fmt.Printf("Added (%-8d m) (%-8d um) | total (%0.2f%% unique) (%0.2f m/sec) (%0.2f um/sec) | recently (%0.2f%% unique) (%0.2f m/sec) (%0.2f um/sec) | index size (%0.2f MiB) (%0.2f b/m)\n",
			count,
			card,

			float64(card)/float64(count)*100,
			float64(count)/time.Since(start).Seconds(),
			float64(card)/time.Since(start).Seconds(),

			float64(card-lcard)/float64(count-lcount)*100,
			float64(count-lcount)/time.Since(lstats).Seconds(),
			float64(card-lcard)/time.Since(lstats).Seconds(),

			size/1024/1024,
			size/float64(card),
		)

		lstats = time.Now()
		lcard = card
		lcount = count
	}

	var cf card.Fixer
	cf.DropTagKey([]byte("inst"))

	scanner := bufio.NewScanner(gzfh)
	for scanner.Scan() {
		idx.Add(bytes.TrimSpace(scanner.Bytes()), nil, &cf)
		count++
		if count%statEvery == 0 {
			stats()
		}
	}

	stats()

	var w rwutils.W
	AppendTo(&idx, &w)

	wfh, _ := os.Create("metrics.idx")
	defer wfh.Close()
	wfh.Write(w.Done().Prefix())
}
