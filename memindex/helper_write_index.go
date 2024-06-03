package memindex

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"fmt"
	"os"
	"runtime/pprof"
	"time"

	"github.com/histdb/histdb/rwutils"
)

const reload = false

func init() {
	if !reload {
		return
	}

	{
		fh, err := os.Create("memindex-load.pprof")
		if err != nil {
			panic(err)
		}
		defer fh.Close()

		if err := pprof.StartCPUProfile(fh); err != nil {
			panic(err)
		}
		defer pprof.StopCPUProfile()
	}

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

	scanner := bufio.NewScanner(gzfh)
	for scanner.Scan() {
		idx.Add(bytes.TrimSpace(scanner.Bytes()), nil, nil)
		count++
		if count%statEvery == 0 {
			stats()
			// if idx.Cardinality() >= 1e6 {
			// 	break
			// }
		}
	}

	stats()

	var w rwutils.W
	AppendTo(&idx, &w)

	wfh, _ := os.Create("metrics.idx")
	defer wfh.Close()
	wfh.Write(w.Done().Prefix())
}
