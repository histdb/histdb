package leveln

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"

	"github.com/zeebo/errs/v2"

	"github.com/histdb/histdb/filesystem"
)

// dump constructs a dot graph of the node layout in an index
func dump(fh filesystem.Handle) { //nolint
	check := func(err error) {
		if err != nil {
			log.Fatalf("%+v", errs.Wrap(err))
		}
	}

	var buf [4096]byte
	var hdr kwPageHeader

	_, err := fh.Seek(0, io.SeekStart)
	check(err)

	fmt.Println("digraph btree { node[shape=box]; spline=line;")

	for i := 0; ; i++ {
		_, err := io.ReadFull(fh, buf[:])
		if errors.Is(err, io.EOF) {
			break
		}
		check(err)

		copy(hdr[:], buf[0:kwHeaderSize])
		fmt.Printf("node%d [label=\"n%d (%d)\"]\n", i, i, hdr.Count())

		if !hdr.Leaf() {
			for j := uint16(0); j < hdr.Count(); j++ {
				// TODO: what are these constants?
				child := binary.BigEndian.Uint32(buf[16+24*j+20:])
				fmt.Printf("node%d -> node%d;\n", i, child)
			}
		} else {
			fmt.Printf("{rank=same node%d node%d}\n", i, hdr.Next())
		}

		fmt.Printf("node%d -> node%d;\n", i, hdr.Next())
	}

	fmt.Println("}")
}
