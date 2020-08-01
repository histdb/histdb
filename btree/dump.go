package btree

import "fmt"

var (
	dumpLeaf = false
	dumpKeys = false
)

// dump constructs a dot graph of the btree
func dump(b *T) {
	var order []uint32
	var twalk func(*node, uint32)
	twalk = func(n *node, nid uint32) {
		if n.leaf {
			return
		}
		for i := uint16(0); i < n.count; i++ {
			order = append(order, n.payload[i].value)
		}
		if n.next != invalidNode {
			order = append(order, n.next)
		}
		for i := uint16(0); i < n.count; i++ {
			cid := n.payload[i].value
			twalk(b.nodes[cid], cid)
		}
	}
	order = append(order, b.rootid)
	twalk(b.root, b.rootid)

	output := func(nid uint32) {
		n := b.nodes[nid]
		fmt.Printf(`node%d [label=<<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0"><TR>`, nid)
		fmt.Printf(`<TD PORT="fb"> </TD><TD PORT="fn">n%d (%d)</TD>`, nid, n.count)
		if !n.leaf || dumpLeaf {
			for i := uint16(0); i < n.count; i++ {
				if dumpKeys {
					fmt.Printf(`<TD PORT="f%d">%s`, i, n.payload[i].key)
				} else {
					fmt.Printf(`<TD PORT="f%d">`, i)
				}
				if n.leaf {
					fmt.Printf(`:%d`, n.payload[i].value)
				}
				fmt.Printf(`</TD>`)
			}
		}
		fmt.Println(`<TD PORT="fe"> </TD></TR></TABLE>>];`)

		if !n.leaf {
			for i := uint16(0); i < n.count; i++ {
				fmt.Printf("node%d:f%d:s -> node%d:fn:n;\n", nid, i, n.payload[i].value)
			}
		}

		if n.parent != invalidNode {
			fmt.Println(`edge[constraint=false];`)
			fmt.Printf(`node%d:fn:n -> node%d:fn:s [style="dashed",color="#0000ff20"];`+"\n", nid, n.parent)
			fmt.Println(`edge[constraint=true];`)
		}
		if n.prev != invalidNode {
			fmt.Println(`edge[constraint=false];`)
			fmt.Printf(`node%d:fb:w -> node%d:fe:e [style="dashed",color="#0000ff20"];`+"\n", nid, n.prev)
			fmt.Println(`edge[constraint=true];`)
		}
		if n.next != invalidNode {
			if n.leaf {
				fmt.Printf(`node%d:fe:e -> node%d:fb:w;`+"\n", nid, n.next)
			} else {
				fmt.Printf(`node%d:fe:s -> node%d:fn:n;`+"\n", nid, n.next)
			}
		}
	}

	fmt.Println("digraph btree { node[shape=plaintext]; ordering=out; splines=line;")

	var seen = map[uint32]bool{}
	for _, nid := range order {
		output(nid)
		seen[nid] = true
	}

	for nid := len(b.nodes) - 1; nid >= 0; nid-- {
		if seen[uint32(nid)] {
			continue
		}
		output(uint32(nid))
	}

	var walk func(*node)
	walk = func(n *node) {
		if n.leaf {
			return
		}
		fmt.Printf("{rank=same ")
		for i := uint16(0); i < n.count; i++ {
			fmt.Printf("node%d ", n.payload[i].value)
		}
		if n.next != invalidNode {
			fmt.Printf("node%d", n.next)
		}
		fmt.Println("}")

		for i := uint16(0); i < n.count; i++ {
			walk(b.nodes[n.payload[i].value])
		}
		if n.next != invalidNode {
			walk(b.nodes[n.next])
		}
	}
	walk(b.root)

	fmt.Printf("{rank=same; ")
	for nid, n := range b.nodes {
		if n.leaf {
			fmt.Printf("node%d ", nid)
		}
	}
	fmt.Println("}")

	fmt.Println("}")
}
