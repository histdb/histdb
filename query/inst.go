package query

import "fmt"

type inst struct {
	_ [0]func() // no equality

	op byte
	s1 int16
	s2 int16
}

const (
	// args are encoded in the instruction stream as varints

	// nop
	inst_nop = iota

	// tags
	inst_tags // push(tags(strs[i.arg1]))

	// comparison operators
	inst_eq  // push(sel(eq,  strs[i.arg1], strs[i.arg2]))
	inst_neq // push(sel(neq, strs[i.arg1], strs[i.arg2]))

	inst_re  // push(sel(re,  strs[i.arg1], strs[i.arg2]))
	inst_nre // push(sel(nre, strs[i.arg1], strs[i.arg2]))

	inst_glob  // push(sel(glob,  strs[i.arg1], strs[i.arg2]))
	inst_nglob // push(sel(nglob, strs[i.arg1], strs[i.arg2]))

	// binary operators
	inst_union   // push(pop() | pop())
	inst_inter   // push(pop() & pop())
	inst_symdiff // push(pop() ^ pop())
	inst_modulo  // push(pop() % pop())
)

func (i inst) String() string {
	var prefix string

	switch i.op {
	case inst_nop:
		return "nop"
	case inst_tags:
		return fmt.Sprintf("(tags %d)", i.s1)

	case inst_eq:
		prefix = "(sel eq "
	case inst_neq:
		prefix = "(sel neq "

	case inst_re:
		prefix = "(sel re "
	case inst_nre:
		prefix = "(sel nre "

	case inst_glob:
		prefix = "(sel glob "
	case inst_nglob:
		prefix = "(sel nglob "

	case inst_union:
		return "union"
	case inst_inter:
		return "inter"
	case inst_symdiff:
		return "symdiff"
	case inst_modulo:
		return "modulo"

	default:
		prefix = fmt.Sprintf("(op%d ", i.op)
	}

	return fmt.Sprintf("%s%d %d)", prefix, i.s1, i.s2)
}
