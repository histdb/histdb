package query

import "fmt"

type inst struct {
	_ [0]func() // no equality

	op byte
	s  int16
	v  int16
}

const (
	// args are encoded in the instruction stream as varints

	// nop
	inst_nop = iota
	inst_true

	// comparison operators
	inst_eq  // push(query(eq,  i.tags, strs[i.arg1], strs[i.arg2]))
	inst_neq // push(query(neq, i.tags, strs[i.arg1], strs[i.arg2]))

	inst_re  // push(query(re,  i.tags, strs[i.arg1], strs[i.arg2]))
	inst_nre // push(query(nre, i.tags, strs[i.arg1], strs[i.arg2]))

	inst_glob  // push(query(glob,  i.tags, strs[i.arg1], strs[i.arg2]))
	inst_nglob // push(query(nglob, i.tags, strs[i.arg1], strs[i.arg2]))

	inst_gt  // push(query(gt,  i.tags, strs[i.arg1], strs[i.arg2]))
	inst_gte // push(query(gte, i.tags, strs[i.arg1], strs[i.arg2]))
	inst_lt  // push(query(lt,  i.tags, strs[i.arg1], strs[i.arg2]))
	inst_lte // push(query(lte, i.tags, strs[i.arg1], strs[i.arg2]))

	// selection operators
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
	case inst_true:
		return fmt.Sprintf("query(true, s=%d)", i.s)

	case inst_eq:
		prefix = "query(eq, "
	case inst_neq:
		prefix = "query(neq, "

	case inst_re:
		prefix = "query(re, "
	case inst_nre:
		prefix = "query(nre, "

	case inst_glob:
		prefix = "query(glob, "
	case inst_nglob:
		prefix = "query(nglob, "

	case inst_gt:
		prefix = "query(gt, "
	case inst_gte:
		prefix = "query(gte, "
	case inst_lt:
		prefix = "query(lt, "
	case inst_lte:
		prefix = "query(lte, "

	case inst_union:
		return "union"
	case inst_inter:
		return "inter"
	case inst_symdiff:
		return "symdiff"
	case inst_modulo:
		return "modulo"

	default:
		prefix = fmt.Sprintf("op%d(", i.op)
	}

	return fmt.Sprintf("%ss=%d, v=%d)", prefix, i.s, i.v)
}
