package query

import (
	"github.com/histdb/histdb/memindex"
)

type Q struct {
	_ [0]func() // no equality

	prog []inst
	strs bytesSet
	mchs []matcher

	// memory cache for parsing
	toks  []token
	tkeys bytesSet
}

func (q *Q) Eval(m *memindex.T) *memindex.Bitmap {
	buf := make([]byte, 0, 32)
	stack := make([]*memindex.Bitmap, 1, 8)
	stack[0] = new(memindex.Bitmap)

	push := func() *memindex.Bitmap {
		if len(stack) < cap(stack) {
			stack = stack[:len(stack)+1]
			if stack[len(stack)-1] == nil {
				stack[len(stack)-1] = new(memindex.Bitmap)
			} else {
				stack[len(stack)-1].Clear()
			}
		} else {
			stack = append(stack, new(memindex.Bitmap))
		}
		return stack[len(stack)-1]
	}

	top := func() *memindex.Bitmap {
		if len(stack) == 0 {
			return nil
		}
		return stack[len(stack)-1]
	}

	pop := func() *memindex.Bitmap {
		if len(stack) == 0 {
			return nil
		}
		ret := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		return ret
	}

	for _, i := range q.prog {
		switch i.op & 15 {
		case inst_nop:

		case inst_true:
			m.QueryTrue(q.strs.list[i.s], push().Or)

		case inst_eq:
			buf = appendTag(buf[:0], q.strs.list[i.s], q.strs.list[i.v])
			m.QueryEqual(buf, push().Or)

		case inst_neq:
			buf = appendTag(buf[:0], q.strs.list[i.s], q.strs.list[i.v])
			m.QueryNotEqual(q.strs.list[i.s], buf, push().Or)

		case inst_re, inst_glob:
			m.QueryFilter(q.strs.list[i.s], q.mchs[i.v].fn, push().Or)
		case inst_nre, inst_nglob:
			m.QueryFilterNot(q.strs.list[i.s], q.mchs[i.v].fn, push().Or)

		case inst_union:
			b := pop()
			top().Or(b) // pop() must sequence before top()

		case inst_inter:
			b := pop() // pop() must sequence before top()
			top().And(b)

		case inst_symdiff:
			b := pop() // pop() must sequence before top()
			top().Xor(b)

		case inst_modulo:
			b := pop() // pop() must sequence before top()
			top().AndNot(b)
		}
	}

	return top()
}
