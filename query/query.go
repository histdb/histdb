package query

import (
	"strings"

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

func (q *Q) String() string {
	var b strings.Builder
	b.WriteString("(query (prog ")

	for i, inst := range q.prog {
		if i > 0 {
			b.WriteByte(' ')
		}

		switch inst.op {
		case inst_nop:
			b.WriteString("nop")
		case inst_tags:
			b.WriteString("(tags ")
			b.Write(q.strs.list[inst.s1])
			b.WriteString(")")

		case inst_eq:
			b.WriteString("(sel eq ")
			b.Write(q.strs.list[inst.s1])
			b.WriteByte(' ')
			b.Write(q.strs.list[inst.s2])
			b.WriteByte(')')
		case inst_neq:
			b.WriteString("(sel neq ")
			b.Write(q.strs.list[inst.s1])
			b.WriteByte(' ')
			b.Write(q.strs.list[inst.s2])
			b.WriteByte(')')

		case inst_re:
			b.WriteString("(sel re")
			b.Write(q.strs.list[inst.s1])
			b.WriteByte(' ')
			b.WriteString(q.mchs[inst.s2].q)
			b.WriteByte(')')
		case inst_nre:
			b.WriteString("(sel nre")
			b.Write(q.strs.list[inst.s1])
			b.WriteByte(' ')
			b.WriteString(q.mchs[inst.s2].q)
			b.WriteByte(')')

		case inst_glob:
			b.WriteString("(sel glob")
			b.Write(q.strs.list[inst.s1])
			b.WriteByte(' ')
			b.WriteString(q.mchs[inst.s2].q)
			b.WriteByte(')')
		case inst_nglob:
			b.WriteString("(sel nglob")
			b.Write(q.strs.list[inst.s1])
			b.WriteByte(' ')
			b.WriteString(q.mchs[inst.s2].q)
			b.WriteByte(')')

		case inst_union:
			b.WriteString("union")
		case inst_inter:
			b.WriteString("inter")
		case inst_symdiff:
			b.WriteString("symdiff")
		case inst_modulo:
			b.WriteString("modulo")

		default:
			b.WriteString("unknown")
		}
	}
	b.WriteString("))")

	return b.String()
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

		case inst_tags:
			m.QueryTrue(q.strs.list[i.s1], push().Or)

		case inst_eq:
			buf = appendTag(buf[:0], q.strs.list[i.s1], q.strs.list[i.s2])
			m.QueryEqual(buf, push().Or)

		case inst_neq:
			buf = appendTag(buf[:0], q.strs.list[i.s1], q.strs.list[i.s2])
			m.QueryNotEqual(q.strs.list[i.s1], buf, push().Or)

		case inst_re, inst_glob:
			m.QueryFilter(q.strs.list[i.s1], q.mchs[i.s2].fn, push().Or)
		case inst_nre, inst_nglob:
			m.QueryFilterNot(q.strs.list[i.s1], q.mchs[i.s2].fn, push().Or)

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
