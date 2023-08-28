package query

import (
	"fmt"
	"regexp"

	"github.com/zeebo/errs/v2"

	"github.com/histdb/histdb/memindex"
)

type Query struct {
	prog []inst
	strs [][]byte
	vals []value
}

func (q Query) Eval(m *memindex.T) (*memindex.Bitmap, error) {
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
			m.QueryTrue(q.strs[i.v1], push().Or)

		case inst_eq:
			// TODO: reduce allocations
			tag := []byte(fmt.Sprintf("%s=%s", q.strs[i.v1], q.vals[i.v2].AsString()))
			m.QueryEqual(tag, push().Or)

		case inst_neq:
			// TODO: reduce allocations
			tag := []byte(fmt.Sprintf("%s=%s", q.strs[i.v1], q.vals[i.v2].AsString()))
			m.QueryNotEqual(tag, push().Or)

		case inst_re: // TODO
			re, err := regexp.Compile(q.vals[i.v2].AsString())
			if err != nil {
				return nil, errs.Wrap(err)
			}
			m.QueryFilter(q.strs[i.v1], re.Match, push().Or)

		case inst_nre: // TODO
			re, err := regexp.Compile(q.vals[i.v2].AsString())
			if err != nil {
				return nil, errs.Wrap(err)
			}
			m.QueryFilter(q.strs[i.v1], func(b []byte) bool { return !re.Match(b) }, push().Or)

		case inst_glob: // TODO
			push()
		case inst_nglob: // TODO
			push()

		case inst_gt: // TODO
			push()
		case inst_gte: // TODO
			push()
		case inst_lt: // TODO
			push()
		case inst_lte: // TODO
			push()

		case inst_union:
			b := pop()
			top().Or(b)

		case inst_inter:
			b := pop()
			top().And(b)

		case inst_symdiff:
			b := pop()
			top().Xor(b)

		case inst_modulo:
			b := pop()
			top().AndNot(b)
		}
	}

	return top(), nil
}
