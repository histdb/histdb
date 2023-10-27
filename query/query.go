package query

import (
	"strconv"

	"github.com/histdb/histdb/memindex"
)

type Query struct {
	_ [0]func() // no equality

	prog []inst
	strs [][]byte
	vals []value
	mchs []matcher
}

func (q Query) Eval(m *memindex.T) (*memindex.Bitmap, error) {
	buf := make([]byte, 0, 16)
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
			m.QueryTrue(q.strs[i.s], push().Or)

		case inst_eq:
			buf = appendTag(buf[:0], q.strs[i.s], q.vals[i.v].AsString())
			m.QueryEqual(buf, push().Or)

		case inst_neq:
			buf = appendTag(buf[:0], q.strs[i.s], q.vals[i.v].AsString())
			m.QueryNotEqual(q.strs[i.s], buf, push().Or)

		case inst_re, inst_glob:
			m.QueryFilter(q.strs[i.s], q.mchs[i.v].fn, push().Or)
		case inst_nre, inst_nglob:
			m.QueryFilterNot(q.strs[i.s], q.mchs[i.v].fn, push().Or)

		// TODO: these could be directly supported by the memindex so that it
		// doesn't need to do the more expensive QueryFilter. it would have to
		// to do some sort of ordered index binary search. that requires maintaining
		// orders in the memindex, which it currently does not do at all.

		case inst_gt, inst_gte, inst_lt, inst_lte:
			v := q.vals[i.v]
			var vi, vf, vb value

			// prepare stringified versions of the value
			switch v.Tag() {
			case tagInt:
				buf = strconv.AppendInt(buf[:0], v.AsInt(), 10)
				vi = valBytes(buf)
			case tagFloat:
				buf = strconv.AppendFloat(buf[:0], v.AsFloat(), 'f', -1, 64)
				vf = valBytes(buf)
			case tagBool:
				if v.AsBool() {
					vb = valStr("true")
				} else {
					vb = valStr("false")
				}
			}

			// determine which comparison function to use
			var cmp func(x, y value) bool
			switch i.op {
			case inst_gt:
				cmp = valueGT
			case inst_gte:
				cmp = valueGTE
			case inst_lt:
				cmp = valueLT
			case inst_lte:
				cmp = valueLTE
			}

			m.QueryFilter(q.strs[i.s], func(b []byte) bool {
				vv := v
				bv := valBytes(b)

				switch v.Tag() {
				case tagStr:
				case tagInt:
					if bi, ok := parseInt(b); ok {
						bv = valInt(bi)
					} else {
						vv = vi
					}
				case tagFloat:
					if bf, ok := parseFloat(b); ok {
						bv = valFloat(bf)
					} else {
						vv = vf
					}
				case tagBool:
					vv = vb
				default:
					return false
				}

				return cmp(bv, vv)
			}, push().Or)

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

	return top(), nil
}
