package query

import (
	"bytes"
	"regexp"

	"github.com/zeebo/errs/v2"
)

type parseState struct {
	query []byte

	tokn uint
	toks []token

	tlock bool
	tkeys bytesSet

	prog []inst
	strs bytesSet
	vals valueSet
	mchs []matcher
}

func (ps *parseState) pushOp(op byte) { ps.pushInst(op, -1, -1) }
func (ps *parseState) pushInst(op byte, v1, v2 int16) {
	ps.prog = append(ps.prog, inst{
		op: op,
		s:  v1,
		v:  v2,
	})
}

func (ps *parseState) tkey(s []byte) bool {
	if ps.tlock {
		if _, ok := ps.tkeys.lookup(s); !ok {
			return false
		}
	}
	_ = ps.tkeys.add(s)
	return true
}

func (ps *parseState) peek() token {
	if ps.tokn < uint(len(ps.toks)) {
		return ps.toks[ps.tokn]
	}
	return token_invalid
}

func (ps *parseState) next() token {
	if ps.tokn < uint(len(ps.toks)) {
		s := ps.toks[ps.tokn]
		ps.tokn++
		return s
	}
	return token_invalid
}

func Parse(query []byte) (Query, error) {
	toks := make([]token, 1, 32)
	toks[0] = token_lbrace

	var braces bool
	err := tokens(query, func(t token) {
		toks = append(toks, t)
		braces = braces || (t == token_lbrace || t == token_rbrace)
	})
	if err != nil {
		return Query{}, err
	}

	// if we had no braces, infer add the right one. otherwise,
	// remove the prefix left one we already added.
	if !braces {
		toks = append(toks, token_rbrace)
	} else {
		toks = toks[1:]
	}

	ps := &parseState{
		query: query,

		toks: toks,

		prog: make([]inst, 0, 32),
	}

	if ok := ps.parseCompoundSel(); !ok || ps.tokn != uint(len(ps.toks)) {
		return Query{}, errs.Errorf("bad parse: %q", query)
	}

	// TODO: escaping values is problematic. {name = \(*Dir\).Commit} vs {name = "\(*Dir\).Commit"}
	// TODO: escaping tkeys is problematic.  {foo\= = 'bar'}

	return Query{
		prog: ps.prog,
		strs: ps.strs.list,
		vals: ps.vals.list,
		mchs: ps.mchs,
	}, nil
}

func (ps *parseState) parseCompoundSel() bool {
	if !ps.parseSel() {
		return false
	}

	for {
		op := ps.peekSelConjugate()
		if op == 0 {
			return true
		}
		ps.tokn++

		if n := ps.tokn; !ps.parseSel() {
			ps.tokn = n
			return true
		}

		ps.pushOp(op)
	}
}

func (ps *parseState) peekSelConjugate() (op byte) {
	switch tok := ps.peek(); tok {
	case token_or1:
		return inst_union
	case token_and1:
		return inst_inter
	case token_xor:
		return inst_symdiff
	case token_mod:
		return inst_modulo
	default:
		return 0
	}
}

func (ps *parseState) parseSel() (ok bool) {
	skipInter := false

	if ps.peek() == token_lparen {
		return ps.parseSelGroup()
	}

	if ps.next() != token_lbrace {
		return false
	}

	if ps.peek() == token_rbrace {
		skipInter = true
		ps.tokn++
		goto done
	}

	if n := ps.tokn; ps.parseCompoundComp() && ps.next() == token_rbrace {
		goto done
	} else {
		ps.tokn = n
	}

	for {
		if _, ok := ps.peekIdent(); ok {
			ps.tokn++
			if ps.peek() == token_comma {
				ps.tokn++
				continue
			}
		}
		break
	}

	if ps.next() != token_or1 {
		return false
	}

	// lock tkeys because we found a pipe
	ps.tlock = true

	if ps.peek() == token_rbrace {
		skipInter = true
		ps.tokn++
		goto done
	}

	if !ps.parseCompoundComp() {
		return false
	}

	if ps.next() != token_rbrace {
		return false
	}

done:

	ps.tlock = false

	// store and reset tags for next selection
	tn := ps.strs.add(bytes.Join(ps.tkeys.list, []byte{','}))
	ps.tkeys = bytesSet{}

	// push the intersection of the tagset for the metrics
	ps.pushInst(inst_true, tn, -1)

	if !skipInter {
		ps.pushOp(inst_inter)
	}

	return true
}

func (ps *parseState) parseSelGroup() bool {
	if ps.next() != token_lparen {
		return false
	}

	if !ps.parseCompoundSel() {
		return false
	}

	if ps.next() != token_rparen {
		return false
	}

	return true
}

func (ps *parseState) parseCompoundComp() bool {
	if !ps.parseComp() {
		return false
	}

	for {
		op := ps.peekCompConjugate()
		if op == 0 {
			return true
		}
		ps.tokn++

		if n := ps.tokn; !ps.parseComp() {
			ps.tokn = n
			return true
		}

		ps.pushOp(op)
	}
}

func (ps *parseState) peekCompConjugate() (op byte) {
	switch tok := ps.peek(); tok {
	case token_or1:
		return inst_union
	case token_and1, token_comma:
		return inst_inter
	default:
		return 0
	}
}

func (ps *parseState) parseComp() bool {
	if ps.peek() == token_lparen {
		return ps.parseCompGroup()
	}

	tag, ok := ps.peekIdent()
	if !ok {
		return false
	}
	ps.tokn++

	op := ps.parseCompComparison()
	if op == 0 {
		return false
	}

	var val int16
	switch op {
	case inst_re, inst_nre:
		val, ok = ps.parseRegexp()
	case inst_glob, inst_nglob:
		val, ok = ps.parseGlob()
	default:
		val, ok = ps.parseValue()
	}
	if !ok {
		return false
	}

	ps.pushInst(op, tag, val)
	return true
}

func (ps *parseState) parseCompComparison() (op byte) {
	switch tok := ps.next(); tok {
	case token_eq1, token_eq2:
		return inst_eq
	case token_neq:
		return inst_neq

	case token_gt:
		return inst_gt
	case token_gte:
		return inst_gte
	case token_lt:
		return inst_lt
	case token_lte:
		return inst_lte

	case token_re:
		return inst_re
	case token_nre:
		return inst_nre

	case token_glob:
		return inst_glob
	case token_nglob:
		return inst_nglob

	default:
		return 0
	}
}

func (ps *parseState) parseCompGroup() bool {
	if ps.next() != token_lparen {
		return false
	}

	if !ps.parseCompoundComp() {
		return false
	}

	if ps.next() != token_rparen {
		return false
	}

	return true
}

func (ps *parseState) parseGlob() (int16, bool) {
	tok := ps.next()
	if !tok.isLiteral() {
		return 0, false
	}

	lit := tok.literal(ps.query)
	if len(lit) == 0 {
		return 0, false
	}
	lits := string(lit)

	glob, ok := makeGlob(lits)
	if !ok {
		return 0, false
	}

	ps.mchs = append(ps.mchs, matcher{
		fn: glob,
		k:  "glob",
		q:  lits,
	})

	return int16(len(ps.mchs) - 1), true
}

func (ps *parseState) parseRegexp() (int16, bool) {
	tok := ps.next()
	if !tok.isLiteral() {
		return 0, false
	}

	lit := tok.literal(ps.query)
	if len(lit) == 0 {
		return 0, false
	}
	lits := string(lit)

	re, err := regexp.Compile(lits)
	if err != nil {
		return 0, false
	}

	ps.mchs = append(ps.mchs, matcher{
		fn: re.Match,
		k:  "re",
		q:  lits,
	})

	return int16(len(ps.mchs) - 1), true
}

func (ps *parseState) parseValue() (int16, bool) {
	tok := ps.next()
	if !tok.isLiteral() {
		return 0, false
	}

	lit := tok.literal(ps.query)
	if len(lit) == 0 {
		return 0, false
	}

	// numeric values
	if ('0' <= lit[0] && lit[0] <= '9') || lit[0] == '-' {
		if num, ok := parseInt(lit); ok {
			return ps.vals.add(valInt(num)), true
		}
		if num, ok := parseFloat(lit); ok {
			return ps.vals.add(valFloat(num)), true
		}
	}

	return ps.vals.add(valBytes(lit)), true
}

func (ps *parseState) peekIdent() (int16, bool) {
	tok := ps.peek()
	if !tok.isLiteral() || tok.isQuoted() {
		return 0, false
	}

	lit := tok.literal(ps.query)
	if len(lit) == 0 {
		return 0, false
	}

	if !ps.tkey(lit) {
		return 0, false
	}

	return ps.strs.add(lit), true
}
