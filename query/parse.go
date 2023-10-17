package query

import (
	"bytes"
	"fmt"
	"regexp"

	"github.com/zeebo/errs/v2"
)

type parseState struct {
	tokn uint
	toks [][]byte

	tlock bool
	tkeys bytesSet

	prog []inst
	strs bytesSet
	vals valueSet
	mats []matcher
}

type matcher struct {
	fn func([]byte) bool
	k  string
	q  string
}

func (m matcher) String() string { return fmt.Sprintf("%s(%s)", m.k, m.q) }

func (ps *parseState) pushOp(op byte) { ps.pushInst(op, -1, -1) }
func (ps *parseState) pushInst(op byte, v1, v2 int64) {
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

func (ps *parseState) peek() []byte {
	if ps.tokn < uint(len(ps.toks)) {
		return ps.toks[ps.tokn]
	}
	return nil
}

func (ps *parseState) next() []byte {
	if ps.tokn < uint(len(ps.toks)) {
		s := ps.toks[ps.tokn]
		ps.tokn++
		return s
	}
	return nil
}

var (
	openBrace  = []byte{'{'}
	closeBrace = []byte{'}'}
	comma      = []byte{','}
)

func Parse(query []byte) (Query, error) {
	toks := make([][]byte, 1, 32)
	toks[0] = openBrace

	err := tokens(query, func(t []byte) { toks = append(toks, t) })
	if err != nil {
		return Query{}, err
	}

	// if we don't have any { or }, then infer add them
	var braces bool
	for _, t := range toks[1:] {
		if string(t) == "{" || string(t) == "}" {
			braces = true
			break
		}
	}
	if !braces {
		toks = append(toks, closeBrace)
	} else {
		toks = toks[1:]
	}

	ps := &parseState{
		toks: toks,

		tkeys: newBytesSet(8),

		prog: make([]inst, 0, 32),
		strs: newBytesSet(8),
		vals: newValueSet(8),
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
		mats: ps.mats,
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
	switch tok := ps.peek(); len(tok) {
	case 1:
		switch tok[0] {
		case '|':
			return inst_union
		case '&':
			return inst_inter
		case '^':
			return inst_symdiff
		case '%':
			return inst_modulo
		}
	}
	return 0
}

func (ps *parseState) parseSel() bool {
	skipInter := false

	if string(ps.peek()) == "(" {
		return ps.parseSelGroup()
	}

	if string(ps.next()) != "{" {
		return false
	}

	if string(ps.peek()) == "}" {
		skipInter = true
		ps.tokn++
		goto done
	}

	if n := ps.tokn; ps.parseCompoundComp() && string(ps.next()) == "}" {
		goto done
	} else {
		ps.tokn = n
	}

	for {
		if _, ok := ps.peekIdent(); ok {
			ps.tokn++
			if string(ps.peek()) == "," {
				ps.tokn++
				continue
			}
		}
		break
	}

	if string(ps.next()) != "|" {
		return false
	}

	// lock tkeys because we found a pipe
	ps.tlock = true

	if string(ps.peek()) == "}" {
		skipInter = true
		ps.tokn++
		goto done
	}

	if !ps.parseCompoundComp() {
		return false
	}

	if string(ps.next()) != "}" {
		return false
	}

done:

	ps.tlock = false

	// store and reset tags for next selection
	tn := ps.strs.add(bytes.Join(ps.tkeys.list, comma))
	ps.tkeys = newBytesSet(8)

	// push the intersection of the tagset for the metrics
	ps.pushInst(inst_true, tn, -1)

	if !skipInter {
		ps.pushOp(inst_inter)
	}

	return true
}

func (ps *parseState) parseSelGroup() bool {
	if string(ps.next()) != "(" {
		return false
	}

	if !ps.parseCompoundSel() {
		return false
	}

	if string(ps.next()) != ")" {
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
	switch tok := ps.peek(); len(tok) {
	case 1:
		switch tok[0] {
		case '|':
			return inst_union
		case '&', ',':
			return inst_inter
		}
	}
	return 0
}

func (ps *parseState) parseComp() bool {
	if string(ps.peek()) == "(" {
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

	var val int64
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
	switch tok := ps.next(); len(tok) {
	case 1:
		switch tok[0] {
		case '=':
			return inst_eq
		case '>':
			return inst_gt
		case '<':
			return inst_lt
		}
	case 2:
		switch u := uint16(tok[0])<<8 | uint16(tok[1]); u {
		case '='<<8 | '=':
			return inst_eq
		case '!'<<8 | '=':
			return inst_neq
		case '='<<8 | '~':
			return inst_re
		case '!'<<8 | '~':
			return inst_nre
		case '='<<8 | '*':
			return inst_glob
		case '!'<<8 | '*':
			return inst_nglob
		case '>'<<8 | '=':
			return inst_gte
		case '<'<<8 | '=':
			return inst_lte
		}
	}
	return 0
}

func (ps *parseState) parseCompGroup() bool {
	if string(ps.next()) != "(" {
		return false
	}

	if !ps.parseCompoundComp() {
		return false
	}

	if string(ps.next()) != ")" {
		return false
	}

	return true
}

func (ps *parseState) parseGlob() (int64, bool) {
	tok := ps.next()
	if len(tok) == 0 {
		return 0, false
	}

	if tok[0] == '"' || tok[0] == '\'' {
		tok = tok[1 : len(tok)-1]
	} else if isSpecial(tok[0]) {
		return 0, false
	}

	glob, ok := makeGlob(string(tok))
	if !ok {
		return 0, false
	}

	ps.mats = append(ps.mats, matcher{fn: glob, k: "glob", q: string(tok)})

	return int64(len(ps.mats) - 1), true
}

func (ps *parseState) parseRegexp() (int64, bool) {
	tok := ps.next()
	if len(tok) == 0 {
		return 0, false
	}

	if tok[0] == '"' || tok[0] == '\'' {
		tok = tok[1 : len(tok)-1]
	} else if isSpecial(tok[0]) {
		return 0, false
	}

	re, err := regexp.Compile(string(tok))
	if err != nil {
		return 0, false
	}

	ps.mats = append(ps.mats, matcher{fn: re.Match, k: "re", q: string(tok)})

	return int64(len(ps.mats) - 1), true
}

func (ps *parseState) parseValue() (int64, bool) {
	tok := ps.next()
	if len(tok) == 0 {
		return 0, false
	}

	// numeric values
	if ('0' <= tok[0] && tok[0] <= '9') || tok[0] == '-' {
		if num, ok := parseInt(tok); ok {
			return ps.vals.add(valInt(num)), true
		}
		if num, ok := parseFloat(tok); ok {
			return ps.vals.add(valFloat(num)), true
		}
	}

	// string values
	if tok[0] == '"' || tok[0] == '\'' {
		tok = tok[1 : len(tok)-1]
		// TODO: unescape
		return ps.vals.add(valBytes(tok)), true
	}

	// identifiers as values
	// TODO: unescape
	if isSpecial(tok[0]) {
		return 0, false
	}

	return ps.vals.add(valBytes(tok)), true
}

func (ps *parseState) peekIdent() (int64, bool) {
	tok := ps.peek()
	if len(tok) == 0 {
		return 0, false
	}

	// TODO: unescape
	if isSpecial(tok[0]) {
		return 0, false
	}

	if !ps.tkey(tok) {
		return 0, false
	}

	return ps.strs.add(tok), true
}
