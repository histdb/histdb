package query

import (
	"fmt"

	"github.com/zeebo/errs/v2"
)

type token uint32

const (
	token_invalid token = 0

	// literals are 0b1BBBBBBB_BBBBBBBB_FLLLLLLL_LLLLLLLL
	// where L and B are the length and byte offset of the token
	// and F is a flag for if the literal was quoted.

	token_and2  token = '&'<<8 | '&'
	token_or2   token = '|'<<8 | '|'
	token_gte   token = '>'<<8 | '='
	token_lte   token = '<'<<8 | '='
	token_eq2   token = '='<<8 | '='
	token_neq   token = '!'<<8 | '='
	token_re    token = '='<<8 | '~'
	token_nre   token = '!'<<8 | '~'
	token_glob  token = '='<<8 | '*'
	token_nglob token = '!'<<8 | '*'

	token_lparen token = '('
	token_rparen token = ')'
	token_eq1    token = '='
	token_gt     token = '>'
	token_lt     token = '<'
	token_and1   token = '&'
	token_or1    token = '|'
	token_lbrace token = '{'
	token_rbrace token = '}'
	token_mod    token = '%'
	token_xor    token = '^'
	token_comma  token = ','
)

func (t token) isLiteral() bool { return t&(1<<31) != 0 }
func (t token) isQuoted() bool  { return t&(1<<15) != 0 }
func (t token) litBounds() (uint, uint) {
	l, b := uint(t)&0x7FFF, (uint(t)>>16)&0x7FFF
	return b, b + l
}

func (t token) literal(x []byte) []byte {
	if b, e := t.litBounds(); b < e && e <= uint(len(x)) {
		return x[b:e]
	}
	return nil
}

func (t token) String() string {
	if t.isLiteral() {
		b, e := t.litBounds()
		q := map[bool]string{true: "quo", false: "lit"}[t.isQuoted()]
		return fmt.Sprintf("%s[%d:%d]", q, b, e)
	}
	if t >= 256 {
		return fmt.Sprintf("%c%c", t>>8, t&0xFF)
	}
	return fmt.Sprintf("%c", t)
}

func tokens(x []byte, cb func(token)) error {
	if uint(len(x)) > 1<<15 {
		return errs.Errorf("query too long")
	}
	for pos := uint(0); uint(pos) < uint(len(x)); {
		t, n := nextToken(pos, x)
		if n == 0 {
			return errs.Errorf("invalid token: %q", x[pos:])
		} else if t == token_invalid {
			break
		}
		cb(t)
		pos += n
	}
	return nil
}

func nextToken(pos uint, x []byte) (t token, l uint) {
	if pos >= uint(len(x)) {
		return token_invalid, 0
	}
	x = x[pos:]

	for len(x) > 0 && (x[0] == ' ' || x[0] == '\t') {
		x = x[1:]
		pos++
		l++
	}

	// nothing left
	if len(x) == 0 {
		return token_invalid, l
	}

	// quoted strings
	if c := x[0]; c == '"' || c == '\'' {
		for i := uint(1); i < uint(len(x)); i++ {
			if x[i] == '\\' {
				if i+1 >= uint(len(x)) {
					return token_invalid, 0
				}
				i++
				if !isSpecial(x[i]) {
					return token_invalid, 0
				}
				continue
			}
			if x[i] == c {
				return token(1<<31 | 1<<15 | (pos+1)<<16 | i - 1), l + i + 1
			}
		}
		return token_invalid, 0
	}

	// length 2 operators
	if len(x) > 1 {
		switch u := uint16(x[0])<<8 | uint16(x[1]); u {
		case
			'&'<<8 | '&', '|'<<8 | '|', // conjunctives
			'>'<<8 | '=', '<'<<8 | '=', // comparison
			'='<<8 | '=', '!'<<8 | '=', // equality
			'='<<8 | '~', '!'<<8 | '~', // regex
			'='<<8 | '*', '!'<<8 | '*': // glob
			return token(u), l + 2
		}
	}

	// length 1 operators
	switch x[0] {
	case
		'(', ')', // sel/expr grouping
		'=',      // expr     equality
		'>', '<', // expr     comparison
		'&', '|', // sel      conjunctives
		'{', '}', // sel      selection delims
		'%', '^', // sel      sel operators
		',': /**/ // sel      tag key separator & conjunction
		return token(x[0]), l + 1
	}

	// tag keys
	for i := uint(0); i < uint(len(x)); i++ {
		c := x[i]
		if c == '\\' {
			if i+1 >= uint(len(x)) {
				return token_invalid, 0
			}
			i++
			if !isSpecial(x[i]) {
				return token_invalid, 0
			}
			continue
		}

		// ok so metrics.PopTag is used to parse the query
		// we pass into the memindex, and this needs to construct
		// that query. that will take anything that's not a comma
		// or an = and use that as literally the tag. so consider
		// a metric like `foo}=2` and a query:
		// 	{foo} | 2 == foo}}
		// that obviously is broken. it needs to look like
		//	{foo\} | 2 == foo\}}
		// but we need to parse the escapes out when we pass it
		// down to the query layer. the decision here is to make
		// that the job of the parser, not the tokenizer.
		// that means the tokenizer can assume proper escaping
		// and the parser is responsible for constructing an
		// accurate query.

		if isSpecial(c) {
			return token(1<<31 | pos<<16 | i), l + i
		}
	}

	return token(1<<31 | pos<<16 | uint(len(x))), l + uint(len(x))
}

func isSpecial(x byte) bool {
	switch x {
	case
		' ', '\t', // whitespace
		'&', '|', // conjunctives
		'>', '<', // comparison
		'=', '!', // equality/regex/glob (first char only)
		'{', '}', // selection
		'(', ')', // grouping
		'%', '^', // sel operators
		',',       // tag key separator
		'\\',      // escape character
		'"', '\'': // quoted strings

		return true

	default:
		return false
	}
}
