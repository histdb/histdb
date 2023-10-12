package query

import "github.com/zeebo/errs/v2"

// tokens calls the callback with successive tokens from the input
// string x. It returns an error if the tokens are invalid.
func tokens(x []byte, cb func([]byte)) error {
	for len(x) > 0 {
		var t []byte
		t, x = token(x)
		if len(t) == 0 {
			return errs.Errorf("invalid token: %q", x)
		}
		cb(t)
	}
	return nil
}

// token returns the length of the next token in the input string x, or -1 if
// the token is invalid. x must not begin with whitespace.
func token(x []byte) ([]byte, []byte) {
	for len(x) > 0 && (x[0] == ' ' || x[0] == '\t') {
		x = x[1:]
	}

	// nothing left
	if len(x) == 0 {
		return nil, nil
	}

	// quoted strings
	if c := x[0]; c == '"' || c == '\'' {
		for i := uint(1); i < uint(len(x)); i++ {
			if x[i] == '\\' {
				i++
				continue
			}
			if x[i] == c {
				return x[:i+1], x[i+1:]
			}
		}
		return nil, x
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
			return x[:2], x[2:]
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
		',': /**/ // sel      tag key separator
		return x[:1], x[1:]
	}

	// tag keys
	for i := uint(0); i < uint(len(x)); i++ {
		c := x[i]
		if c == '\\' {
			if i+1 >= uint(len(x)) {
				return nil, x
			}
			i++
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
			return x[:i], x[i:]
		}
	}

	return x, nil
}

func isSpecial(x byte) bool {
	switch x {
	case
		' ', 0x9, // whitespace
		'&', '|', // conjunctives
		'>', '<', // comparison
		'=', '!', // equality/regex/glob (first char only)
		'{', '}', // selection
		'(', ')', // grouping
		'%', '^', // sel operators
		',',       // tag key separator
		'"', '\'': // quoted strings

		return true

	default:
		return false
	}
}
