package testutil

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"unicode"

	"github.com/leftmike/maho/pkg/types"
)

var (
	errUnexpectedEOF             = errors.New("parse: unexpected eof")
	errBadBytesString            = errors.New("parse: bad bytes string")
	errExpectedComma             = errors.New("parse: expected comma")
	errExpectedOpenParen         = errors.New("parse: expected open paren")
	errExpectedCommaOrCloseParen = errors.New("parse: expected comma or close paren")
)

func skipWhitespace(rs io.RuneScanner) (rune, error) {
	for {
		r, _, err := rs.ReadRune()
		if err != nil {
			return 0, err
		}
		if !unicode.IsSpace(r) {
			return r, nil
		}
	}
}

func readRunes(rs io.RuneScanner, r rune, eofError bool, pred func(rune) bool) (string, error) {
	var buf strings.Builder
	if r != 0 {
		buf.WriteRune(r)
	}

	for {
		r, _, err := rs.ReadRune()
		if err == io.EOF {
			if eofError {
				return "", errUnexpectedEOF
			}
			break
		} else if err != nil {
			return "", errUnexpectedEOF
		}

		if pred(r) {
			buf.WriteRune(r)
		} else {
			rs.UnreadRune()
			break
		}
	}

	return buf.String(), nil
}

func hexDigit(ch byte) byte {
	if ch >= '0' && ch <= '9' {
		return ch - '0'
	} else if ch >= 'a' && ch <= 'f' {
		return ch - 'a' + 10
	}
	return ch - 'A' + 10
}

func ParseValue(rs io.RuneScanner) (types.Value, error) {
	r, err := skipWhitespace(rs)
	if err != nil {
		return nil, err
	}

	if unicode.IsDigit(r) || r == '.' {
		s, err := readRunes(rs, r, false,
			func(r rune) bool {
				return unicode.IsDigit(r) || r == '.'
			})
		if err != nil {
			return nil, err
		}

		if i, err := strconv.ParseInt(s, 10, 64); err == nil {
			return types.Int64Value(i), nil
		}

		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return types.Float64Value(f), nil
		}
		return nil, fmt.Errorf("parse: expected a number: %s", s)
	} else if unicode.IsLetter(r) {
		s, err := readRunes(rs, r, false,
			func(r rune) bool {
				return unicode.IsLetter(r)
			})
		if err != nil {
			return nil, err
		}

		s = strings.ToLower(s)
		switch s {
		case "true":
			return types.BoolValue(true), nil
		case "false":
			return types.BoolValue(false), nil
		case "null":
			return nil, nil
		default:
			return nil, fmt.Errorf("parse: unexpected identifier: %s", s)
		}
	} else if r != '\'' {
		return nil, fmt.Errorf("parse: unexpected rune parsing value: %v %d", r, r)
	}

	r, _, err = rs.ReadRune()
	if err == io.EOF {
		return nil, errUnexpectedEOF
	} else if err != nil {
		return nil, err
	}

	if r == '\\' {
		r, _, err = rs.ReadRune()
		if err == io.EOF {
			return nil, errUnexpectedEOF
		} else if err != nil {
			return nil, err
		} else if r != 'x' && r != 'X' {
			return nil, errBadBytesString
		}

		s, err := readRunes(rs, 0, true,
			func(r rune) bool {
				return r != '\''
			})
		if err != nil {
			return nil, err
		}
		rs.ReadRune()

		if len(s)%2 != 0 {
			return nil, errBadBytesString
		}

		b := make([]byte, len(s)/2)
		for idx := 0; idx < len(s); idx += 2 {
			b[idx/2] = (hexDigit(s[idx]) << 4) | hexDigit(s[idx+1])
		}

		return types.BytesValue(b), nil
	} else if r == '\'' {
		return types.StringValue(""), nil
	}

	s, err := readRunes(rs, r, true,
		func(r rune) bool {
			return r != '\''
		})
	if err != nil {
		return nil, err
	}
	rs.ReadRune()

	return types.StringValue(s), nil
}

func ParseRow(rs io.RuneScanner) (types.Row, error) {
	// (123, 'abc', true, 456.789, '\x010203')

	r, err := skipWhitespace(rs)
	if err != nil {
		return nil, err
	}

	if r != '(' {
		return nil, errExpectedOpenParen
	}

	var row types.Row
	for {
		_, err := skipWhitespace(rs)
		if err != nil {
			return nil, err
		}
		rs.UnreadRune()

		val, err := ParseValue(rs)
		if err != nil {
			return nil, err
		}

		row = append(row, val)

		r, err := skipWhitespace(rs)
		if err != nil {
			return nil, err
		}
		if r == ')' {
			break
		} else if r != ',' {
			return nil, errExpectedCommaOrCloseParen
		}
	}

	return row, nil
}

func ParseRows(rs io.RuneScanner) ([]types.Row, error) {
	// (123, 'abc', true), (456, 'def', false), (789, 'ghi', null)

	var rows []types.Row
	for {
		row, err := ParseRow(rs)
		if err != nil {
			return nil, err
		}

		rows = append(rows, row)

		r, err := skipWhitespace(rs)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		} else if r != ',' {
			return nil, errExpectedComma
		}
	}

	return rows, nil
}
