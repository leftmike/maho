package testutil

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"unicode"

	"github.com/leftmike/maho/parser"
	"github.com/leftmike/maho/types"
)

var (
	errUnexpectedEOF             = errors.New("parse: unexpected eof")
	errBadBytesString            = errors.New("parse: bad bytes string")
	errExpectedComma             = errors.New("parse: expected comma")
	errExpectedOpenParen         = errors.New("parse: expected open paren")
	errExpectedCloseParen        = errors.New("parse: expected close paren")
	errExpectedCommaOrCloseParen = errors.New("parse: expected comma or close paren")
	errExpectedIdentifier        = errors.New("parse: expected identifier")
	errExpectedNumber            = errors.New("parse: expected number")
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

func MustParseRow(s string) types.Row {
	row, err := ParseRow(strings.NewReader(s))
	if err != nil {
		panic(fmt.Sprintf("must parse row: %s: %s", s, err))
	}
	return row
}

func MustParseRows(s string) []types.Row {
	rows, err := ParseRows(strings.NewReader(s))
	if err != nil {
		panic(fmt.Sprintf("must parse rows: %s: %s", s, err))
	}
	return rows
}

func ParseIdentifier(rs io.RuneScanner) (types.Identifier, error) {
	r, err := skipWhitespace(rs)
	if err != nil {
		return 0, err
	}

	var buf strings.Builder
	for {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' && r != '$' {
			rs.UnreadRune()
			break
		}

		buf.WriteRune(r)

		r, _, err = rs.ReadRune()
		if err != nil {
			if err == io.EOF && buf.Len() > 0 {
				break
			}
			return 0, err
		}
	}

	s := buf.String()
	if len(s) == 0 {
		return 0, errExpectedIdentifier
	}

	return types.ID(buf.String(), false), nil
}

func ParseIdentifiers(rs io.RuneScanner) ([]types.Identifier, error) {
	var ids []types.Identifier
	for {
		id, err := ParseIdentifier(rs)
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)

		r, err := skipWhitespace(rs)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		} else if r != ',' {
			return nil, errExpectedComma
		}
	}

	return ids, nil
}

func parseOptionalSize(rs io.RuneScanner) (uint32, bool, error) {
	r, err := skipWhitespace(rs)
	if err == io.EOF {
		return 0, false, nil
	} else if err != nil {
		return 0, false, err
	} else if r != '(' {
		rs.UnreadRune()
		return 0, false, nil
	}

	r, err = skipWhitespace(rs)
	if err != nil {
		return 0, false, err
	}

	if !unicode.IsDigit(r) {
		return 0, false, errExpectedNumber
	}

	s, err := readRunes(rs, r, false,
		func(r rune) bool {
			return unicode.IsDigit(r)
		})
	if err != nil {
		return 0, false, err
	}

	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, false, err
	}

	r, err = skipWhitespace(rs)
	if err != nil {
		return 0, false, err
	} else if r != ')' {
		return 0, false, errExpectedCloseParen
	}

	return uint32(i), true, nil
}

func ParseColumns(rs io.RuneScanner) ([]types.Identifier, []types.ColumnType, []types.ColumnKey,
	error) {

	// column data_type [PRIMARY KEY | NOT NULL], ...

	var cols []types.Identifier
	var colTypes []types.ColumnType
	var key []types.ColumnKey
	for {
		col, err := ParseIdentifier(rs)
		if err != nil {
			return nil, nil, nil, err
		}
		cols = append(cols, col)

		typ, err := ParseIdentifier(rs)
		if err != nil {
			return nil, nil, nil, err
		}
		ct, found := parser.ColumnTypes[typ]
		if !found {
			return nil, nil, nil, fmt.Errorf("expected a data type, got %s", typ)
		}
		if ct.Type == types.StringType || ct.Type == types.BytesType {
			sz, ok, err := parseOptionalSize(rs)
			if err != nil {
				return nil, nil, nil, err
			} else if ok {
				ct.Size = sz
			}
		}
		colTypes = append(colTypes, ct)

		// XXX: [PRIMARY KEY | NOT NULL], ...

		r, err := skipWhitespace(rs)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, nil, nil, err
		} else if r != ',' {
			return nil, nil, nil, errExpectedComma
		}
	}

	return cols, colTypes, key, nil
}

func MustParseIdentifiers(s string) []types.Identifier {
	ids, err := ParseIdentifiers(strings.NewReader(s))
	if err != nil {
		panic(fmt.Sprintf("must parse identifiers: %s: %s", s, err))
	}
	return ids
}

func MustParseColumns(s string) ([]types.Identifier, []types.ColumnType, []types.ColumnKey) {
	cols, colTypes, key, err := ParseColumns(strings.NewReader(s))
	if err != nil {
		panic(fmt.Sprintf("must parse columns: %s: %s", s, err))
	}
	return cols, colTypes, key

}
