package testutil

import (
	"errors"
	"fmt"
	"io"
	"runtime"
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

func panicError(err error) {
	panic(err)
}

func readRune(rs io.RuneScanner) rune {
	r, _, err := rs.ReadRune()
	if err == io.EOF {
		panicError(errUnexpectedEOF)
	} else if err != nil {
		panicError(err)
	}

	return r
}

func skipWhitespace(rs io.RuneScanner, eofAllowed bool) (rune, bool) {
	for {
		r, _, err := rs.ReadRune()
		if err == io.EOF {
			if eofAllowed {
				return 0, true
			}
			panicError(errUnexpectedEOF)
		} else if err != nil {
			panicError(err)
		}
		if !unicode.IsSpace(r) {
			return r, false
		}
	}
}

func expectRune(rs io.RuneScanner, e rune, err error, eofAllowed bool) bool {
	r, eof := skipWhitespace(rs, eofAllowed)
	if eof {
		return true
	} else if r != e {
		panicError(err)
	}

	return false
}

func readRunes(rs io.RuneScanner, r rune, eofAllowed bool, pred func(rune) bool) string {
	var buf strings.Builder
	if r != 0 {
		buf.WriteRune(r)
	}

	for {
		r, _, err := rs.ReadRune()
		if err == io.EOF {
			if eofAllowed {
				break
			}
			panicError(errUnexpectedEOF)
		} else if err != nil {
			panicError(err)
		}

		if pred(r) {
			buf.WriteRune(r)
		} else {
			rs.UnreadRune()
			break
		}
	}

	return buf.String()
}

func hexDigit(ch byte) byte {
	if ch >= '0' && ch <= '9' {
		return ch - '0'
	} else if ch >= 'a' && ch <= 'f' {
		return ch - 'a' + 10
	}
	return ch - 'A' + 10
}

func parseValue(rs io.RuneScanner) types.Value {
	r, _ := skipWhitespace(rs, false)

	if unicode.IsDigit(r) || r == '.' {
		s := readRunes(rs, r, true,
			func(r rune) bool {
				return unicode.IsDigit(r) || r == '.'
			})

		if i, err := strconv.ParseInt(s, 10, 64); err == nil {
			return types.Int64Value(i)
		}

		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return types.Float64Value(f)
		}
		panicError(fmt.Errorf("parse: expected a number: %s", s))
	} else if unicode.IsLetter(r) {
		s := strings.ToLower(readRunes(rs, r, true, unicode.IsLetter))
		switch s {
		case "true":
			return types.BoolValue(true)
		case "false":
			return types.BoolValue(false)
		case "null":
			return nil
		default:
			panicError(fmt.Errorf("parse: unexpected identifier: %s", s))
		}
	} else if r != '\'' {
		panicError(fmt.Errorf("parse: unexpected rune parsing value: %v %d", r, r))
	}

	r = readRune(rs)
	if r == '\\' {
		r = readRune(rs)
		if r != 'x' && r != 'X' {
			panicError(errBadBytesString)
		}

		s := readRunes(rs, 0, false,
			func(r rune) bool {
				return r != '\''
			})
		rs.ReadRune()

		if len(s)%2 != 0 {
			panicError(errBadBytesString)
		}

		b := make([]byte, len(s)/2)
		for idx := 0; idx < len(s); idx += 2 {
			b[idx/2] = (hexDigit(s[idx]) << 4) | hexDigit(s[idx+1])
		}

		return types.BytesValue(b)
	} else if r == '\'' {
		return types.StringValue("")
	}

	s := readRunes(rs, r, false,
		func(r rune) bool {
			return r != '\''
		})
	rs.ReadRune()

	return types.StringValue(s)
}

func ParseValue(rs io.RuneScanner) (val types.Value, err error) {
	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(runtime.Error); ok {
				panic(r)
			}
			err = r.(error)
			val = nil
		}
	}()

	val = parseValue(rs)
	return
}

func parseRow(rs io.RuneScanner) types.Row {
	// (123, 'abc', true, 456.789, '\x010203')

	expectRune(rs, '(', errExpectedOpenParen, false)

	var row types.Row
	for {
		skipWhitespace(rs, false)
		rs.UnreadRune()

		row = append(row, parseValue(rs))

		r, _ := skipWhitespace(rs, false)
		if r == ')' {
			break
		} else if r != ',' {
			panicError(errExpectedCommaOrCloseParen)
		}
	}

	return row
}

func ParseRow(rs io.RuneScanner) (row types.Row, err error) {
	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(runtime.Error); ok {
				panic(r)
			}
			err = r.(error)
			row = nil
		}
	}()

	row = parseRow(rs)
	return
}

func parseRows(rs io.RuneScanner) []types.Row {
	// (123, 'abc', true), (456, 'def', false), (789, 'ghi', null)

	var rows []types.Row
	for {
		rows = append(rows, parseRow(rs))

		if expectRune(rs, ',', errExpectedComma, true) {
			break
		}
	}

	return rows
}

func ParseRows(rs io.RuneScanner) (rows []types.Row, err error) {
	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(runtime.Error); ok {
				panic(r)
			}
			err = r.(error)
			rows = nil
		}
	}()

	rows = parseRows(rs)
	return
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

func identifierRune(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' || r == '$'
}

func expectIdentifier(rs io.RuneScanner) types.Identifier {
	r, _ := skipWhitespace(rs, false)
	if !identifierRune(r) {
		panicError(errExpectedIdentifier)
	}

	return types.ID(readRunes(rs, r, true, identifierRune), false)
}

func optionalIdentifier(rs io.RuneScanner) (types.Identifier, bool) {
	r, eof := skipWhitespace(rs, true)
	if eof {
		return 0, false
	} else if !identifierRune(r) {
		rs.UnreadRune()
		return 0, false
	}

	return types.ID(readRunes(rs, r, true, identifierRune), false), true
}

func ParseIdentifier(rs io.RuneScanner) (id types.Identifier, err error) {
	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(runtime.Error); ok {
				panic(r)
			}
			err = r.(error)
			id = 0
		}
	}()

	id = expectIdentifier(rs)
	return
}

func parseIdentifiers(rs io.RuneScanner) []types.Identifier {
	var ids []types.Identifier
	for {
		ids = append(ids, expectIdentifier(rs))

		if expectRune(rs, ',', errExpectedComma, true) {
			break
		}
	}

	return ids
}

func ParseIdentifiers(rs io.RuneScanner) (ids []types.Identifier, err error) {
	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(runtime.Error); ok {
				panic(r)
			}
			err = r.(error)
			ids = nil
		}
	}()

	ids = parseIdentifiers(rs)
	return
}

func parseOptionalSize(rs io.RuneScanner) (uint32, bool) {
	r, eof := skipWhitespace(rs, true)
	if eof {
		return 0, false
	} else if r != '(' {
		rs.UnreadRune()
		return 0, false
	}

	r, _ = skipWhitespace(rs, false)
	if !unicode.IsDigit(r) {
		panicError(errExpectedNumber)
	}

	i, err := strconv.ParseInt(readRunes(rs, r, true, unicode.IsDigit), 10, 64)
	if err != nil {
		panicError(err)
	}

	expectRune(rs, ')', errExpectedCloseParen, false)
	return uint32(i), true
}

func parseColumns(rs io.RuneScanner) ([]types.Identifier, []types.ColumnType, []types.ColumnKey) {
	// column data_type [PRIMARY KEY | NOT NULL], ...

	var cols []types.Identifier
	var colTypes []types.ColumnType
	var key []types.ColumnKey
	for {
		cols = append(cols, expectIdentifier(rs))

		typ := expectIdentifier(rs)
		ct, found := parser.ColumnTypes[typ]
		if !found {
			panicError(fmt.Errorf("expected a data type, got %s", typ))
		}
		if ct.Type == types.StringType || ct.Type == types.BytesType {
			sz, ok := parseOptionalSize(rs)
			if ok {
				ct.Size = sz
			}
		}

		for {
			id, ok := optionalIdentifier(rs)
			if !ok {
				break
			}

			switch id {
			case types.PRIMARY:
				if expectIdentifier(rs) != types.KEY {
					panicError(fmt.Errorf("expected KEY, got %s", id))
				}
				key = append(key, types.MakeColumnKey(types.ColumnNum(len(cols)-1), false))
			case types.NOT:
				if expectIdentifier(rs) != types.NULL {
					panicError(fmt.Errorf("expected NULL, got %s", id))
				}
				ct.NotNull = true
			default:
				panicError(fmt.Errorf("expected a keyword, got %s", id))
			}
		}
		colTypes = append(colTypes, ct)

		if expectRune(rs, ',', errExpectedComma, true) {
			break
		}
	}

	return cols, colTypes, key
}

func ParseColumns(rs io.RuneScanner) (cols []types.Identifier, colTypes []types.ColumnType,
	key []types.ColumnKey, err error) {

	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(runtime.Error); ok {
				panic(r)
			}
			err = r.(error)
			cols = nil
			colTypes = nil
			key = nil
		}
	}()

	cols, colTypes, key = parseColumns(rs)
	return
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
