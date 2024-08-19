package engine

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"unicode"

	"github.com/leftmike/maho/pkg/types"
)

func skipWhitespace(rs io.RuneScanner) {
	for {
		r, _, err := rs.ReadRune()
		if err != nil {
			break
		} else if r != ' ' {
			rs.UnreadRune()
			break
		}
	}
}

func readIdentifier(rs io.RuneScanner, quoted bool) (types.Identifier, error) {
	var buf strings.Builder
	for {
		r, _, err := rs.ReadRune()
		if err == io.EOF && buf.Len() > 0 {
			break
		} else if err != nil {
			return 0, err
		} else if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' && r != '$' {
			rs.UnreadRune()
			break
		}

		buf.WriteRune(r)
	}

	return types.ID(buf.String(), quoted), nil
}

func readInteger(rs io.RuneScanner) (uint, error) {
	var ui uint
	var valid bool
	for {
		r, _, err := rs.ReadRune()
		if err == io.EOF {
			break
		} else if err != nil {
			return 0, err
		} else if r < '0' || r > '9' {
			rs.UnreadRune()
			break
		}

		ui = ui*10 + uint(r-'0')
		valid = true
	}

	if !valid {
		return 0, errors.New("typed table: expected an integer")
	}

	return ui, nil
}

func columnNumber(id types.Identifier, colNames []types.Identifier) (types.ColumnNum, bool) {
	for num, col := range colNames {
		if id == col {
			return types.ColumnNum(num), true
		}
	}
	return 0, false
}

func ParsePrimary(s string, colNames []types.Identifier) ([]types.ColumnKey, error) {
	// column [',' ...]

	rs := strings.NewReader(s)

	var key []types.ColumnKey
	for {
		if len(key) > 0 {
			skipWhitespace(rs)
			r, _, err := rs.ReadRune()
			if err == io.EOF {
				break
			} else if err != nil {
				return nil, err
			} else if r != ',' {
				return nil, fmt.Errorf("typed table: comma expected: %c", r)
			}
		}

		skipWhitespace(rs)
		id, err := readIdentifier(rs, true)
		if err != nil {
			return nil, err
		}

		num, ok := columnNumber(id, colNames)
		if !ok {
			return nil, fmt.Errorf("typed table: column not found: %s", id)
		}

		key = append(key, types.MakeColumnKey(num, false))
	}

	return key, nil
}

func ParseColumn(s string) (types.ColumnType, bool, error) {
	/*
		data_type [column_constraint ...]
		column_constraint =
		      NOT NULL
		    | PRIMARY KEY
		data_type =
			  BINARY ['(' length ')']
			| VARBINARY ['(' length ')']
			| BLOB ['(' length ')']
			| BYTEA ['(' length ')']
			| BYTES ['(' length ')']
			| CHAR ['(' length ')']
			| CHARACTER ['(' length ')']
			| VARCHAR ['(' length ')']
			| TEXT ['(' length ')']
			| BOOL
			| BOOLEAN
			| DOUBLE
			| REAL
			| SMALLINT
			| INT2
			| INT
			| INTEGER
			| INT4
			| INTEGER
			| BIGINT
			| INT8
	*/

	rs := strings.NewReader(s)

	skipWhitespace(rs)
	typ, err := readIdentifier(rs, false)
	if err != nil {
		return types.ColumnType{}, false, err
	}

	ct, found := types.ColumnTypes[typ]
	if !found {
		return ct, false, fmt.Errorf("typed table: expected a valid data type: %s", typ)
	}

	if ct.Type == types.StringType || ct.Type == types.BytesType {
		skipWhitespace(rs)
		r, _, err := rs.ReadRune()
		if err == io.EOF {
			return ct, false, nil
		} else if err != nil {
			return ct, false, err
		} else if r == '(' {
			skipWhitespace(rs)
			ui, err := readInteger(rs)
			if err != nil {
				return ct, false, err
			}

			skipWhitespace(rs)
			ct.Size = uint32(ui)

			skipWhitespace(rs)
			r, _, err := rs.ReadRune()
			if err != nil {
				return ct, false, err
			} else if r != ')' {
				return ct, false, fmt.Errorf("typed table: expected ): %c", r)
			}
		} else {
			rs.UnreadRune()
		}
	}

	var primary bool
	for {
		skipWhitespace(rs)
		id, err := readIdentifier(rs, false)
		if err == io.EOF {
			break
		} else if err != nil {
			return ct, false, err
		}

		if id == types.NOT {
			skipWhitespace(rs)
			id, err = readIdentifier(rs, false)
			if err != nil {
				return ct, false, err
			}
			if id != types.NULL {
				return ct, false, fmt.Errorf("typed table: expected NULL: %s", id)
			}

			ct.NotNull = true
		} else if id == types.PRIMARY {
			skipWhitespace(rs)
			id, err = readIdentifier(rs, false)
			if err != nil {
				return ct, false, err
			}
			if id != types.KEY {
				return ct, false, fmt.Errorf("typed table: expected KEY: %s", id)
			}

			primary = true
		} else {
			return ct, false, fmt.Errorf("typed table: expected a column constraint: %s", id)
		}
	}

	return ct, primary, nil
}
