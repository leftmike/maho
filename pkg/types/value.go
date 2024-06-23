package types

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode/utf8"
)

type Value interface {
	String() string
}

type Row []Value

type ValueType int

const (
	UnknownType ValueType = iota // XXX: Is this needed?
	BoolType
	StringType
	BytesType
	Float64Type
	Int64Type
)

func (vt ValueType) String() string {
	switch vt {
	case UnknownType:
		return "UNKNOWN"
	case BoolType:
		return "BOOL"
	case StringType:
		return "STRING"
	case BytesType:
		return "BYTES"
	case Float64Type:
		return "DOUBLE"
	case Int64Type:
		return "INT"
	default:
		panic(fmt.Sprintf("unexpected datatype; got %#v", vt))
	}
}

type BoolValue bool

func (b BoolValue) String() string {
	if b {
		return "true"
	}
	return "false"
}

type Int64Value int64

func (i Int64Value) String() string {
	return fmt.Sprintf("%v", int64(i))
}

type Float64Value float64

func (d Float64Value) String() string {
	return fmt.Sprintf("%v", float64(d))
}

type StringValue string

func (s StringValue) String() string {
	return fmt.Sprintf("'%s'", string(s)) // XXX
}

type BytesValue []byte

var (
	hexDigits = [16]rune{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd',
		'e', 'f'}
)

func (b BytesValue) String() string {
	var buf bytes.Buffer
	buf.WriteString(`'\x`)
	for _, v := range b {
		buf.WriteRune(hexDigits[v>>4])
		buf.WriteRune(hexDigits[v&0xF])
	}

	buf.WriteRune('\'')
	return buf.String()
}

func Compare(val1, val2 Value) int {
	// XXX
	return 0
}

func FormatValue(v Value) string {
	if v == nil {
		return "NULL"
	}

	return v.String()
}

func (r Row) String() string {
	var buf bytes.Buffer
	buf.WriteRune('[')
	for idx, val := range r {
		if idx > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(FormatValue(val))
	}
	buf.WriteRune(']')
	return buf.String()
}

var (
	errNotNullValue = errors.New("expected a non-null value")
)

func CastValue(vt ValueType, val Value) (Value, error) {
	if val == nil {
		return nil, nil
	}

	switch vt {
	case BoolType:
		if sv, ok := val.(StringValue); ok {
			s := strings.Trim(string(sv), " \t\n")
			if s == "t" || s == "true" || s == "y" || s == "yes" || s == "on" || s == "1" {
				return BoolValue(true), nil
			} else if s == "f" || s == "false" || s == "n" || s == "no" || s == "off" || s == "0" {
				return BoolValue(false), nil
			} else {
				return nil, fmt.Errorf("expected a boolean value: %v", val)
			}
		} else if _, ok := val.(BoolValue); !ok {
			return nil, fmt.Errorf("expected a boolean value: %v", val)
		}
	case StringType:
		if i, ok := val.(Int64Value); ok {
			return StringValue(strconv.FormatInt(int64(i), 10)), nil
		} else if f, ok := val.(Float64Value); ok {
			return StringValue(strconv.FormatFloat(float64(f), 'g', -1, 64)), nil
		} else if b, ok := val.(BytesValue); ok {
			if !utf8.Valid([]byte(b)) {
				return nil, fmt.Errorf("expected a valid utf8 string: %v", val)
			}
			return StringValue(b), nil
		} else if _, ok := val.(StringValue); !ok {
			return nil, fmt.Errorf("expected a string value: %v", val)
		}
	case BytesType:
		if s, ok := val.(StringValue); ok {
			return BytesValue(s), nil
		} else if _, ok = val.(BytesValue); !ok {
			return nil, fmt.Errorf("expected a bytes value: %v", val)
		}
	case Float64Type:
		if i, ok := val.(Int64Value); ok {
			return Float64Value(i), nil
		} else if s, ok := val.(StringValue); ok {
			d, err := strconv.ParseFloat(strings.Trim(string(s), " \t\n"), 64)
			if err != nil {
				return nil, fmt.Errorf("expected a float: %v: %s", val, err)
			}
			return Float64Value(d), nil
		} else if _, ok := val.(Float64Value); !ok {
			return nil, fmt.Errorf("expected a float value: %v", val)
		}
	case Int64Type:
		if f, ok := val.(Float64Value); ok {
			return Int64Value(f), nil
		} else if s, ok := val.(StringValue); ok {
			i, err := strconv.ParseInt(strings.Trim(string(s), " \t\n"), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("expected an integer: %v: %s", val, err)
			}
			return Int64Value(i), nil
		} else if _, ok = val.(Int64Value); !ok {
			return nil, fmt.Errorf("expected an integer value: %v", val)
		}
	default:
		panic(fmt.Sprintf("expected a valid data type; got %v", vt))
	}

	return val, nil
}

func ConvertValue(ct ColumnType, val Value) (Value, error) {
	if val == nil {
		if ct.NotNull {
			return nil, errNotNullValue
		}
		return nil, nil
	}

	val, err := CastValue(ct.Type, val)
	if err != nil {
		return nil, err
	}

	switch ct.Type {
	case BoolType:
		if _, ok := val.(BoolValue); !ok {
			return nil, fmt.Errorf("expected a boolean value: %v", val)
		}
	case StringType:
		s, ok := val.(StringValue)
		if !ok {
			return nil, fmt.Errorf("expected a string value: %v", val)
		}

		if uint32(len(s)) > ct.Size {
			return nil, fmt.Errorf("string value too long: %d; expected %d", len(s), ct.Size)
		}
	case BytesType:
		b, ok := val.(BytesValue)
		if !ok {
			return nil, fmt.Errorf("expected a bytes value: %v", val)
		}

		if uint32(len(b)) > ct.Size {
			return nil, fmt.Errorf("bytes value too long: %d; expected %d", len(b), ct.Size)
		}
	case Float64Type:
		if _, ok := val.(Float64Value); !ok {
			return nil, fmt.Errorf("expected a float value: %v", val)
		}
	case Int64Type:
		i, ok := val.(Int64Value)
		if !ok {
			return nil, fmt.Errorf("expected an integer value: %v", val)
		}

		if ct.Size == 2 && (i > math.MaxInt16 || i < math.MinInt16) {
			return nil, fmt.Errorf("expected a 16 bit integer value: %d", i)
		} else if ct.Size == 4 && (i > math.MaxInt32 || i < math.MinInt32) {
			return nil, fmt.Errorf("expected a 32 bit integer value: %d", i)
		}
	default:
		panic(fmt.Sprintf("expected a valid data type; got %v", ct.Type))
	}

	return val, nil
}

func ConvertRow(colTypes []ColumnType, row Row) (Row, error) {
	var nrow Row

	for idx, val := range row {
		nval, err := ConvertValue(colTypes[idx], val)
		if err != nil {
			return nil, err
		}

		if nval != val && nrow == nil {
			nrow = append(make([]Value, 0, len(row)), row...)
		} else if nrow == nil {
			continue
		}

		nrow[idx] = nval
	}

	if nrow != nil {
		return nrow, nil
	}
	return row, nil
}

/*
database/sql package ==>
Scan converts from columns to Go types:
*string
*[]byte
*int, *int8, *int16, *int32, *int64
*uint, *uint8, *uint16, *uint32, *uint64
*bool
*float32, *float64
*interface{}
*RawBytes
any type implementing Scanner (see Scanner docs)

database/sql/driver package ==>
nil
int64
float64
bool
[]byte
string
time.Time
*/
