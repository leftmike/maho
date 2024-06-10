package types

import (
	"bytes"
	"fmt"
)

type Value interface {
	String() string
}

type ValueType int

const (
	BooleanType ValueType = iota
	StringType
	BytesType
	FloatType
	IntegerType
	UnknownType // XXX: Is this needed?
)

func (vt ValueType) String() string {
	switch vt {
	case BooleanType:
		return "BOOL"
	case StringType:
		return "STRING"
	case BytesType:
		return "BYTES"
	case FloatType:
		return "DOUBLE"
	case IntegerType:
		return "INT"
	case UnknownType:
		return "UNKNOWN"
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

func FormatValue(v Value) string {
	if v == nil {
		return "NULL"
	}

	return v.String()
}
