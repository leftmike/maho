package types_test

import (
	"bytes"
	"math"
	"testing"

	"github.com/leftmike/maho/pkg/types"
)

func TestValue(t *testing.T) {
	cases := []struct {
		val types.Value
		s   string
	}{
		{types.BoolValue(true), "true"},
		{types.BoolValue(false), "false"},
		{types.Int64Value(123), "123"},
		{types.Int64Value(0xFF), "255"},
		{types.Int64Value(-123), "-123"},
		{types.Int64Value(0), "0"},
		{types.Float64Value(1.234), "1.234"},
		{types.Float64Value(0.0), "0"},
		{types.Float64Value(-1.234), "-1.234"},
		{types.StringValue("abcdef"), "'abcdef'"},
		{types.StringValue(""), "''"},
		{types.BytesValue(bytes.Repeat([]byte{0x12, 0xef}, 3)), `'\x12ef12ef12ef'`},
		{types.BytesValue(nil), `'\x'`},
		{types.BytesValue([]byte{}), `'\x'`},
	}

	for _, c := range cases {
		s := c.val.String()
		if s != c.s {
			t.Errorf("%#v (%T) got %s want %s", c.val, c.val, s, c.s)
		}

		s = types.FormatValue(c.val)
		if s != c.s {
			t.Errorf("FormatValue(%#v) got %s want %s", c.val, s, c.s)
		}
	}

	s := types.FormatValue(nil)
	if s != "NULL" {
		t.Errorf("FormatValue(nil) got %s want NULL", s)
	}
}

func valueTypeString(vt types.ValueType) (s string, panicked bool) {
	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(string); ok {
				panicked = true
			} else {
				panic(r)
			}
		}
	}()

	return vt.String(), false
}

func TestValueType(t *testing.T) {
	cases := []struct {
		vt types.ValueType
		s  string
		p  bool
	}{
		{types.BoolType, "BOOL", false},
		{types.StringType, "STRING", false},
		{types.BytesType, "BYTES", false},
		{types.Float64Type, "DOUBLE", false},
		{types.Int64Type, "INT", false},
		{types.UnknownType, "UNKNOWN", false},
		{types.ValueType(-1), "", true},
	}

	for _, c := range cases {
		s, p := valueTypeString(c.vt)
		if s != c.s {
			t.Errorf("%#v.String() got %s want %s", c.vt, s, c.s)
		}
		if p != c.p {
			if c.p {
				t.Errorf("%#v.String() did not panic", c.vt)
			} else {
				t.Errorf("%#v.String() panicked", c.vt)
			}
		}
	}
}

func TestRow(t *testing.T) {
	cases := []struct {
		r types.Row
		s string
	}{
		{
			r: types.Row{types.BoolValue(true), types.Int64Value(123), types.Float64Value(0.0)},
			s: "[true, 123, 0]",
		},
		{types.Row{}, "[]"},
		{nil, "[]"},
		{types.Row{types.BoolValue(false)}, "[false]"},
		{
			r: types.Row{
				types.Int64Value(0xFF),
				types.Int64Value(-123),
				types.Float64Value(1.234),
				types.StringValue("abcdef"),
			},
			s: "[255, -123, 1.234, 'abcdef']",
		},
		{
			r: types.Row{types.StringValue(""),
				types.BytesValue(bytes.Repeat([]byte{0x12, 0xef}, 3)),
				types.BytesValue([]byte{}),
			},
			s: `['', '\x12ef12ef12ef', '\x']`,
		},
	}

	for _, c := range cases {
		s := c.r.String()
		if s != c.s {
			t.Errorf("%#v (%T) got %s want %s", c.r, c.r, s, c.s)
		}
	}
}

func TestConvertValue(t *testing.T) {
	boolColType := types.ColumnType{Type: types.BoolType, NotNull: true}
	nullBoolColType := types.ColumnType{Type: types.BoolType}
	textColType := types.ColumnType{Type: types.StringType, NotNull: true,
		Size: types.MaxColumnSize}
	nullTextColType := types.ColumnType{Type: types.StringType, Size: types.MaxColumnSize}
	charColType := types.ColumnType{Type: types.StringType, Size: 8, Fixed: true}
	varCharColType := types.ColumnType{Type: types.StringType, Size: 8}
	bytesColType := types.ColumnType{Type: types.BytesType, NotNull: true,
		Size: types.MaxColumnSize}
	nullBytesColType := types.ColumnType{Type: types.BytesType, Size: types.MaxColumnSize}
	binaryColType := types.ColumnType{Type: types.BytesType, Size: 4, Fixed: true}
	varBinaryColType := types.ColumnType{Type: types.BytesType, Size: 4}
	floatColType := types.ColumnType{Type: types.Float64Type, NotNull: true}
	nullFloatColType := types.ColumnType{Type: types.Float64Type}
	int64ColType := types.ColumnType{Type: types.Int64Type, NotNull: true, Size: 8}
	nullInt64ColType := types.ColumnType{Type: types.Int64Type, Size: 8}
	int32ColType := types.ColumnType{Type: types.Int64Type, NotNull: true, Size: 4}
	int16ColType := types.ColumnType{Type: types.Int64Type, NotNull: true, Size: 2}

	cases := []struct {
		val  types.Value
		ct   types.ColumnType
		s    string
		fail bool
	}{
		{types.BoolValue(true), boolColType, "true", false},
		{types.BoolValue(false), nullBoolColType, "false", false},
		{nil, boolColType, "", true},
		{nil, nullBoolColType, "NULL", false},
		{types.StringValue("123"), boolColType, "", true},
		{types.StringValue("123"), nullBoolColType, "", true},
		{types.StringValue("this"), boolColType, "true", true},
		{types.StringValue(" t"), boolColType, "true", false},
		{types.StringValue("\ttrue\n"), boolColType, "true", false},
		{types.StringValue("y"), boolColType, "true", false},
		{types.StringValue("yes"), boolColType, "true", false},
		{types.StringValue("on"), boolColType, "true", false},
		{types.StringValue("1"), boolColType, "true", false},
		{types.StringValue("f"), nullBoolColType, "false", false},
		{types.StringValue("false"), nullBoolColType, "false", false},
		{types.StringValue("n"), nullBoolColType, "false", false},
		{types.StringValue("no"), nullBoolColType, "false", false},
		{types.StringValue("off"), nullBoolColType, "false", false},
		{types.StringValue("0"), nullBoolColType, "false", false},
		{types.BytesValue([]byte{123}), boolColType, "", true},
		{types.Float64Value(123.456), boolColType, "", true},
		{types.Int64Value(123), boolColType, "", true},
		{types.StringValue("abcdef"), textColType, "'abcdef'", false},
		{types.StringValue("abcdef"), nullTextColType, "'abcdef'", false},
		{nil, textColType, "", true},
		{nil, nullTextColType, "NULL", false},
		{types.BoolValue(true), textColType, "", true},
		{types.BytesValue([]byte("abcdef")), textColType, "'abcdef'", false},
		{types.BytesValue([]byte{0xFF, 0xFF, 0xFF}), textColType, "", true},
		{types.Float64Value(123.456), textColType, "'123.456'", false},
		{types.Int64Value(123456), textColType, "'123456'", false},
		{types.StringValue("12345678"), charColType, "'12345678'", false},
		{types.StringValue("abcdefgh"), varCharColType, "'abcdefgh'", false},
		{types.StringValue("123456789"), charColType, "", true},
		{types.StringValue("abcdefghi"), varCharColType, "", true},
		{types.BytesValue([]byte{1, 2, 3, 4}), bytesColType, `'\x01020304'`, false},
		{types.BytesValue([]byte{1, 2, 3, 4}), nullBytesColType, `'\x01020304'`, false},
		{nil, bytesColType, "", true},
		{nil, nullBytesColType, "NULL", false},
		{types.BoolValue(true), bytesColType, "", true},
		{types.StringValue("abc"), bytesColType, `'\x616263'`, false},
		{types.Float64Value(123.456), bytesColType, "", true},
		{types.Int64Value(123456), bytesColType, "", true},
		{types.BytesValue([]byte{1, 2, 3, 4}), binaryColType, `'\x01020304'`, false},
		{types.BytesValue([]byte{1, 2, 3, 4}), varBinaryColType, `'\x01020304'`, false},
		{types.BytesValue([]byte{1, 2, 3, 4, 5}), binaryColType, "", true},
		{types.BytesValue([]byte{1, 2, 3, 4, 5}), varBinaryColType, "", true},
		{types.Float64Value(123.456), floatColType, "123.456", false},
		{types.Float64Value(123.456), nullFloatColType, "123.456", false},
		{nil, floatColType, "", true},
		{nil, nullFloatColType, "NULL", false},
		{types.BoolValue(true), floatColType, "", true},
		{types.StringValue("123.456"), floatColType, "123.456", false},
		{types.StringValue("abc"), floatColType, "", true},
		{types.BytesValue([]byte{1, 2, 3}), nullFloatColType, "", true},
		{types.Int64Value(123), floatColType, "123", false},
		{types.Int64Value(123), int64ColType, "123", false},
		{types.Int64Value(-123), nullInt64ColType, "-123", false},
		{nil, int64ColType, "", true},
		{nil, nullInt64ColType, "NULL", false},
		{types.BoolValue(true), int64ColType, "", true},
		{types.StringValue("123"), int64ColType, "123", false},
		{types.StringValue("abc"), int64ColType, "", true},
		{types.BytesValue([]byte{1, 2, 3}), nullInt64ColType, "", true},
		{types.Float64Value(123.), int64ColType, "123", false},
		{types.Float64Value(123.4), int64ColType, "123", false},
		{types.Int64Value(math.MaxInt32), int32ColType, "2147483647", false},
		{types.Int64Value(math.MinInt32), int32ColType, "-2147483648", false},
		{types.Int64Value(math.MaxInt32 + 1), int32ColType, "", true},
		{types.Int64Value(math.MinInt32 - 1), int32ColType, "", true},
		{types.Int64Value(math.MaxInt16), int16ColType, "32767", false},
		{types.Int64Value(math.MinInt16), int16ColType, "-32768", false},
		{types.Int64Value(math.MaxInt16 + 1), int16ColType, "", true},
		{types.Int64Value(math.MinInt16 - 1), int16ColType, "", true},
	}

	for _, c := range cases {
		val, err := types.ConvertValue(c.ct, c.val)
		if err != nil {
			if !c.fail {
				t.Errorf("ConvertValue(%s, %s) failed with %s", c.ct, c.val, err)
			}
		} else if c.fail {
			t.Errorf("ConvertValue(%s, %s) did not fail", c.ct, c.val)
		} else if s := types.FormatValue(val); s != c.s {
			t.Errorf("ConvertValue(%s, %s) got %s want %s", c.ct, c.val, s, c.s)
		}
	}
}

func TestConvertRow(t *testing.T) {
	colTypes := []types.ColumnType{
		{Type: types.BoolType, NotNull: true},
		{Type: types.StringType, Size: types.MaxColumnSize},
		{Type: types.Int64Type, Size: 8},
	}

	cases := []struct {
		r    types.Row
		s    string
		fail bool
	}{
		{
			r: types.Row{types.BoolValue(true), types.StringValue("abc"), types.Int64Value(123)},
			s: "[true, 'abc', 123]",
		},
		{
			r: types.Row{types.StringValue("t"), types.StringValue("abc"), types.Int64Value(123)},
			s: "[true, 'abc', 123]",
		},
		{
			r: types.Row{types.BoolValue(true), types.StringValue("abc"),
				types.StringValue("123")},
			s: "[true, 'abc', 123]",
		},
		{
			r: types.Row{types.BoolValue(true), types.Int64Value(123), types.Int64Value(123)},
			s: "[true, '123', 123]",
		},
		{
			r: types.Row{types.StringValue("t"), types.BytesValue([]byte("abc")),
				types.StringValue("123")},
			s: "[true, 'abc', 123]",
		},
		{
			r:    types.Row{nil, types.BytesValue([]byte("abc")), types.StringValue("123")},
			s:    "[NULL, 'abc', 123]",
			fail: true,
		},
		{
			r: types.Row{types.StringValue("t"), nil, types.StringValue("123")},
			s: "[true, NULL, 123]",
		},
	}

	for _, c := range cases {
		r, err := types.ConvertRow(colTypes, c.r)
		if err != nil {
			if !c.fail {
				t.Errorf("ConvertRow(%s) failed with %s", c.r, err)
			}
		} else if c.fail {
			t.Errorf("ConvertRow(%s) did not fail", c.r)
		} else if s := r.String(); s != c.s {
			t.Errorf("%s.String() got %s want %s", c.r, s, c.s)
		}
	}
}
