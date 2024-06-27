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
		vt       types.ValueType
		s        string
		panicked bool
	}{
		{vt: types.BoolType, s: "BOOL"},
		{vt: types.StringType, s: "STRING"},
		{vt: types.BytesType, s: "BYTES"},
		{vt: types.Float64Type, s: "DOUBLE"},
		{vt: types.Int64Type, s: "INT"},
		{vt: types.UnknownType, s: "UNKNOWN"},
		{vt: types.ValueType(-1), panicked: true},
	}

	for _, c := range cases {
		s, panicked := valueTypeString(c.vt)
		if s != c.s {
			t.Errorf("%#v.String() got %s want %s", c.vt, s, c.s)
		}
		if panicked != c.panicked {
			if c.panicked {
				t.Errorf("%#v.String() did not panic", c.vt)
			} else {
				t.Errorf("%#v.String() panicked", c.vt)
			}
		}
	}
}

func TestCompare(t *testing.T) {
	cases := []struct {
		v1, v2 types.Value
		cmp    int
	}{
		{nil, types.BoolValue(true), -1},
		{nil, nil, 0},

		{types.BoolValue(false), nil, 1},
		{types.BoolValue(true), types.BoolValue(true), 0},
		{types.BoolValue(false), types.BoolValue(false), 0},
		{types.BoolValue(false), types.BoolValue(true), -1},
		{types.BoolValue(true), types.BoolValue(false), 1},
		{types.BoolValue(false), types.Float64Value(1.23), -1},

		{types.Float64Value(1.23), types.BoolValue(false), 1},
		{types.Float64Value(1.23), types.Int64Value(123), -1},
		{types.Float64Value(1.23), types.StringValue("abc"), -1},
		{types.Float64Value(1.23), types.Float64Value(2.34), -1},
		{types.Float64Value(1.23), types.Float64Value(1.23), 0},
		{types.Float64Value(1.23), types.Float64Value(0.12), 1},

		{types.Int64Value(123), types.BoolValue(false), 1},
		{types.Int64Value(123), types.Float64Value(1.23), 1},
		{types.Int64Value(123), types.StringValue("abc"), -1},
		{types.Int64Value(123), types.Int64Value(234), -1},
		{types.Int64Value(123), types.Int64Value(123), 0},
		{types.Int64Value(123), types.Int64Value(12), 1},

		{types.StringValue("abc"), types.BoolValue(false), 1},
		{types.StringValue("abc"), types.Float64Value(1.23), 1},
		{types.StringValue("abc"), types.Int64Value(123), 1},
		{types.StringValue("def"), types.StringValue("ghi"), -1},
		{types.StringValue("def"), types.StringValue("def"), 0},
		{types.StringValue("def"), types.StringValue("abc"), 1},
	}

	for _, c := range cases {
		cmp := types.Compare(c.v1, c.v2)
		if cmp != c.cmp {
			t.Errorf("Compare(%v, %v) got %d want %d", c.v1, c.v2, cmp, c.cmp)
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
			s: "(true, 123, 0)",
		},
		{types.Row{}, "()"},
		{nil, "()"},
		{types.Row{types.BoolValue(false)}, "(false)"},
		{
			r: types.Row{
				types.Int64Value(0xFF),
				types.Int64Value(-123),
				types.Float64Value(1.234),
				types.StringValue("abcdef"),
			},
			s: "(255, -123, 1.234, 'abcdef')",
		},
		{
			r: types.Row{types.StringValue(""),
				types.BytesValue(bytes.Repeat([]byte{0x12, 0xef}, 3)),
				types.BytesValue([]byte{}),
			},
			s: `('', '\x12ef12ef12ef', '\x')`,
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
		{val: types.BoolValue(true), ct: boolColType, s: "true"},
		{val: types.BoolValue(false), ct: nullBoolColType, s: "false"},
		{val: nil, ct: boolColType, fail: true},
		{val: nil, ct: nullBoolColType, s: "NULL"},
		{val: types.StringValue("123"), ct: boolColType, fail: true},
		{val: types.StringValue("123"), ct: nullBoolColType, fail: true},
		{val: types.StringValue("this"), ct: boolColType, fail: true},
		{val: types.StringValue(" t"), ct: boolColType, s: "true"},
		{val: types.StringValue("\ttrue\n"), ct: boolColType, s: "true"},
		{val: types.StringValue("y"), ct: boolColType, s: "true"},
		{val: types.StringValue("yes"), ct: boolColType, s: "true"},
		{val: types.StringValue("on"), ct: boolColType, s: "true"},
		{val: types.StringValue("1"), ct: boolColType, s: "true"},
		{val: types.StringValue("f"), ct: nullBoolColType, s: "false"},
		{val: types.StringValue("false"), ct: nullBoolColType, s: "false"},
		{val: types.StringValue("n"), ct: nullBoolColType, s: "false"},
		{val: types.StringValue("no"), ct: nullBoolColType, s: "false"},
		{val: types.StringValue("off"), ct: nullBoolColType, s: "false"},
		{val: types.StringValue("0"), ct: nullBoolColType, s: "false"},
		{val: types.BytesValue([]byte{123}), ct: boolColType, fail: true},
		{val: types.Float64Value(123.456), ct: boolColType, fail: true},
		{val: types.Int64Value(123), ct: boolColType, fail: true},
		{val: types.StringValue("abcdef"), ct: textColType, s: "'abcdef'"},
		{val: types.StringValue("abcdef"), ct: nullTextColType, s: "'abcdef'"},
		{val: nil, ct: textColType, fail: true},
		{val: nil, ct: nullTextColType, s: "NULL"},
		{val: types.BoolValue(true), ct: textColType, fail: true},
		{val: types.BytesValue([]byte("abcdef")), ct: textColType, s: "'abcdef'"},
		{val: types.BytesValue([]byte{0xFF, 0xFF, 0xFF}), ct: textColType, fail: true},
		{val: types.Float64Value(123.456), ct: textColType, s: "'123.456'"},
		{val: types.Int64Value(123456), ct: textColType, s: "'123456'"},
		{val: types.StringValue("12345678"), ct: charColType, s: "'12345678'"},
		{val: types.StringValue("abcdefgh"), ct: varCharColType, s: "'abcdefgh'"},
		{val: types.StringValue("123456789"), ct: charColType, fail: true},
		{val: types.StringValue("abcdefghi"), ct: varCharColType, fail: true},
		{val: types.BytesValue([]byte{1, 2, 3, 4}), ct: bytesColType, s: `'\x01020304'`},
		{val: types.BytesValue([]byte{1, 2, 3, 4}), ct: nullBytesColType, s: `'\x01020304'`},
		{val: nil, ct: bytesColType, fail: true},
		{val: nil, ct: nullBytesColType, s: "NULL"},
		{val: types.BoolValue(true), ct: bytesColType, fail: true},
		{val: types.StringValue("abc"), ct: bytesColType, s: `'\x616263'`},
		{val: types.Float64Value(123.456), ct: bytesColType, fail: true},
		{val: types.Int64Value(123456), ct: bytesColType, fail: true},
		{val: types.BytesValue([]byte{1, 2, 3, 4}), ct: binaryColType, s: `'\x01020304'`},
		{val: types.BytesValue([]byte{1, 2, 3, 4}), ct: varBinaryColType, s: `'\x01020304'`},
		{val: types.BytesValue([]byte{1, 2, 3, 4, 5}), ct: binaryColType, fail: true},
		{val: types.BytesValue([]byte{1, 2, 3, 4, 5}), ct: varBinaryColType, fail: true},
		{val: types.Float64Value(123.456), ct: floatColType, s: "123.456"},
		{val: types.Float64Value(123.456), ct: nullFloatColType, s: "123.456"},
		{val: nil, ct: floatColType, fail: true},
		{val: nil, ct: nullFloatColType, s: "NULL"},
		{val: types.BoolValue(true), ct: floatColType, fail: true},
		{val: types.StringValue("123.456"), ct: floatColType, s: "123.456"},
		{val: types.StringValue("abc"), ct: floatColType, fail: true},
		{val: types.BytesValue([]byte{1, 2, 3}), ct: nullFloatColType, fail: true},
		{val: types.Int64Value(123), ct: floatColType, s: "123"},
		{val: types.Int64Value(123), ct: int64ColType, s: "123"},
		{val: types.Int64Value(-123), ct: nullInt64ColType, s: "-123"},
		{val: nil, ct: int64ColType, fail: true},
		{val: nil, ct: nullInt64ColType, s: "NULL"},
		{val: types.BoolValue(true), ct: int64ColType, fail: true},
		{val: types.StringValue("123"), ct: int64ColType, s: "123"},
		{val: types.StringValue("abc"), ct: int64ColType, fail: true},
		{val: types.BytesValue([]byte{1, 2, 3}), ct: nullInt64ColType, fail: true},
		{val: types.Float64Value(123.), ct: int64ColType, s: "123"},
		{val: types.Float64Value(123.4), ct: int64ColType, s: "123"},
		{val: types.Int64Value(math.MaxInt32), ct: int32ColType, s: "2147483647"},
		{val: types.Int64Value(math.MinInt32), ct: int32ColType, s: "-2147483648"},
		{val: types.Int64Value(math.MaxInt32 + 1), ct: int32ColType, fail: true},
		{val: types.Int64Value(math.MinInt32 - 1), ct: int32ColType, fail: true},
		{val: types.Int64Value(math.MaxInt16), ct: int16ColType, s: "32767"},
		{val: types.Int64Value(math.MinInt16), ct: int16ColType, s: "-32768"},
		{val: types.Int64Value(math.MaxInt16 + 1), ct: int16ColType, fail: true},
		{val: types.Int64Value(math.MinInt16 - 1), ct: int16ColType, fail: true},
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
		{Type: types.StringType, Size: types.MaxColumnSize, NotNull: true},
		{Type: types.Int64Type, Size: 8},
	}

	cases := []struct {
		r    types.Row
		s    string
		fail bool
	}{
		{
			r: types.Row{types.BoolValue(true), types.StringValue("abc"), types.Int64Value(123)},
			s: "(true, 'abc', 123)",
		},
		{
			r: types.Row{types.StringValue("t"), types.StringValue("abc"), types.Int64Value(123)},
			s: "(true, 'abc', 123)",
		},
		{
			r: types.Row{types.BoolValue(true), types.StringValue("abc"),
				types.StringValue("123")},
			s: "(true, 'abc', 123)",
		},
		{
			r: types.Row{types.BoolValue(true), types.Int64Value(123), types.Int64Value(123)},
			s: "(true, '123', 123)",
		},
		{
			r: types.Row{types.StringValue("t"), types.BytesValue([]byte("abc")),
				types.StringValue("123")},
			s: "(true, 'abc', 123)",
		},
		{
			r:    types.Row{nil, types.BytesValue([]byte("abc")), types.StringValue("123")},
			fail: true,
		},
		{
			r: types.Row{types.StringValue("t"), types.StringValue(""), types.StringValue("123")},
			s: "(true, '', 123)",
		},
		{
			r: types.Row{types.BoolValue(true), types.StringValue("abc"), types.Int64Value(123),
				types.Int64Value(456)},
			fail: true,
		},
		{
			r: types.Row{types.StringValue("t"), types.StringValue("abc")},
			s: "(true, 'abc', NULL)",
		},
		{
			r: types.Row{types.StringValue("t")},
			s: "(true, NULL, NULL)",
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
