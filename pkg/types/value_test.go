package types_test

import (
	"bytes"
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
		{types.BooleanType, "BOOL", false},
		{types.StringType, "STRING", false},
		{types.BytesType, "BYTES", false},
		{types.FloatType, "DOUBLE", false},
		{types.IntegerType, "INT", false},
		{types.UnknownType, "UNKNOWN", false},
		{types.ValueType(-1), "", true},
	}

	for _, c := range cases {
		s, p := valueTypeString(c.vt)
		if s != c.s {
			t.Errorf("%#v (%T) got %s want %s", c.vt, c.vt, s, c.s)
		}
		if p != c.p {
			if c.p {
				t.Errorf("%#v (%T) did not panic", c.vt, c.vt)
			} else {
				t.Errorf("%#v (%T) panicked", c.vt, c.vt)
			}
		}
	}
}
