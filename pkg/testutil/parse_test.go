package test_test

import (
	"strings"
	"testing"

	"github.com/leftmike/maho/pkg/storage/test"
	"github.com/leftmike/maho/pkg/types"
)

func TestParseValue(t *testing.T) {
	cases := []struct {
		s    string
		val  types.Value
		fail bool
	}{
		{s: "'abc'", val: types.StringValue("abc")},
		{s: " \t\n 'abc'  ", val: types.StringValue("abc")},
		{s: "'", fail: true},
		{s: "' ", fail: true},
		{s: "123", val: types.Int64Value(123)},
		{s: " 123 ", val: types.Int64Value(123)},
		{s: "123.456", val: types.Float64Value(123.456)},
		{s: "123..456", fail: true},
		{s: ".", fail: true},
		{s: ".123", val: types.Float64Value(0.123)},
		{s: "true", val: types.BoolValue(true)},
		{s: "  False  ", val: types.BoolValue(false)},
		{s: "t", fail: true},
		{s: "null", val: nil},
		{s: `'\a'`, fail: true},
		{s: `'\x1234`, fail: true},
		{s: `'\x123'`, fail: true},
		{
			s:   `'\x1234567890abcdef'`,
			val: types.BytesValue([]byte{0x12, 0x34, 0x56, 0x78, 0x90, 0xAB, 0xcd, 0xEf}),
		},
	}

	for _, c := range cases {
		val, err := test.ParseValue(strings.NewReader(c.s))
		if err != nil {
			if !c.fail {
				t.Errorf("ParseValue(%s) failed with %s", c.s, err)
			}
		} else if c.fail {
			t.Errorf("ParseValue(%s) did not fail", c.s)
		} else if types.Compare(val, c.val) != 0 {
			t.Errorf("ParseValue(%s) got %s want %s", c.s, val, c.val)
		}
	}
}

func TestParseRow(t *testing.T) {
	cases := []struct {
		s    string
		r    string
		fail bool
	}{
		{s: `(123, 'abc', true, 456.789, '\x010203')`},
		{s: "(123 true)", fail: true},
		{s: "123, true)", fail: true},
		{s: "(123, true", fail: true},
		{s: "()", fail: true},
		{
			s: " (    123,456 ,'abc'  ,   'def'    )   ",
			r: "(123, 456, 'abc', 'def')",
		},
	}

	for _, c := range cases {
		row, err := test.ParseRow(strings.NewReader(c.s))
		if err != nil {
			if !c.fail {
				t.Errorf("ParseRow(%s) failed with %s", c.s, err)
			}
		} else if c.fail {
			t.Errorf("ParseRow(%s) did not fail", c.s)
		} else {
			r := row.String()
			if c.r == "" && r != c.s {
				t.Errorf("ParseRow(%s) got %s want %s", c.s, r, c.s)
			} else if c.r != "" && r != c.r {
				t.Errorf("ParseRow(%s) got %s want %s", c.s, r, c.r)
			}
		}
	}
}
