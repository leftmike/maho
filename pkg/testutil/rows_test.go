package testutil_test

import (
	"testing"

	"github.com/leftmike/maho/pkg/testutil"
	"github.com/leftmike/maho/pkg/types"
)

func TestFormatRows(t *testing.T) {
	rows := []types.Row{
		{types.StringValue("abc"), nil, types.BoolValue(true)},
		{types.Float64Value(12.45), types.Int64Value(678), types.BoolValue(false)},
		{types.Int64Value(1), types.Int64Value(23), types.Int64Value(456)},
	}

	cases := []struct {
		sep string
		s   string
	}{
		{"", `('abc', NULL, true)(12.45, 678, false)(1, 23, 456)`},
		{"\n", `('abc', NULL, true)
(12.45, 678, false)
(1, 23, 456)`},
		{"---", `('abc', NULL, true)---(12.45, 678, false)---(1, 23, 456)`},
	}

	for _, c := range cases {
		s := testutil.FormatRows(rows, c.sep)
		if s != c.s {
			t.Errorf("FormatRows() got %s want %s", s, c.s)
		}
	}
}

func TestRowsEqual(t *testing.T) {
	cases := []struct {
		s1, s2    string
		unordered bool
		eq        bool
	}{
		{
			s1:        `('abc', NULL, true), (12.45, 678, false), (1, 23, 456)`,
			s2:        `('abc', NULL, true), (12.45, 678, false), (1, 23, 456)`,
			unordered: false,
			eq:        true,
		},
		{
			s1:        `('abc', NULL, true), (12.45, 678, false), (1, 23, 456)`,
			s2:        `(12.45, 678, false), (1, 23, 456), ('abc', NULL, true)`,
			unordered: false,
			eq:        false,
		},
		{
			s1:        `('abc', NULL, true), (12.45, 678, false), (1, 23, 456)`,
			s2:        `(12.45, 678, false), (1, 23, 456), ('abc', NULL, true)`,
			unordered: true,
			eq:        true,
		},
	}

	for _, c := range cases {
		rows1 := testutil.MustParseRows(c.s1)
		rows2 := testutil.MustParseRows(c.s2)
		eq := testutil.RowsEqual(rows1, rows2, c.unordered)
		if eq != c.eq {
			t.Errorf("RowsEqual(%s, %s, %v) got %v want %v", c.s1, c.s2, c.unordered, eq, c.eq)
		}
	}
}
