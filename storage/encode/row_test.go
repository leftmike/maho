package encode_test

import (
	"reflect"
	"testing"

	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/storage/encode"
)

func TestRowValues(t *testing.T) {
	cases := []struct {
		row []sql.Value
		s   string
	}{
		{
			row: []sql.Value{sql.BoolValue(true)},
			s:   "true",
		},
		{
			row: []sql.Value{sql.Int64Value(345)},
			s:   "345",
		},
		{
			row: []sql.Value{sql.Float64Value(987.6543)},
			s:   "987.6543",
		},
		{
			row: []sql.Value{sql.StringValue("abcdefghijklmnopqrstuvwxyz")},
			s:   "'abcdefghijklmnopqrstuvwxyz'",
		},
		{
			row: []sql.Value{sql.BoolValue(true), sql.Int64Value(345), sql.Float64Value(987.6543),
				sql.StringValue("abcdefghijklmnopqrstuvwxyz")},
			s: "true, 345, 987.6543, 'abcdefghijklmnopqrstuvwxyz'",
		},
		{
			row: []sql.Value{sql.BoolValue(true), nil, sql.Int64Value(345)},
			s:   "true, NULL, 345",
		},
		{
			row: []sql.Value{nil, nil, nil, sql.StringValue("ABCDEFG")},
			s:   "NULL, NULL, NULL, 'ABCDEFG'",
		},
		{
			row: []sql.Value{sql.Int64Value(1234567890), sql.StringValue(""), sql.BoolValue(true)},
			s:   "1234567890, '', true",
		},
		{
			row: []sql.Value{sql.Int64Value(123), sql.StringValue(""),
				sql.BytesValue{0xFF, 1, 2, 3}},
			s: `123, '', '\xff010203'`,
		},
		{
			row: []sql.Value{
				sql.StringValue("19064"),
				sql.StringValue("303"),
				sql.Float64Value(0.8),
				sql.Int64Value(1),
				nil,
				sql.Int64Value(1),
				sql.StringValue("A"),
				sql.StringValue("     "),
				sql.StringValue(" "),
				nil,
				nil,
				nil,
				nil,
				nil,
				nil,
				sql.StringValue(""),
				sql.StringValue("D"),
			},
			s: `'19064', '303', 0.8, 1, NULL, 1, 'A', '     ', ' ', NULL, NULL, NULL, NULL, NULL, NULL, '', 'D'`,
		},
	}

	for _, c := range cases {
		buf := encode.EncodeRowValue(c.row)
		dest := encode.DecodeRowValue(buf)
		if dest == nil {
			t.Errorf("DecodeRowValue(%s) failed", c.s)
		} else if !reflect.DeepEqual(c.row, dest) {
			t.Errorf("DecodeRowValue(%s) got %v want %v", c.s, dest, c.row)
		}

		var s string
		for num, val := range dest {
			if num > 0 {
				s += ", "
			}
			s += sql.Format(val)
		}
		if s != c.s {
			t.Errorf("DecodeRowValue: got %s want %s", s, c.s)
		}
	}
}
