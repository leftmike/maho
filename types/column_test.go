package types_test

import (
	"testing"

	"github.com/leftmike/maho/types"
)

func columnTypeString(ct types.ColumnType) (s string, panicked bool) {
	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(string); ok {
				panicked = true
			} else {
				panic(r)
			}
		}
	}()

	return ct.String(), false
}

func TestColumnType(t *testing.T) {
	cases := []struct {
		ct types.ColumnType
		s  string
		p  bool
	}{
		{types.ColumnType{}, "UNKNOWN", false},
		{types.ColumnType{Type: types.BoolType}, "BOOL", false},
		{types.ColumnType{Type: types.BoolType, Size: 10}, "BOOL", false},
		{types.ColumnType{Type: types.BoolType, Fixed: true}, "BOOL", false},
		{types.ColumnType{Type: types.StringType}, "", true},
		{types.ColumnType{Type: types.StringType, Size: 123}, "VARCHAR(123)", false},
		{types.ColumnType{Type: types.StringType, Size: 45, Fixed: true}, "CHAR(45)", false},
		{types.ColumnType{Type: types.StringType, Size: types.MaxColumnSize}, "TEXT", false},
		{types.ColumnType{Type: types.BytesType}, "", true},
		{types.ColumnType{Type: types.BytesType, Size: 123}, "VARBINARY(123)", false},
		{types.ColumnType{Type: types.BytesType, Size: 45, Fixed: true}, "BINARY(45)", false},
		{types.ColumnType{Type: types.BytesType, Size: types.MaxColumnSize}, "BYTES", false},
		{types.ColumnType{Type: types.Float64Type}, "DOUBLE", false},
		{types.ColumnType{Type: types.Float64Type, Size: 10}, "DOUBLE", false},
		{types.ColumnType{Type: types.Float64Type, Fixed: true}, "DOUBLE", false},
		{types.ColumnType{Type: types.Int64Type, Size: 2}, "SMALLINT", false},
		{types.ColumnType{Type: types.Int64Type, Size: 4}, "INT", false},
		{types.ColumnType{Type: types.Int64Type, Size: 8}, "BIGINT", false},
		{types.ColumnType{Type: types.Int64Type, Size: 2, Fixed: true}, "SMALLINT", false},
		{types.ColumnType{Type: types.Int64Type, Size: 1}, "", true},
		{types.ColumnType{Type: 123}, "", true},
	}

	for _, c := range cases {
		s, p := columnTypeString(c.ct)
		if s != c.s {
			t.Errorf("%#v.String() got %s want %s", c.ct, s, c.s)
		}
		if p != c.p {
			if c.p {
				t.Errorf("%#v.String() did not panic", c.ct)
			} else {
				t.Errorf("%#v.String() panicked", c.ct)
			}
		}
	}
}
