package engine_test

import (
	"reflect"
	"testing"

	"github.com/leftmike/maho/pkg/engine"
	"github.com/leftmike/maho/pkg/types"
)

func TestParsePrimary(t *testing.T) {
	cases := []struct {
		s    string
		key  []types.ColumnKey
		fail bool
	}{
		{s: "c1", key: []types.ColumnKey{types.MakeColumnKey(0, false)}},
		{s: "col1", fail: true},
		{
			s: "c1, c2",
			key: []types.ColumnKey{
				types.MakeColumnKey(0, false),
				types.MakeColumnKey(1, false),
			},
		},
		{
			s: "c4 ,c3,c2   ",
			key: []types.ColumnKey{
				types.MakeColumnKey(3, false),
				types.MakeColumnKey(2, false),
				types.MakeColumnKey(1, false),
			},
		},
		{s: "c1 c2", fail: true},
		{s: "c1, c2,", fail: true},
		{s: ", c1, c2", fail: true},
	}

	colNames := []types.Identifier{
		types.ID("c1", false),
		types.ID("c2", false),
		types.ID("c3", false),
		types.ID("c4", false),
	}

	for _, c := range cases {
		key, err := engine.ParsePrimary(c.s, colNames)
		if err != nil {
			if !c.fail {
				t.Errorf("ParsePrimary(%s) failed with %s", c.s, err)
			}
		} else if c.fail {
			t.Errorf("ParsePrimary(%s) did not fail", c.s)
		} else if !reflect.DeepEqual(key, c.key) {
			t.Errorf("ParsePrimary(%s) got %v want %v", c.s, key, c.key)
		}
	}
}

func TestParseColumn(t *testing.T) {
	cases := []struct {
		s       string
		ct      types.ColumnType
		primary bool
		fail    bool
	}{
		{s: "varchar(123)", ct: types.ColumnType{Type: types.StringType, Size: 123}},
		{s: "varchar(abc)", fail: true},
		{s: "char(123", fail: true},
		{s: "char", ct: types.ColumnType{Type: types.StringType, Fixed: true, Size: 1}},
		{s: "bytes ( 456 ) ", ct: types.ColumnType{Type: types.BytesType, Size: 456}},
		{s: "int not null", ct: types.ColumnType{Type: types.Int64Type, Size: 4, NotNull: true}},
		{s: "int not", fail: true},
		{s: "int not null not", fail: true},
		{
			s:       "int primary key",
			ct:      types.ColumnType{Type: types.Int64Type, Size: 4},
			primary: true,
		},
		{
			s:       "int not null primary key",
			ct:      types.ColumnType{Type: types.Int64Type, Size: 4, NotNull: true},
			primary: true,
		},
		{
			s:       "int primary key not null",
			ct:      types.ColumnType{Type: types.Int64Type, Size: 4, NotNull: true},
			primary: true,
		},
		{
			s:       "int primary key",
			ct:      types.ColumnType{Type: types.Int64Type, Size: 4},
			primary: true,
		},
		{s: "int primary", fail: true},
		{s: "int key", fail: true},
	}

	for _, c := range cases {
		ct, primary, err := engine.ParseColumn(c.s)
		_ = primary
		if err != nil {
			if !c.fail {
				t.Errorf("ParseColumn(%s) failed with %s", c.s, err)
			}
		} else if c.fail {
			t.Errorf("ParseColumn(%s) did not fail", c.s)
		} else {
			if ct != c.ct {
				t.Errorf("ParseColumn(%s) ct: got %v want %v", c.s, ct, c.ct)
			}
			if primary != c.primary {
				t.Errorf("ParseColumn(%s) primary: got %v want %v", c.s, primary, c.primary)
			}
		}
	}
}
