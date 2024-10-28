package testutil_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/leftmike/maho/testutil"
	"github.com/leftmike/maho/types"
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
		val, err := testutil.ParseValue(strings.NewReader(c.s))
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
		r, err := testutil.ParseRow(strings.NewReader(c.s))
		if err != nil {
			if !c.fail {
				t.Errorf("ParseRow(%s) failed with %s", c.s, err)
			}
		} else if c.fail {
			t.Errorf("ParseRow(%s) did not fail", c.s)
		} else {
			s := r.String()
			if c.r == "" && s != c.s {
				t.Errorf("ParseRow(%s) got %s want %s", c.s, s, c.s)
			} else if c.r != "" && s != c.r {
				t.Errorf("ParseRow(%s) got %s want %s", c.s, s, c.r)
			}
		}
	}
}

func TestParseRows(t *testing.T) {
	cases := []struct {
		s    string
		r    string
		fail bool
	}{
		{s: `(123, 'abc', true, 456.789, '\x010203')`},
		{s: "(12, 345), (123 true)", fail: true},
		{s: "(true, false), 123, true)", fail: true},
		{s: "('abc', 'def'), (123, true", fail: true},
		{s: "(12, 345), ()", fail: true},
		{
			s: " (    123,456 ,'abc'  ,   'def'    )   ",
			r: "(123, 456, 'abc', 'def')",
		},
		{
			s: "(12,34,'abc')   ,('def',567,89),    (true,true,false),(null,null,null)",
			r: "(12, 34, 'abc'), ('def', 567, 89), (true, true, false), (NULL, NULL, NULL)",
		},
	}

	for _, c := range cases {
		rows, err := testutil.ParseRows(strings.NewReader(c.s))
		if err != nil {
			if !c.fail {
				t.Errorf("ParseRows(%s) failed with %s", c.s, err)
			}
		} else if c.fail {
			t.Errorf("ParseRows(%s) did not fail", c.s)
		} else {
			s := testutil.FormatRows(rows, ", ")
			if c.r == "" && s != c.s {
				t.Errorf("ParseRows(%s) got %s want %s", c.s, s, c.s)
			} else if c.r != "" && s != c.r {
				t.Errorf("ParseRows(%s) got %s want %s", c.s, s, c.r)
			}
		}
	}
}

func TestParseIdentifier(t *testing.T) {
	cases := []struct {
		s    string
		id   types.Identifier
		fail bool
	}{
		{s: "id1", id: types.ID("id1", false)},
		{s: "+id1", fail: true},
		{s: "  id2  ", id: types.ID("id2", false)},
		{s: "  id3,  ", id: types.ID("id3", false)},
	}

	for _, c := range cases {
		id, err := testutil.ParseIdentifier(strings.NewReader(c.s))
		if err != nil {
			if !c.fail {
				t.Errorf("ParseIdentifier(%s) failed with %s", c.s, err)
			}
		} else if c.fail {
			t.Errorf("ParseIdentifier(%s) did not fail", c.s)
		} else if id != c.id {
			t.Errorf("ParseIdentifier(%s) got %s want %s", c.s, id, c.id)
		}
	}
}

func TestParseIdentifiers(t *testing.T) {
	cases := []struct {
		s    string
		ids  []types.Identifier
		fail bool
	}{
		{s: "id1", ids: []types.Identifier{types.ID("id1", false)}},
		{s: "id1 id2", fail: true},
		{s: "id1, id2", ids: []types.Identifier{types.ID("id1", false), types.ID("id2", false)}},
		{s: " id1,id2  ", ids: []types.Identifier{types.ID("id1", false), types.ID("id2", false)}},
		{s: "id1, id2,   ", fail: true},
		{s: "", fail: true},
		{s: "  ", fail: true},
		{s: "int, char, bytes", ids: []types.Identifier{types.INT, types.CHAR, types.BYTES}},
	}

	for _, c := range cases {
		ids, err := testutil.ParseIdentifiers(strings.NewReader(c.s))
		if err != nil {
			if !c.fail {
				t.Errorf("ParseIdentifiers(%s) failed with %s", c.s, err)
			}
		} else if c.fail {
			t.Errorf("ParseIdentifiers(%s) did not fail", c.s)
		} else if !reflect.DeepEqual(ids, c.ids) {
			t.Errorf("ParseIdentifiers(%s) got %v want %v", c.s, ids, c.ids)
		}
	}
}

func TestParseColumns(t *testing.T) {
	cases := []struct {
		s        string
		cols     []types.Identifier
		colTypes []types.ColumnType
		key      []types.ColumnKey
		fail     bool
	}{
		{
			s:        "col1 int",
			cols:     []types.Identifier{types.ID("col1", false)},
			colTypes: []types.ColumnType{{Type: types.Int64Type, Size: 4}},
		},
		{s: "col1 bad", fail: true},
		{
			s:    "col1 int, col2 bool",
			cols: []types.Identifier{types.ID("col1", false), types.ID("col2", false)},
			colTypes: []types.ColumnType{
				{Type: types.Int64Type, Size: 4},
				{Type: types.BoolType, Size: 1},
			},
		},
		{
			s: "c1 varbinary ( 123 ), c2 varchar(45), c3 text(678)",
			cols: []types.Identifier{types.ID("c1", false), types.ID("c2", false),
				types.ID("c3", false)},
			colTypes: []types.ColumnType{
				{Type: types.BytesType, Size: 123},
				{Type: types.StringType, Size: 45},
				{Type: types.StringType, Size: 678},
			},
		},
		{s: "c1 binary 123)", fail: true},
		{s: "c1 binary(123), c2 binary(456", fail: true},
		{s: "c1 text()", fail: true},
		{
			s: "c1 int, c2 bool not null, c3 char primary key",
			cols: []types.Identifier{types.ID("c1", false), types.ID("c2", false),
				types.ID("c3", false)},
			colTypes: []types.ColumnType{
				{Type: types.Int64Type, Size: 4},
				{Type: types.BoolType, Size: 1, NotNull: true},
				{Type: types.StringType, Fixed: true, Size: 1},
			},
			key: []types.ColumnKey{types.MakeColumnKey(2, false)},
		},
		{
			s: "c1 int not null primary key, c2 bool, c3 char primary key",
			cols: []types.Identifier{types.ID("c1", false), types.ID("c2", false),
				types.ID("c3", false)},
			colTypes: []types.ColumnType{
				{Type: types.Int64Type, Size: 4, NotNull: true},
				{Type: types.BoolType, Size: 1},
				{Type: types.StringType, Fixed: true, Size: 1},
			},
			key: []types.ColumnKey{types.MakeColumnKey(0, false), types.MakeColumnKey(2, false)},
		},
		{s: "c1 int not, c2 bool", fail: true},
		{s: "c1 int, c2 bool primary", fail: true},
		{s: "c1 int null, c2 bool", fail: true},
		{s: "c1 int, c2 bool 123", fail: true},
	}

	for _, c := range cases {
		cols, colTypes, key, err := testutil.ParseColumns(strings.NewReader(c.s))
		if err != nil {
			if !c.fail {
				t.Errorf("ParseColumns(%s) failed with %s", c.s, err)
			}
		} else if c.fail {
			t.Errorf("ParseColumns(%s) did not fail", c.s)
		} else {
			if !reflect.DeepEqual(cols, c.cols) {
				t.Errorf("ParseColumns(%s) got %v want %v", c.s, cols, c.cols)
			}
			if !reflect.DeepEqual(colTypes, c.colTypes) {
				t.Errorf("ParseColumns(%s) got %v want %v", c.s, colTypes, c.colTypes)
			}
			if !reflect.DeepEqual(key, c.key) {
				t.Errorf("ParseColumns(%s) got %v want %v", c.s, key, c.key)
			}
		}
	}
}
