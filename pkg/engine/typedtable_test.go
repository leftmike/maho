package engine

import (
	"reflect"
	"testing"

	"github.com/leftmike/maho/pkg/storage"
	"github.com/leftmike/maho/pkg/types"
)

func typedInfoPanicked(fn func() *typedInfo) (ti *typedInfo, panicked bool) {
	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(string); ok {
				panicked = true
			} else {
				panic(r)
			}
		}
	}()

	return fn(), false
}

func TestMakeTypedTableInfo(t *testing.T) {
	tn := types.TableName{
		Database: types.ID("db", false),
		Schema:   types.ID("scm", false),
		Table:    types.ID("tbl", false),
	}

	cases := []struct {
		tid      storage.TableId
		tn       types.TableName
		row      interface{}
		ti       *typedInfo
		panicked bool
	}{
		{row: 123, panicked: true},
		{row: struct{}{}, ti: &typedInfo{}},
		{row: struct{ aBC int }{}, panicked: true},
		{
			row: struct {
				Abc int `maho:"notnull,name=ghi=jkl,primary"`
			}{},
			panicked: true,
		},
		{
			row: struct {
				Abc int `maho:"notnull=true"`
			}{},
			panicked: true,
		},
		{
			row: struct {
				Abc int `maho:"name"`
			}{},
			panicked: true,
		},
		{
			row: struct {
				Abc int `maho:"size=abc"`
			}{},
			panicked: true,
		},
		{
			row: struct {
				Abc []int16
			}{},
			panicked: true,
		},
		{
			row: struct {
				Abc [8]string
			}{},
			panicked: true,
		},
		{
			tid: maxReservedTableId + 1,
			tn:  tn,
			row: struct {
				ColNum   byte    `db:"name,primary=123"`
				Database string  `maho:"size=123"`
				Abcdef   *string `maho:"size=45,fixed"`
				AbcID    []byte  `maho:"size=16"`
				Aaaaa    [32]byte
				ABCDEF   *uint32
				DefGHi   int16 `maho:"name=DEFGHI"`
			}{},
			ti: &typedInfo{
				tid: maxReservedTableId + 1,
				tn:  tn,
				colNames: []types.Identifier{
					types.ID("col_num", true),
					types.ID("database", true),
					types.ID("abcdef", true),
					types.ID("abc_id", true),
					types.ID("aaaaa", true),
					types.ID("abcdef", true),
					types.ID("DEFGHI", true),
				},
				colTypes: []types.ColumnType{
					{Type: types.Int64Type, Size: 1, NotNull: true},
					{Type: types.StringType, Size: 123, NotNull: true},
					{Type: types.StringType, Size: 45, Fixed: true},
					{Type: types.BytesType, Size: 16, NotNull: true},
					{Type: types.BytesType, Size: 32, Fixed: true, NotNull: true},
					{Type: types.Int64Type, Size: 4},
					{Type: types.Int64Type, Size: 2, NotNull: true},
				},
			},
		},
		{
			row: &struct {
				Name  string
				Field string `db:"novalue"`
			}{},
			ti: &typedInfo{
				colNames: []types.Identifier{types.ID("name", true), types.ID("field", true)},
				colTypes: []types.ColumnType{
					{Type: types.StringType, Size: 1, NotNull: true},
					{Type: types.StringType, Size: 1, NotNull: true},
				},
			},
		},
		{
			row: sequencesRow{},
			ti: &typedInfo{
				colNames: []types.Identifier{
					types.ID("sequence", true),
					types.ID("current", true),
				},
				colTypes: []types.ColumnType{
					{Type: types.StringType, Size: 128, NotNull: true},
					{Type: types.Int64Type, Size: 8, NotNull: true},
				},
				primary: []types.ColumnKey{types.MakeColumnKey(0, false)},
			},
		},
		{
			row: &tablesRow{},
			ti: &typedInfo{
				colNames: []types.Identifier{
					types.ID("database", true),
					types.ID("schema", true),
					types.ID("table", true),
					types.ID("table_id", true),
				},
				colTypes: []types.ColumnType{
					{Type: types.StringType, Size: 128, NotNull: true},
					{Type: types.StringType, Size: 128, NotNull: true},
					{Type: types.StringType, Size: 128, NotNull: true},
					{Type: types.Int64Type, Size: 4, NotNull: true},
				},
				primary: []types.ColumnKey{
					types.MakeColumnKey(0, false),
					types.MakeColumnKey(1, false),
					types.MakeColumnKey(2, false),
				},
			},
		},
	}

	for _, c := range cases {
		ti, panicked := typedInfoPanicked(func() *typedInfo {
			return makeTypedInfo(c.tid, c.tn, c.row)
		})
		if panicked {
			if !c.panicked {
				t.Errorf("makeTypedTableInfo(%#v) panicked", c.row)
			}
		} else if c.panicked {
			t.Errorf("makeTypedTableInfo(%#v) did not panic", c.row)
		} else if !reflect.DeepEqual(ti, c.ti) {
			t.Errorf("makeTypedTableInfo(%#v) got %#v want %#v", c.row, ti, c.ti)
		}
	}
}
