package engine

import (
	"context"
	"io"
	"reflect"
	"testing"

	"github.com/leftmike/maho/pkg/storage"
	"github.com/leftmike/maho/pkg/storage/basic"
	u "github.com/leftmike/maho/pkg/testutil"
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

func allRows(t *testing.T, tx storage.Transaction, tid storage.TableId) []types.Row {
	t.Helper()

	ctx := context.Background()
	tbl, err := tx.OpenTable(ctx, tid)
	if err != nil {
		t.Fatalf("OpenTable(%d) failed with %s", tid, err)
	}
	rows, err := tbl.Rows(ctx, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("Rows(%d) failed with %s", tid, err)
	}

	var all []types.Row
	for {
		row, err := rows.Next(ctx)
		if err == io.EOF {
			break
		} else if err != nil {
			t.Fatalf("Next(%d) failed with %s", tid, err)
		}

		all = append(all, row)
	}

	err = rows.Close(ctx)
	if err != nil {
		t.Fatalf("Close(%d) failed with %s", tid, err)
	}

	return all
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
		} else {
			ti.typ = nil

			if !reflect.DeepEqual(ti, c.ti) {
				t.Errorf("makeTypedTableInfo(%#v) got %#v want %#v", c.row, ti, c.ti)
			}
		}
	}
}

func TestTypedInsert(t *testing.T) {
	s2 := "xyz"
	i2 := int64(1234)

	structs := []struct {
		Col0 int `maho:"primary"`
		Col1 bool
		Col2 string  `maho:"size=64"`
		Col3 *string `maho:"size=3,fixed"`
		Col4 []byte  `maho:"size=32"`
		Col5 [4]byte
		Col6 float64
		Col7 byte
		Col8 *int64
	}{
		{Col0: 0},
		{
			Col0: 1,
			Col1: true,
			Col2: "abcdef",
			Col4: []byte{0, 1, 2},
			Col5: [4]byte{3, 4, 5, 6},
			Col6: 123.456,
			Col7: 78,
		},
		{
			Col0: 2,
			Col3: &s2,
			Col8: &i2,
		},
	}

	rows := []types.Row{
		{u.I(0), u.B(false), u.S(""), nil, u.Bytes(), u.Bytes(0, 0, 0, 0), u.F(0), u.I(0), nil},
		{u.I(1), u.B(true), u.S("abcdef"), nil, u.Bytes(0, 1, 2), u.Bytes(3, 4, 5, 6),
			u.F(123.456), u.I(78), nil},
		{u.I(2), u.B(false), u.S(""), u.S("xyz"), u.Bytes(), u.Bytes(0, 0, 0, 0), u.F(0), u.I(0),
			u.I(1234)},
	}

	tid := maxReservedTableId + 1
	tn := types.TableName{types.ID("d", false), types.ID("s", false), types.ID("t", false)}
	ti := makeTypedInfo(tid, tn, structs[0])

	dataDir := t.TempDir()
	store, err := basic.NewStore(dataDir)
	if err != nil {
		t.Fatalf("NewStore(%s) failed with %s", dataDir, err)
	}

	ctx := context.Background()
	tx := store.Begin()
	err = createTypedTable(ctx, tx, ti)
	if err != nil {
		t.Fatalf("createTypedTable(%s) failed with %s", tn, err)
	}

	tt, err := openTypedTable(ctx, tx, ti)
	if err != nil {
		t.Fatalf("openTypedTable(%s) failed with %s", tn, err)
	}

	for _, st := range structs {
		err = tt.insert(ctx, &st)
		if err != nil {
			t.Errorf("insert(%#v) failed with %s", st, err)
		}
	}

	err = tx.Commit(ctx)
	if err != nil {
		t.Fatalf("Commit() failed with %s", err)
	}

	tx = store.Begin()
	all := allRows(t, tx, tid)
	err = tx.Commit(ctx)
	if err != nil {
		t.Fatalf("Commit() failed with %s", err)
	}

	if !u.RowsEqual(rows, all, true) {
		t.Errorf("Rows(%d) got %v want %v", tid, all, rows)
	}
}
