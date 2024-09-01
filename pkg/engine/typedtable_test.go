package engine_test

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"testing"

	"github.com/leftmike/maho/pkg/engine"
	"github.com/leftmike/maho/pkg/storage"
	"github.com/leftmike/maho/pkg/storage/basic"
	u "github.com/leftmike/maho/pkg/testutil"
	"github.com/leftmike/maho/pkg/types"
)

func allRows(t *testing.T, tx storage.Transaction, tid storage.TableId, tn types.TableName,
	ti *engine.TypedInfo) []types.Row {

	t.Helper()

	ctx := context.Background()
	tt := ti.TableType()
	tbl, err := tx.OpenTable(ctx, tid, tn, tt.ColumnNames, tt.ColumnTypes, tt.Key)
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

type testRow struct {
	Col0 int `maho:"primary"`
	Col1 bool
	Col2 string  `maho:"size=64"`
	Col3 *string `maho:"size=3"`
	Col4 []byte  `maho:"size=32,notnull"`
	Col5 []byte  `maho:"size=4"`
	Col6 float64
	Col7 int8
	Col8 *int64
}

func testRows(t *testing.T, tn types.TableName, tt *engine.TypedTable, structs []testRow,
	min, max int) {

	ctx := context.Background()

	var minSt, maxSt interface{}
	if min < 0 {
		min = 0
	} else {
		minSt = &testRow{Col0: min}
	}
	if max < 0 {
		max = len(structs) - 1
	} else {
		maxSt = &testRow{Col0: max}
	}
	tr, err := tt.Rows(ctx, minSt, maxSt)
	if err != nil {
		t.Fatalf("rows(%s) failed with %s", tn, err)
	}
	defer func() {
		err := tr.Close(ctx)
		if err != nil {
			t.Fatalf("close(%s) failed with %s", tn, err)
		}
	}()

	for idx := min; idx <= max; idx += 1 {
		st := structs[idx]
		var trow testRow
		err = tr.Next(ctx, &trow)
		if err != nil {
			t.Errorf("next(%s) failed with %s", tn, err)
		} else if !reflect.DeepEqual(trow, st) {
			t.Errorf("next(%s) got %#v want %#v", tn, trow, st)
		}
	}

	var trow testRow
	err = tr.Next(ctx, &trow)
	if err != io.EOF {
		t.Errorf("next(%s) got %s want io.EOF", tn, err)
	}
}

func testLookup(t *testing.T, tn types.TableName, tt *engine.TypedTable, structs []testRow,
	idx int) {

	ctx := context.Background()

	trow := testRow{Col0: idx}
	err := tt.Lookup(ctx, &trow)
	if err != nil {
		t.Errorf("lookup(%s) failed with %s", tn, err)
	} else if !reflect.DeepEqual(trow, structs[idx]) {
		t.Errorf("lookup(%s) got %#v want %#v", tn, trow, structs[idx])
	}
}

func TestTypedTable(t *testing.T) {
	s2 := "xyz"
	i2 := int64(1234)

	structs := []testRow{
		{Col0: 0},
		{
			Col0: 1,
			Col1: true,
			Col2: "abcdef",
			Col4: []byte{0, 1, 2},
			Col5: []byte{3, 4, 5, 6},
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
		{u.I(0), u.B(false), u.S(""), nil, u.Bytes(), nil, u.F(0), u.I(0), nil},
		{u.I(1), u.B(true), u.S("abcdef"), nil, u.Bytes(0, 1, 2), u.Bytes(3, 4, 5, 6),
			u.F(123.456), u.I(78), nil},
		{u.I(2), u.B(false), u.S(""), u.S("xyz"), u.Bytes(), nil, u.F(0), u.I(0), u.I(1234)},
	}

	tid := storage.TableId(2048)
	tn := types.TableName{types.ID("d", false), types.ID("s", false), types.ID("t", false)}
	ti := engine.MakeTypedInfo(tid, tn, structs[0])

	dataDir := t.TempDir()
	store, err := basic.NewStore(dataDir)
	if err != nil {
		t.Fatalf("NewStore(%s) failed with %s", dataDir, err)
	}

	ctx := context.Background()
	tx := store.Begin()
	err = engine.CreateTypedTable(ctx, tx, ti)
	if err != nil {
		t.Fatalf("CreateTypedTable(%s) failed with %s", tn, err)
	}

	tt, err := engine.OpenTypedTable(ctx, tx, ti)
	if err != nil {
		t.Fatalf("OpenTypedTable(%s) failed with %s", tn, err)
	}

	for _, st := range structs {
		err = tt.Insert(ctx, &st)
		if err != nil {
			t.Errorf("insert(%#v) failed with %s", st, err)
		}
	}

	err = tx.Commit(ctx)
	if err != nil {
		t.Fatalf("Commit() failed with %s", err)
	}

	tx = store.Begin()
	all := allRows(t, tx, tid, tn, ti)
	err = tx.Commit(ctx)
	if err != nil {
		t.Fatalf("Commit() failed with %s", err)
	}

	if !u.RowsEqual(rows, all, true) {
		t.Errorf("Rows(%d) got %v want %v", tid, all, rows)
	}

	tx = store.Begin()
	tt, err = engine.OpenTypedTable(ctx, tx, ti)
	if err != nil {
		t.Fatalf("OpenTypedTable(%s) failed with %s", tn, err)
	}

	testRows(t, tn, tt, structs, -1, -1)
	testRows(t, tn, tt, structs, 1, -1)
	testRows(t, tn, tt, structs, -1, 1)
	testRows(t, tn, tt, structs, 2, 2)

	testLookup(t, tn, tt, structs, 0)
	testLookup(t, tn, tt, structs, 1)
	testLookup(t, tn, tt, structs, 2)

	err = tx.Commit(ctx)
	if err != nil {
		t.Fatalf("Commit() failed with %s", err)
	}
}

type openTypedTable struct {
	ti   *engine.TypedInfo
	fail bool
}

type createTypedTable struct {
	ti   *engine.TypedInfo
	fail bool
}

func testTypedTables(t *testing.T, tx storage.Transaction, cases []interface{}) {
	t.Helper()

	ctx := context.Background()

	var tt *engine.TypedTable
	_ = tt
	//var tr *engine.TypedRows
	//var trr *engine.TypedRowRef
	var err error
	for _, c := range cases {
		switch c := c.(type) {
		case openTypedTable:
			tt, err = engine.OpenTypedTable(ctx, tx, c.ti)
			if c.fail {
				if err == nil {
					t.Errorf("OpenTypedTable(%s, %d) did not fail", c.ti.TableName(),
						c.ti.TableId())
				}
			} else if err != nil {
				t.Errorf("OpenTypedTable(%s, %d) failed with %s", c.ti.TableName(),
					c.ti.TableId(), err)
			}
		case createTypedTable:
			err = engine.CreateTypedTable(ctx, tx, c.ti)
			if c.fail {
				if err == nil {
					t.Errorf("CreateTypedTable(%s, %d) did not fail", c.ti.TableName(),
						c.ti.TableId())
				}
			} else if err != nil {
				t.Errorf("CreateTypedTable(%s, %d) failed with %s", c.ti.TableName(),
					c.ti.TableId(), err)
			}
		default:
			panic(fmt.Sprintf("unexpected case: %T %#v", c, c))
		}
	}
}
