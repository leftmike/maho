package engine_test

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"testing"

	"github.com/leftmike/maho/pkg/engine"
	"github.com/leftmike/maho/pkg/storage"
	"github.com/leftmike/maho/pkg/storage/basic"
	"github.com/leftmike/maho/pkg/types"
)

func intp(i int64) *int64 {
	return &i
}

func makeTypedInfo() *engine.TypedInfo {
	return engine.MakeTypedInfo(storage.TableId(2048),
		types.TableName{types.ID("d", false), types.ID("s", false), types.ID("t", false)},
		&typedRow{})
}

func newStore(t *testing.T) storage.Store {
	dataDir := t.TempDir()
	store, err := basic.NewStore(dataDir)
	if err != nil {
		t.Fatalf("NewStore(%s) failed with %s", dataDir, err)
	}

	return store
}

func TestTypedTable(t *testing.T) {
	ti := makeTypedInfo()
	store := newStore(t)

	rows := []typedRow{
		{C0: 0},
		{
			C0: 1,
			C1: true,
			C2: "abcd",
			C3: []byte{0, 1, 2, 3, 4, 5},
			C4: 123.456,
			C5: intp(123),
		},
		{C0: 2, C2: "efghijkl", C4: 0.123},
		{C0: 3, C1: true, C3: []byte{6, 7, 8}, C5: intp(4567)},
	}

	testTypedTables(t, store.Begin(),
		[]interface{}{
			createTypedTable{ti: ti},
			openTypedTable{ti: ti},
			insertTypedTable{rows: rows},
			commit{},
		})

	testTypedTables(t, store.Begin(),
		[]interface{}{
			openTypedTable{ti: ti},
			selectTypedTable{rows: rows},
			selectTypedTable{
				minSt: &typedRow{C0: 1},
				rows:  rows[1:],
			},
			selectTypedTable{
				maxSt: &typedRow{C0: 1},
				rows:  rows[:2],
			},
			selectTypedTable{
				minSt: &typedRow{C0: 1},
				maxSt: &typedRow{C0: 2},
				rows:  rows[1:3],
			},
			selectTypedTable{
				minSt: &typedRow{C0: 4},
			},
			lookupTypedTable{
				st:   typedRow{C0: 3},
				want: rows[3],
			},
			lookupTypedTable{
				st:   typedRow{C0: 0},
				want: rows[0],
			},
			lookupTypedTable{
				st:   typedRow{C0: 2},
				want: rows[2],
			},
			lookupTypedTable{
				st:   typedRow{C0: 1},
				want: rows[1],
			},
			lookupTypedTable{
				st:   typedRow{C0: 4},
				fail: true,
			},
			commit{},
		})

	testTypedTables(t, store.Begin(),
		[]interface{}{
			openTypedTable{ti: ti},
			insertTypedTable{
				rows: []typedRow{{C0: 0}},
				fail: true,
			},
			rollback{},
		})
}

func TestTypedUpdate(t *testing.T) {
	ti := makeTypedInfo()
	store := newStore(t)

	testTypedTables(t, store.Begin(),
		[]interface{}{
			createTypedTable{ti: ti},
			openTypedTable{ti: ti},
			insertTypedTable{
				rows: []typedRow{
					{C0: 0},
					{C0: 1},
					{C0: 2},
					{C0: 3},
					{C0: 4},
					{C0: 5},
				},
			},
			commit{},
		})

	testTypedTables(t, store.Begin(),
		[]interface{}{
			openTypedTable{ti: ti},
			updateTypedTable{
				update: func(st typedRow) interface{} {
					if st.C0%2 == 0 {
						return &struct {
							C1 bool
							C2 string
							C4 float64
						}{
							C1: true,
							C2: strconv.Itoa(st.C0),
							C4: float64(st.C0 * 10),
						}
					}

					return &struct {
						C3 []byte
						C5 *int64
					}{
						C3: []byte{byte(st.C0), byte(st.C0), byte(st.C0)},
						C5: intp(int64(st.C0 * 10)),
					}
				},
			},
			commit{},
		})

	testTypedTables(t, store.Begin(),
		[]interface{}{
			openTypedTable{ti: ti},
			selectTypedTable{
				rows: []typedRow{
					{C0: 0, C1: true, C2: "0", C4: 0},
					{C0: 1, C3: []byte{1, 1, 1}, C5: intp(10)},
					{C0: 2, C1: true, C2: "2", C4: 20},
					{C0: 3, C3: []byte{3, 3, 3}, C5: intp(30)},
					{C0: 4, C1: true, C2: "4", C4: 40},
					{C0: 5, C3: []byte{5, 5, 5}, C5: intp(50)},
				},
			},
			commit{},
		})
}

func TestTypedDelete(t *testing.T) {
	ti := makeTypedInfo()
	store := newStore(t)

	testTypedTables(t, store.Begin(),
		[]interface{}{
			createTypedTable{ti: ti},
			openTypedTable{ti: ti},
			insertTypedTable{
				rows: []typedRow{
					{C0: 0},
					{C0: 1},
					{C0: 2},
					{C0: 3},
					{C0: 4},
					{C0: 5},
					{C0: 6},
					{C0: 7},
					{C0: 8},
					{C0: 9},
				},
			},
			commit{},
		})

	testTypedTables(t, store.Begin(),
		[]interface{}{
			openTypedTable{ti: ti},
			deleteTypedTable{
				minSt: &typedRow{C0: 7},
			},
			commit{},
		})

	testTypedTables(t, store.Begin(),
		[]interface{}{
			openTypedTable{ti: ti},
			selectTypedTable{
				rows: []typedRow{
					{C0: 0},
					{C0: 1},
					{C0: 2},
					{C0: 3},
					{C0: 4},
					{C0: 5},
					{C0: 6},
				},
			},
			commit{},
		})

	testTypedTables(t, store.Begin(),
		[]interface{}{
			openTypedTable{ti: ti},
			deleteTypedTable{
				minSt: &typedRow{C0: 2},
				maxSt: &typedRow{C0: 5},
			},
			commit{},
		})

	testTypedTables(t, store.Begin(),
		[]interface{}{
			openTypedTable{ti: ti},
			selectTypedTable{
				rows: []typedRow{
					{C0: 0},
					{C0: 1},
					{C0: 6},
				},
			},
			commit{},
		})
}

type openTypedTable struct {
	ti   *engine.TypedInfo
	fail bool
}

type createTypedTable struct {
	ti   *engine.TypedInfo
	fail bool
}

type typedRow struct {
	C0 int `maho:"primary"`
	C1 bool
	C2 string `maho:"size=32"`
	C3 []byte `maho:"size=16"`
	C4 float64
	C5 *int64
}

type insertTypedTable struct {
	rows []typedRow
	fail bool
}

type lookupTypedTable struct {
	st   typedRow
	want typedRow
	fail bool
}

type selectTypedTable struct {
	minSt interface{}
	maxSt interface{}
	rows  []typedRow
}

type updateTypedTable struct {
	minSt  interface{}
	maxSt  interface{}
	update func(st typedRow) interface{}
}

type deleteTypedTable struct {
	minSt interface{}
	maxSt interface{}
}

type commit struct{}
type rollback struct{}

func selectFunc(t *testing.T, what string, tt *engine.TypedTable, minSt, maxSt interface{},
	fn func(trr *engine.TypedRowRef, st typedRow)) {

	t.Helper()

	ctx := context.Background()
	tr, err := tt.Rows(ctx, minSt, maxSt)
	if err != nil {
		t.Errorf("%s(%s, %d).Rows() failed with %s", what, tt.TypedInfo().TableName(),
			tt.TypedInfo().TableId(), err)
		return
	}

	for {
		var st typedRow
		err = tr.Next(ctx, &st)
		if err == io.EOF {
			break
		} else if err != nil {
			t.Errorf("%s(%s, %d).Next() failed with %s", what, tt.TypedInfo().TableName(),
				tt.TypedInfo().TableId(), err)
			break
		}

		trr, err := tr.Current()
		if err != nil {
			t.Errorf("%s(%s, %d).Current() failed with %s", what, tt.TypedInfo().TableName(),
				tt.TypedInfo().TableId(), err)
			break
		}

		fn(trr, st)
	}

	err = tr.Close(ctx)
	if err != nil {
		t.Errorf("%s(%s, %d).Close() failed with %s", what, tt.TypedInfo().TableName(),
			tt.TypedInfo().TableId(), err)
	}
}

func testTypedTables(t *testing.T, tx storage.Transaction, cases []interface{}) {
	t.Helper()

	ctx := context.Background()

	var tt *engine.TypedTable
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
		case insertTypedTable:
			for _, r := range c.rows {
				err = tt.Insert(ctx, &r)
				if c.fail {
					if err == nil {
						t.Errorf("Insert(%s, %d) did not fail", tt.TypedInfo().TableName(),
							tt.TypedInfo().TableId())
					}
				} else if err != nil {
					t.Errorf("Insert(%s, %d) failed with %s", tt.TypedInfo().TableName(),
						tt.TypedInfo().TableId(), err)
				}
			}
		case lookupTypedTable:
			st := c.st
			err = tt.Lookup(ctx, &st)
			if c.fail {
				if err == nil {
					t.Errorf("Lookup(%s, %d) did not fail", tt.TypedInfo().TableName(),
						tt.TypedInfo().TableId())
				}
			} else if err != nil {
				t.Errorf("Lookup(%s, %d) failed with %s", tt.TypedInfo().TableName(),
					tt.TypedInfo().TableId(), err)
			} else if !reflect.DeepEqual(st, c.want) {
				t.Errorf("Lookup(%s, %d) got %#v want %#v", tt.TypedInfo().TableName(),
					tt.TypedInfo().TableId(), st, c.want)
			}
		case selectTypedTable:
			var rows []typedRow
			selectFunc(t, "Select", tt, c.minSt, c.maxSt,
				func(trr *engine.TypedRowRef, st typedRow) {
					rows = append(rows, st)
				})
			if !reflect.DeepEqual(rows, c.rows) {
				t.Errorf("Select(%s, %d) got %#v want %#v", tt.TypedInfo().TableName(),
					tt.TypedInfo().TableId(), rows, c.rows)
			}
		case updateTypedTable:
			selectFunc(t, "Update", tt, c.minSt, c.maxSt,
				func(trr *engine.TypedRowRef, st typedRow) {
					err := trr.Update(ctx, c.update(st))
					if err != nil {
						t.Errorf("Update(%s, %d) failed with %s", tt.TypedInfo().TableName(),
							tt.TypedInfo().TableId(), err)
					}

				})
		case deleteTypedTable:
			selectFunc(t, "Delete", tt, c.minSt, c.maxSt,
				func(trr *engine.TypedRowRef, st typedRow) {
					err := trr.Delete(ctx)
					if err != nil {
						t.Errorf("Delete(%s, %d) failed with %s", tt.TypedInfo().TableName(),
							tt.TypedInfo().TableId(), err)
					}
				})
		case commit:
			err = tx.Commit(ctx)
			if err != nil {
				t.Fatalf("Commit() failed with %s", err)
			}
		case rollback:
			err = tx.Rollback()
			if err != nil {
				t.Fatalf("Rollback() failed with %s", err)
			}
		default:
			panic(fmt.Sprintf("unexpected case: %T %#v", c, c))
		}
	}
}
