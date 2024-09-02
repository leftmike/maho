package engine_test

import (
	"context"
	"fmt"
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
			typedTableInsert{
				ti:   ti,
				rows: rows,
			},
			commit{},
		})

	testTypedTables(t, store.Begin(),
		[]interface{}{
			typedTableSelect{
				ti:   ti,
				rows: rows,
			},
			typedTableSelect{
				ti:    ti,
				minSt: &typedRow{C0: 1},
				rows:  rows[1:],
			},
			typedTableSelect{
				ti:    ti,
				maxSt: &typedRow{C0: 1},
				rows:  rows[:2],
			},
			typedTableSelect{
				ti:    ti,
				minSt: &typedRow{C0: 1},
				maxSt: &typedRow{C0: 2},
				rows:  rows[1:3],
			},
			typedTableSelect{
				ti:    ti,
				minSt: &typedRow{C0: 4},
			},
			typedTableLookup{
				ti:   ti,
				st:   typedRow{C0: 3},
				want: rows[3],
			},
			typedTableLookup{
				ti:   ti,
				st:   typedRow{C0: 0},
				want: rows[0],
			},
			typedTableLookup{
				ti:   ti,
				st:   typedRow{C0: 2},
				want: rows[2],
			},
			typedTableLookup{
				ti:   ti,
				st:   typedRow{C0: 1},
				want: rows[1],
			},
			typedTableLookup{
				ti:   ti,
				st:   typedRow{C0: 4},
				fail: true,
			},
			commit{},
		})

	testTypedTables(t, store.Begin(),
		[]interface{}{
			typedTableInsert{
				ti:   ti,
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
			typedTableInsert{
				ti: ti,
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
			typedTableUpdate{
				ti: ti,
				update: func(st typedRow) (interface{}, error) {
					if st.C0%2 == 0 {
						return &struct {
							C1 bool
							C2 string
							C4 float64
						}{
							C1: true,
							C2: strconv.Itoa(st.C0),
							C4: float64(st.C0 * 10),
						}, nil
					}

					return &struct {
						C3 []byte
						C5 *int64
					}{
						C3: []byte{byte(st.C0), byte(st.C0), byte(st.C0)},
						C5: intp(int64(st.C0 * 10)),
					}, nil
				},
			},
			commit{},
		})

	testTypedTables(t, store.Begin(),
		[]interface{}{
			typedTableSelect{
				ti: ti,
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
			typedTableInsert{
				ti: ti,
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
			typedTableDelete{
				ti:    ti,
				minSt: &typedRow{C0: 7},
			},
			commit{},
		})

	testTypedTables(t, store.Begin(),
		[]interface{}{
			typedTableSelect{
				ti: ti,
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
			typedTableDelete{
				ti:    ti,
				minSt: &typedRow{C0: 2},
				maxSt: &typedRow{C0: 5},
			},
			commit{},
		})

	testTypedTables(t, store.Begin(),
		[]interface{}{
			typedTableSelect{
				ti: ti,
				rows: []typedRow{
					{C0: 0},
					{C0: 1},
					{C0: 6},
				},
			},
			commit{},
		})
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

type typedTableInsert struct {
	ti   *engine.TypedInfo
	rows []typedRow
	fail bool
}

type typedTableLookup struct {
	ti   *engine.TypedInfo
	st   typedRow
	want typedRow
	fail bool
}

type typedTableSelect struct {
	ti    *engine.TypedInfo
	minSt interface{}
	maxSt interface{}
	rows  []typedRow
}

type typedTableUpdate struct {
	ti     *engine.TypedInfo
	minSt  interface{}
	maxSt  interface{}
	update func(row typedRow) (interface{}, error)
}

type typedTableDelete struct {
	ti    *engine.TypedInfo
	minSt interface{}
	maxSt interface{}
}

type commit struct{}
type rollback struct{}

func testTypedTables(t *testing.T, tx storage.Transaction, cases []interface{}) {
	t.Helper()

	ctx := context.Background()

	for _, c := range cases {
		switch c := c.(type) {
		case createTypedTable:
			err := engine.CreateTypedTable(ctx, tx, c.ti)
			if c.fail {
				if err == nil {
					t.Errorf("CreateTypedTable(%s, %d) did not fail", c.ti.TableName(),
						c.ti.TableId())
				}
			} else if err != nil {
				t.Errorf("CreateTypedTable(%s, %d) failed with %s", c.ti.TableName(),
					c.ti.TableId(), err)
			}
		case typedTableInsert:
			for _, r := range c.rows {
				err := engine.TypedTableInsert(ctx, tx, c.ti, &r)
				if c.fail {
					if err == nil {
						t.Errorf("Insert(%s, %d) did not fail", c.ti.TableName(), c.ti.TableId())
					}
				} else if err != nil {
					t.Errorf("Insert(%s, %d) failed with %s", c.ti.TableName(), c.ti.TableId(),
						err)
				}
			}
		case typedTableLookup:
			st := c.st
			err := engine.TypedTableLookup(ctx, tx, c.ti, &st)
			if c.fail {
				if err == nil {
					t.Errorf("Lookup(%s, %d) did not fail", c.ti.TableName(), c.ti.TableId())
				}
			} else if err != nil {
				t.Errorf("Lookup(%s, %d) failed with %s", c.ti.TableName(), c.ti.TableId(), err)
			} else if !reflect.DeepEqual(st, c.want) {
				t.Errorf("Lookup(%s, %d) got %#v want %#v", c.ti.TableName(), c.ti.TableId(), st,
					c.want)
			}
		case typedTableSelect:
			var rows []typedRow
			err := engine.TypedTableSelect(ctx, tx, c.ti, c.minSt, c.maxSt,
				func(row types.Row) error {
					var st typedRow
					c.ti.RowToStruct(row, &st)
					rows = append(rows, st)
					return nil
				})
			if err != nil {
				t.Errorf("Select(%s, %d) failed with %s", c.ti.TableName(), c.ti.TableId(), err)
			} else if !reflect.DeepEqual(rows, c.rows) {
				t.Errorf("Select(%s, %d) got %#v want %#v", c.ti.TableName(), c.ti.TableId(),
					rows, c.rows)
			}
		case typedTableUpdate:
			err := engine.TypedTableUpdate(ctx, tx, c.ti, c.minSt, c.maxSt,
				func(row types.Row) (interface{}, error) {
					var st typedRow
					c.ti.RowToStruct(row, &st)
					return c.update(st)
				})
			if err != nil {
				t.Errorf("Update(%s, %d) failed with %s", c.ti.TableName(), c.ti.TableId(), err)
			}
		case typedTableDelete:
			err := engine.TypedTableDelete(ctx, tx, c.ti, c.minSt, c.maxSt,
				func(row types.Row) (bool, error) {
					return true, nil
				})
			if err != nil {
				t.Errorf("Delete(%s, %d) failed with %s", c.ti.TableName(), c.ti.TableId(), err)
			}
		case commit:
			err := tx.Commit(ctx)
			if err != nil {
				t.Fatalf("Commit() failed with %s", err)
			}
		case rollback:
			err := tx.Rollback()
			if err != nil {
				t.Fatalf("Rollback() failed with %s", err)
			}
		default:
			panic(fmt.Sprintf("unexpected case: %T %#v", c, c))
		}
	}
}
