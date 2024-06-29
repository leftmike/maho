package test

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"testing"

	"github.com/leftmike/maho/pkg/storage"
	"github.com/leftmike/maho/pkg/testutil"
	"github.com/leftmike/maho/pkg/types"
)

type NewStore func(dataDir string) (storage.Store, error)

func errorPanicked(fn func() error) (err error, panicked bool) {
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

func tableErrorPanicked(fn func() (storage.Table, error)) (tbl storage.Table, err error,
	panicked bool) {

	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(string); ok {
				panicked = true
			} else {
				panic(r)
			}
		}
	}()

	tbl, err = fn()
	panicked = false
	return
}

func rowErrorPanicked(fn func() (types.Row, error)) (row types.Row, err error,
	panicked bool) {

	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(string); ok {
				panicked = true
			} else {
				panic(r)
			}
		}
	}()

	row, err = fn()
	panicked = false
	return
}

func rowIdErrorPanicked(fn func() (storage.RowId, error)) (rowId storage.RowId, err error,
	panicked bool) {

	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(string); ok {
				panicked = true
			} else {
				panic(r)
			}
		}
	}()

	rowId, err = fn()
	panicked = false
	return
}

func tableName(tid storage.TableId) types.TableName {
	return types.TableName{
		Database: types.ID("db", false),
		Schema:   types.ID("scm", false),
		Table:    types.ID(fmt.Sprintf("tbl%d", tid), false),
	}
}

type OpenTable struct {
	tid      storage.TableId
	panicked bool
}

type CreateTable struct {
	tid      storage.TableId
	colNames []types.Identifier
	colTypes []types.ColumnType
	primary  []types.ColumnKey
	panicked bool
}

type DropTable struct {
	tid      storage.TableId
	panicked bool
}

type TableType struct {
	tid      storage.TableId
	ver      uint32
	colNames []types.Identifier
	colTypes []types.ColumnType
	primary  []types.ColumnKey
}

type Commit struct {
	panicked bool
}

type Rollback struct {
	panicked bool
}

type NextStmt struct{}

type Rows struct {
	cols   []types.ColumnNum
	minRow types.Row
	maxRow types.Row
	pred   storage.Predicate
	fail   bool
}

type Update struct {
	rid  storage.RowId
	cols []types.ColumnNum
	vals []types.Value
	fail bool
}
type Delete struct {
	rid  storage.RowId
	fail bool
}

type Insert struct {
	rows []types.Row
	fail bool
}

type Next struct {
	row      types.Row
	fail     bool
	eof      bool
	panicked bool
}

type Current struct {
	fail     bool
	panicked bool
}

type Close struct {
	fail     bool
	panicked bool
}

// XXX: add ways to call Select, UpdateSet, DeleteFrom

func testStorage(t *testing.T, tx storage.Transaction, tbl storage.Table,
	cases []interface{}) storage.Table {

	ctx := context.Background()

	var rows storage.Rows
	var rid storage.RowId
	var err error
	for _, c := range cases {
		switch c := c.(type) {
		case OpenTable:
			var panicked bool
			tbl, err, panicked = tableErrorPanicked(func() (storage.Table, error) {
				return tx.OpenTable(ctx, c.tid)
			})
			if panicked {
				if !c.panicked {
					t.Errorf("OpenTable(%d) panicked", c.tid)
				}
			} else if c.panicked {
				t.Errorf("OpenTable(%d) did not panic", c.tid)
			} else if err != nil {
				t.Errorf("OpenTable(%d) failed with %s", c.tid, err)
			}
		case CreateTable:
			tn := tableName(c.tid)
			err, panicked := errorPanicked(func() error {
				return tx.CreateTable(ctx, c.tid, tn, c.colNames, c.colTypes, c.primary)
			})
			if panicked {
				if !c.panicked {
					t.Errorf("CreateTable(%d) panicked", c.tid)
				}
			} else if c.panicked {
				t.Errorf("CreateTable(%d) did not panic", c.tid)
			} else if err != nil {
				t.Errorf("CreateTable(%d) failed with %s", c.tid, err)
			}
		case DropTable:
			err, panicked := errorPanicked(func() error {
				return tx.DropTable(ctx, c.tid)
			})
			if panicked {
				if !c.panicked {
					t.Errorf("DropTable(%d) panicked", c.tid)
				}
				continue
			} else if c.panicked {
				t.Errorf("DropTable(%d) did not panic", c.tid)
			} else if err != nil {
				t.Errorf("DropTable(%d) failed with %s", c.tid, err)
			}
		case TableType:
			// tbl must be valid

			tid := tbl.TID()
			if tid != c.tid {
				t.Errorf("%d.TID() got %d want %d", c.tid, tid, c.tid)
			}

			tn := tableName(c.tid)
			if tbl.Name() != tn {
				t.Errorf("%d.Name() got %s want %s", c.tid, tbl.Name(), tn)
			}
			ver := tbl.Version()
			if ver != c.ver {
				t.Errorf("%d.Version() got %d want %d", c.tid, ver, c.ver)
			}

			cn := tbl.ColumnNames()
			ct := tbl.ColumnTypes()
			if c.primary == nil {
				for {
					if len(cn) == 0 || cn[0] != 0 {
						break
					}

					cn = cn[1:]
					ct = ct[1:]
				}
			}

			if !reflect.DeepEqual(cn, c.colNames) {
				t.Errorf("%d.ColumnNames() got %#v want %#v", c.tid, cn, c.colNames)
			}
			if !reflect.DeepEqual(ct, c.colTypes) {
				t.Errorf("%d.ColumnTypes() got %#v want %#v", c.tid, ct, c.colTypes)
			}
			if c.primary != nil {
				p := tbl.Primary()
				if !reflect.DeepEqual(p, c.primary) {
					t.Errorf("%d.Primary() got %#v want %#v", c.tid, p, c.primary)
				}
			}
		case Commit:
			err, panicked := errorPanicked(func() error {
				return tx.Commit(ctx)
			})
			if panicked {
				if !c.panicked {
					t.Errorf("Commit() panicked")
				}
			} else if c.panicked {
				t.Errorf("Commit() did not panic")
			} else if err != nil {
				t.Errorf("Commit() failed with %s", err)
			}

			if err == nil && !panicked {
				tx = nil
			}
		case Rollback:
			err, panicked := errorPanicked(func() error {
				return tx.Rollback()
			})
			if panicked {
				if !c.panicked {
					t.Errorf("Rollback() panicked")
				}
			} else if c.panicked {
				t.Errorf("Rollback() did not panic")
				tx = nil
			} else if err != nil {
				t.Errorf("Rollback() failed with %s", err)
			}

			if err == nil && !panicked {
				tx = nil
			}
		case NextStmt:
			tx.NextStmt()
		case Rows:
			var err error
			rows, err = tbl.Rows(ctx, c.cols, c.minRow, c.maxRow, c.pred)
			if c.fail {
				if err == nil {
					t.Errorf("%d.Rows() did not fail", tbl.TID())
				}
			} else if err != nil {
				t.Errorf("%d.Rows() failed with %s", tbl.TID(), err)
			}
		case Update:
			err := tbl.Update(ctx, rid, c.cols, c.vals)
			if c.fail {
				if err == nil {
					t.Errorf("%d.Update() did not fail", tbl.TID())
				}
			} else if err != nil {
				t.Errorf("%d.Update() failed with %s", tbl.TID(), err)
			}
		case Delete:
			err := tbl.Delete(ctx, rid)
			if c.fail {
				if err == nil {
					t.Errorf("%d.Delete() did not fail", tbl.TID())
				}
			} else if err != nil {
				t.Errorf("%d.Delete() failed with %s", tbl.TID(), err)
			}
		case Insert:
			err := tbl.Insert(ctx, c.rows)
			if c.fail {
				if err == nil {
					t.Errorf("%d.Insert() did not fail", tbl.TID())
				}
			} else if err != nil {
				t.Errorf("%d.Insert() failed with %s", tbl.TID(), err)
			}
		case Next:
			row, err, panicked := rowErrorPanicked(func() (types.Row, error) {
				return rows.Next(ctx)
			})
			if panicked {
				if !c.panicked {
					t.Errorf("Rows(%d).Next() panicked", tbl.TID())
				}
			} else if c.panicked {
				t.Errorf("Rows(%d).Next() did not panic", tbl.TID())
			} else if c.fail {
				if err == nil {
					t.Errorf("Rows(%d).Next() did not fail", tbl.TID())
				}
			} else if c.eof {
				if err != io.EOF {
					if err != nil {
						t.Errorf("Rows(%d).Next() did not return io.EOF: %s", tbl.TID(), err)
					} else {
						t.Errorf("Rows(%d).Next() did not return io.EOF: %s", tbl.TID(), row)
					}
				}
			} else if err != nil {
				t.Errorf("Rows(%d).Next() failed with %s", tbl.TID(), err)
			} else if testutil.CompareRows(row, c.row) != 0 {
				t.Errorf("Rows(%d).Next() got %s want %s", tbl.TID(), row, c.row)
			}
		case Current:
			var panicked bool
			rid, err, panicked = rowIdErrorPanicked(func() (storage.RowId, error) {
				return rows.Current()
			})
			if panicked {
				if !c.panicked {
					t.Errorf("Rows(%d).Current() panicked", tbl.TID())
				}
			} else if c.panicked {
				t.Errorf("Rows(%d).Current() did not panic", tbl.TID())
			} else if c.fail {
				if err == nil {
					t.Errorf("Rows(%d).Current() did not fail", tbl.TID())
				}
			} else if err != nil {
				t.Errorf("Rows(%d).Current() failed with %s", tbl.TID(), err)
			}
		case Close:
			err, panicked := errorPanicked(func() error {
				return rows.Close(ctx)
			})
			if panicked {
				if !c.panicked {
					t.Errorf("Rows(%d).Close() panicked", tbl.TID())
				}
			} else if c.panicked {
				t.Errorf("Rows(%d).Close() did not panic", tbl.TID())
			} else if c.fail {
				if err == nil {
					t.Errorf("Rows(%d).Close() did not fail", tbl.TID())
				}
			} else if err != nil {
				t.Errorf("Rows(%d).Close() failed with %s", tbl.TID(), err)
			}
		}
	}

	return tbl
}
