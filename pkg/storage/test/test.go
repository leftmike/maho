package test

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/leftmike/maho/pkg/storage"
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

type Commit struct{}
type Rollback struct{}
type NextStmt struct{}

type Rows struct{}   // XXX
type Update struct{} // XXX
type Delete struct{} // XXX

type Insert struct {
	rows []types.Row
	fail bool
}

func testStorage(t *testing.T, tx storage.Transaction, tbl storage.Table,
	cases []interface{}) (storage.Transaction, storage.Table) {

	ctx := context.Background()

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
				continue
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
				continue
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
			err := tx.Commit(ctx)
			if err != nil {
				t.Errorf("Commit() failed with %s", err)
			}
			tx = nil
		case Rollback:
			err := tx.Rollback()
			if err != nil {
				t.Errorf("Rollback() failed with %s", err)
			}
			tx = nil
		case NextStmt:
			tx.NextStmt()
			// XXX
		case Insert:
			err := tbl.Insert(ctx, c.rows)
			if c.fail {
				if err == nil {
					t.Errorf("%d.Insert() did not fail", tbl.TID())
				}
			} else if err != nil {
				t.Errorf("%d.Insert() failed with %s", tbl.TID(), err)
			}
		}
	}

	return tx, tbl
}
