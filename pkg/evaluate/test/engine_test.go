package test_test

import (
	"context"
	"reflect"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/leftmike/maho/pkg/engine"
	"github.com/leftmike/maho/pkg/evaluate/test"
	"github.com/leftmike/maho/pkg/types"
)

var (
	database  = types.ID("db", false)
	database1 = types.ID("db1", false)
	database2 = types.ID("db2", false)
	schema    = types.SchemaName{database, types.ID("scm", false)}
	schema1   = types.SchemaName{database, types.ID("scm1", false)}
	schema2   = types.SchemaName{database, types.ID("scm2", false)}
	table     = types.TableName{database, types.ID("scm", false), types.ID("tbl", false)}
	table1    = types.TableName{database, types.ID("scm", false), types.ID("tbl1", false)}
	table2    = types.TableName{database, types.ID("scm", false), types.ID("tbl2", false)}
	table3    = types.TableName{database, types.ID("scm", false), types.ID("tbl3", false)}
	index1    = types.ID("idx1", false)
	index2    = types.ID("idx2", false)
	colNames  = []types.Identifier{
		types.ID("c1", false),
		types.ID("c2", false),
		types.ID("c3", false),
	}
	colTypes = []types.ColumnType{types.Int64ColType, types.Int64ColType, types.Int64ColType}
	primary  = []types.ColumnKey{types.MakeColumnKey(0, false)}
	key      = []types.ColumnKey{types.MakeColumnKey(1, false)}
)

func TestDatabase(t *testing.T) {
	eng := test.NewEngine(nil)

	err := eng.CreateDatabase(database1, nil)
	if err != nil {
		t.Errorf("CreateDatabase(%s) failed with %s", database1, err)
	}
	err = eng.CreateDatabase(database1, nil)
	if err == nil {
		t.Errorf("CreateDatabase(%s) did not fail", database1)
	}
	err = eng.CreateDatabase(database2, nil)
	if err != nil {
		t.Errorf("CreateDatabase(%s) failed with %s", database2, err)
	}
	err = eng.DropDatabase(database1, false)
	if err != nil {
		t.Errorf("DropDatabase(%s) failed with %s", database1, err)
	}
	err = eng.DropDatabase(database1, false)
	if err == nil {
		t.Errorf("DropDatabase(%s) did not fail", database1)
	}
	err = eng.DropDatabase(database1, true)
	if err != nil {
		t.Errorf("DropDatabase(%s) failed with %s", database1, err)
	}
	err = eng.CreateDatabase(database1, nil)
	if err != nil {
		t.Errorf("CreateDatabase(%s) failed with %s", database1, err)
	}
}

func sortIdentifiers(ids []types.Identifier) []string {
	var strings []string
	for _, id := range ids {
		strings = append(strings, id.String())
	}

	sort.Strings(strings)
	return strings
}

func TestSchema(t *testing.T) {
	eng := test.NewEngine(nil)

	err := eng.CreateDatabase(database, nil)
	if err != nil {
		t.Errorf("CreateDatabase(%s) failed with %s", database, err)
	}

	tx := eng.Begin()
	ctx := context.Background()

	err = tx.CreateSchema(ctx, schema1)
	if err != nil {
		t.Errorf("CreateSchema(%s) failed with %s", schema1, err)
	}
	err = tx.CreateSchema(ctx, schema1)
	if err == nil {
		t.Errorf("CreateSchema(%s) did not fail", schema1)
	}
	err = tx.CreateSchema(ctx, schema2)
	if err != nil {
		t.Errorf("CreateSchema(%s) failed with %s", schema2, err)
	}
	err = tx.DropSchema(ctx, schema1, false)
	if err != nil {
		t.Errorf("DropSchema(%s) failed with %s", schema1, err)
	}
	err = tx.DropSchema(ctx, schema1, false)
	if err == nil {
		t.Errorf("DropSchema(%s) did not fail", schema1)
	}
	err = tx.DropSchema(ctx, schema1, true)
	if err != nil {
		t.Errorf("DropSchema(%s) failed with %s", schema1, err)
	}
	err = tx.CreateSchema(ctx, schema1)
	if err != nil {
		t.Errorf("CreateSchema(%s) failed with %s", schema1, err)
	}

	sn := types.SchemaName{database2, types.ID("scm1", false)}
	err = tx.CreateSchema(ctx, sn)
	if err != nil {
		t.Errorf("CreateSchema(%s) failed with %s", sn, err)
	}

	ids, err := tx.ListSchemas(ctx, database)
	if err != nil {
		t.Errorf("ListSchemas(%s) failed with %s", database, err)
	} else {
		want := "scm1 scm2"
		got := strings.Join(sortIdentifiers(ids), " ")
		if want != got {
			t.Errorf("ListSchemas(%s) got %s want %s", database, got, want)
		}
	}
}

func TestTable(t *testing.T) {
	eng := test.NewEngine(nil)

	err := eng.CreateDatabase(database, nil)
	if err != nil {
		t.Errorf("CreateDatabase(%s) failed with %s", database, err)
	}

	tx := eng.Begin()
	ctx := context.Background()

	err = tx.CreateSchema(ctx, schema)
	if err != nil {
		t.Errorf("CreateSchema(%s) failed with %s", schema, err)
	}

	err = tx.CreateTable(ctx, table1, colNames, colTypes, primary)
	if err != nil {
		t.Errorf("CreateTable(%s) failed with %s", table1, err)
	}
	_, err = tx.OpenTable(ctx, table1)
	if err != nil {
		t.Errorf("OpenTable(%s) failed with %s", table1, err)
	}
	err = tx.CreateTable(ctx, table1, colNames, colTypes, primary)
	if err == nil {
		t.Errorf("CreateTable(%s) did not fail", table1)
	}
	err = tx.CreateTable(ctx, table2, colNames, colTypes, primary)
	if err != nil {
		t.Errorf("CreateTable(%s) failed with %s", table2, err)
	}
	_, err = tx.OpenTable(ctx, table2)
	if err != nil {
		t.Errorf("OpenTable(%s) failed with %s", table2, err)
	}
	err = tx.DropTable(ctx, table1)
	if err != nil {
		t.Errorf("DropTable(%s) failed with %s", table1, err)
	}
	err = tx.DropTable(ctx, table1)
	if err == nil {
		t.Errorf("DropTable(%s) did not fail", table1)
	}
	_, err = tx.OpenTable(ctx, table1)
	if err == nil {
		t.Errorf("OpenTable(%s) did not fail", table1)
	}
	err = tx.CreateTable(ctx, table1, colNames, colTypes, primary)
	if err != nil {
		t.Errorf("CreateTable(%s) failed with %s", table1, err)
	}
	_, err = tx.OpenTable(ctx, table1)
	if err != nil {
		t.Errorf("OpenTable(%s) failed with %s", table1, err)
	}

	tn := types.TableName{database2, types.ID("scm", false), types.ID("tbl1", false)}
	err = tx.CreateTable(ctx, tn, colNames, colTypes, primary)
	if err != nil {
		t.Errorf("CreateTable(%s) failed with %s", tn, err)
	}

	ids, err := tx.ListTables(ctx, schema)
	if err != nil {
		t.Errorf("ListTables(%s) failed with %s", schema, err)
	} else {
		want := "tbl1 tbl2"
		got := strings.Join(sortIdentifiers(ids), " ")
		if want != got {
			t.Errorf("ListTables(%s) got %s want %s", schema, got, want)
		}
	}
}

func checkIndex(t *testing.T, tx engine.Transaction, tn types.TableName, ids []types.Identifier) {
	ctx := context.Background()

	tbl, err := tx.OpenTable(ctx, tn)
	if err != nil {
		t.Errorf("OpenTable(%s) failed with %s", tn, err)
	}
	tt := tbl.Type()
	indexes := tt.Indexes()
	if len(indexes) != len(ids) {
		t.Errorf("TableType(%s).Indexes() got %d want %d", tn, len(indexes), len(ids))
	} else {
		for _, idx := range indexes {
			if !slices.Contains(ids, idx.Name()) {
				t.Errorf("TableType(%s).Indexes()[0].Name() got %s want %v", tn, idx.Name(), ids)
			}
			if !reflect.DeepEqual(idx.Key(), key) {
				t.Errorf("TableType(%s).Indexes().Key() got %v want %v", tn, idx.Key(), key)
			}
		}
	}
}

func TestIndex(t *testing.T) {
	eng := test.NewEngine(nil)

	err := eng.CreateDatabase(database, nil)
	if err != nil {
		t.Errorf("CreateDatabase(%s) failed with %s", database, err)
	}

	tx := eng.Begin()
	ctx := context.Background()

	err = tx.CreateSchema(ctx, schema)
	if err != nil {
		t.Errorf("CreateSchema(%s) failed with %s", schema, err)
	}

	err = tx.CreateTable(ctx, table1, colNames, colTypes, primary)
	if err != nil {
		t.Errorf("CreateTable(%s) failed with %s", table1, err)
	}

	err = tx.CreateIndex(ctx, table1, index1, key)
	if err != nil {
		t.Errorf("CreateIndex(%s, %s) failed with %s", table1, index1, err)
	}
	err = tx.CreateIndex(ctx, table1, index1, key)
	if err == nil {
		t.Errorf("CreateIndex(%s, %s) did not fail", table1, index1)
	}
	checkIndex(t, tx, table1, []types.Identifier{index1})

	err = tx.CreateIndex(ctx, table1, index2, key)
	if err != nil {
		t.Errorf("CreateIndex(%s, %s) failed with %s", table1, index2, err)
	}
	checkIndex(t, tx, table1, []types.Identifier{index1, index2})

	err = tx.DropIndex(ctx, table1, index1)
	if err != nil {
		t.Errorf("DropIndex(%s, %s) failed with %s", table1, index1, err)
	}
	checkIndex(t, tx, table1, []types.Identifier{index2})

	err = tx.DropIndex(ctx, table1, index2)
	if err != nil {
		t.Errorf("DropIndex(%s, %s) failed with %s", table1, index2, err)
	}
	checkIndex(t, tx, table1, nil)
}
