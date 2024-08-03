package evaluate_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"reflect"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/leftmike/maho/pkg/engine"
	"github.com/leftmike/maho/pkg/evaluate"
	"github.com/leftmike/maho/pkg/parser"
	"github.com/leftmike/maho/pkg/parser/sql"
	"github.com/leftmike/maho/pkg/testutil"
	"github.com/leftmike/maho/pkg/types"
)

var (
	db = types.ID("db", false)
	sn = types.SchemaName{
		Database: db,
		Schema:   types.ID("sn", false),
	}
)

func mustParse(s string) sql.Stmt {
	p := parser.NewParser(strings.NewReader(s), "")
	stmt, err := p.Parse()
	if err != nil {
		panic(fmt.Sprintf("must parse failed: %s: %s", err, s))
	}
	return stmt
}

func TestEvaluatePanic(t *testing.T) {
	cases := []struct {
		stmt     sql.Stmt
		panicked bool
		fail     bool
	}{
		{
			stmt:     mustParse("begin"),
			panicked: true,
		},
		{
			stmt:     mustParse("commit"),
			panicked: true,
		},
		{
			stmt:     mustParse("rollback"),
			panicked: true,
		},
		{
			stmt:     mustParse("set database = 'test'"),
			panicked: true,
		},
		{
			stmt:     mustParse("create database db"),
			panicked: true,
		},
		{
			stmt:     mustParse("drop database db"),
			panicked: true,
		},
	}

	ctx := context.Background()
	for _, c := range cases {
		err, panicked := testutil.ErrorPanicked(func() error {
			return evaluate.Evaluate(ctx, nil, c.stmt)
		})
		if panicked {
			if !c.panicked {
				t.Errorf("Evaluate(%s) panicked", c.stmt)
			}
		} else if c.panicked {
			t.Errorf("Evaluate(%s) did not panic", c.stmt)
		} else if err != nil {
			if !c.fail {
				t.Errorf("Evaluate(%s) failed with %s", c.stmt, err)
			}
		} else if c.fail {
			t.Errorf("Evaluate(%s) did not fail", c.stmt)
		}
	}
}

func testListTables(t *testing.T, tx engine.Transaction, sn types.SchemaName,
	ids []types.Identifier) {

	ret, err := tx.ListTables(context.Background(), sn)
	if err != nil {
		t.Errorf("ListTables(%s) failed with %s", sn, err)
	} else if !reflect.DeepEqual(ids, ret) {
		t.Errorf("ListTables(%s) got %v want %v", sn, ret, ids)
	}
}

func TestEvaluateCreateTable(t *testing.T) {
	var buf bytes.Buffer
	tx := newEvalTx(&buf)

	cases := []evaluateCase{
		{
			stmt:  mustParse("create schema sn"),
			trace: "CreateSchema(db.sn)",
		},
		{
			stmt: mustParse("create table t1 (c1 int, c2 bool)"),
			trace: `OpenTable(db.sn.t1)
CreateTable(db.sn.t1, [c1 c2], [INT BOOL], [])`,
		},
		{
			stmt:  mustParse("create table t1 (c1 int, c2 bool)"),
			trace: "OpenTable(db.sn.t1)",
			fail:  true,
		},
		{
			stmt: mustParse("create table t2 (c1 int primary key, c2 bool)"),
			trace: `OpenTable(db.sn.t2)
CreateTable(db.sn.t2, [c1 c2], [INT BOOL], [1])`,
		},
		{
			stmt: mustParse("create table t3 (c1 int, c2 bool, primary key(c2, c1))"),
			trace: `OpenTable(db.sn.t3)
CreateTable(db.sn.t3, [c1 c2], [INT BOOL], [2 1])`,
		},
		{
			stmt: mustParse("create table t4 (c1 int, c2 bool, primary key(c2, c1 desc))"),
			trace: `OpenTable(db.sn.t4)
CreateTable(db.sn.t4, [c1 c2], [INT BOOL], [2 -1])`,
		},
		{
			stmt:  mustParse("create table tf (c1 int, c2 bool, primary key(c2, c3))"),
			trace: "OpenTable(db.sn.tf)",
			fail:  true,
		},
		{
			fn: func() {
				testListTables(t, tx, sn, []types.Identifier{
					types.ID("t1", false),
					types.ID("t2", false),
					types.ID("t3", false),
					types.ID("t4", false),
				})
			},
			trace: "ListTables(db.sn)",
		},
		{
			stmt:  mustParse("drop table t1"),
			trace: "DropTable(db.sn.t1)",
		},
		{
			fn: func() {
				testListTables(t, tx, sn, []types.Identifier{
					types.ID("t2", false),
					types.ID("t3", false),
					types.ID("t4", false),
				})
			},
			trace: "ListTables(db.sn)",
		},
		{
			stmt:  mustParse("drop table t3"),
			trace: "DropTable(db.sn.t3)",
		},
		{
			fn: func() {
				testListTables(t, tx, sn, []types.Identifier{
					types.ID("t2", false),
					types.ID("t4", false),
				})
			},
			trace: "ListTables(db.sn)",
		},
	}

	r := resolver{
		Database: types.ID("db", false),
		Schema:   types.ID("sn", false),
	}

	ctx := context.Background()
	for _, c := range cases {
		if c.fn != nil {
			if c.stmt != nil {
				panic("must specify one of fn or stmt")
			}

			c.fn()
			continue
		}

		c.stmt.Resolve(r)

		err := evaluate.Evaluate(ctx, tx, c.stmt)
		if err != nil {
			if !c.fail {
				t.Errorf("Evaluate(%s) failed with %s", c.stmt, err)
			}
		} else if c.fail {
			t.Errorf("Evaluate(%s) did not fail", c.stmt)
		}
	}

	got := buf.String()
	want := evaluateTrace(cases)
	if got != want {
		t.Errorf("Evaluate() got %s want %s", got, want)
	}
}

type resolver struct {
	Database types.Identifier
	Schema   types.Identifier
}

func (r resolver) ResolveTable(tn types.TableName) types.TableName {
	if tn.Database == 0 {
		tn.Database = r.Database
		if tn.Schema == 0 {
			tn.Schema = r.Schema
		}
	}
	return tn
}

func (r resolver) ResolveSchema(sn types.SchemaName) types.SchemaName {
	if sn.Database == 0 {
		sn.Database = r.Database
	}
	return sn
}

type evalTx struct {
	trace   io.Writer
	schemas map[types.SchemaName]struct{}
	tables  map[types.TableName]*evalTable
}

type evalTable struct {
	name     types.TableName
	version  uint32
	colNames []types.Identifier
	colTypes []types.ColumnType
	primary  []types.ColumnKey
	indexes  map[types.Identifier]*evalIndex
}

type evalIndex struct {
	name types.Identifier
	key  []types.ColumnKey
}

func newEvalTx(trace io.Writer) *evalTx {
	return &evalTx{
		trace:   trace,
		schemas: map[types.SchemaName]struct{}{},
		tables:  map[types.TableName]*evalTable{},
	}
}

func (tx *evalTx) Commit(ctx context.Context) error {
	fmt.Fprintln(tx.trace, "Commit()")
	return nil
}

func (tx *evalTx) Rollback() error {
	fmt.Fprintln(tx.trace, "Rollback()")
	return nil
}

func (tx *evalTx) CreateSchema(ctx context.Context, sn types.SchemaName) error {
	fmt.Fprintf(tx.trace, "CreateSchema(%s)\n", sn)

	if _, ok := tx.schemas[sn]; ok {
		return fmt.Errorf("create schema: schema already exists: %s", sn)
	}
	tx.schemas[sn] = struct{}{}
	return nil
}

func (tx *evalTx) DropSchema(ctx context.Context, sn types.SchemaName, ifExists bool) error {
	fmt.Fprintf(tx.trace, "DropSchema(%s, %v)\n", sn, ifExists)

	if _, ok := tx.schemas[sn]; !ok {
		if ifExists {
			return nil
		}
		return fmt.Errorf("drop schema: schema not found: %s", sn)
	}
	delete(tx.schemas, sn)
	return nil
}

func (tx *evalTx) ListSchemas(ctx context.Context, dn types.Identifier) ([]types.Identifier,
	error) {

	fmt.Fprintf(tx.trace, "ListSchemas(%s)\n", dn)

	var ids []types.Identifier
	for sn := range tx.schemas {
		if sn.Database == dn {
			ids = append(ids, sn.Schema)
		}
	}

	sort.Slice(ids,
		func(i, j int) bool {
			return strings.Compare(ids[i].String(), ids[j].String()) < 0
		})
	return ids, nil
}

func (tx *evalTx) OpenTable(ctx context.Context, tn types.TableName) (engine.Table, error) {
	fmt.Fprintf(tx.trace, "OpenTable(%s)\n", tn)

	tbl, ok := tx.tables[tn]
	if !ok {
		return nil, fmt.Errorf("lookup table: table not found: %s", tn)
	}
	return tbl, nil
}

func (tx *evalTx) CreateTable(ctx context.Context, tn types.TableName,
	colNames []types.Identifier, colTypes []types.ColumnType, primary []types.ColumnKey) error {

	fmt.Fprintf(tx.trace, "CreateTable(%s, %v, %v, %v)\n", tn, colNames, colTypes, primary)

	if _, ok := tx.tables[tn]; ok {
		return fmt.Errorf("create table: table already exists: %s", tn)
	}
	tx.tables[tn] = &evalTable{
		name:     tn,
		version:  1,
		colNames: slices.Clone(colNames),
		colTypes: slices.Clone(colTypes),
		primary:  slices.Clone(primary),
		indexes:  map[types.Identifier]*evalIndex{},
	}
	return nil
}

func (tx *evalTx) DropTable(ctx context.Context, tn types.TableName) error {
	fmt.Fprintf(tx.trace, "DropTable(%s)\n", tn)

	if _, ok := tx.tables[tn]; !ok {
		return fmt.Errorf("drop table: table not found: %s", tn)
	}
	delete(tx.tables, tn)
	return nil
}

func (tx *evalTx) ListTables(ctx context.Context, sn types.SchemaName) ([]types.Identifier,
	error) {

	fmt.Fprintf(tx.trace, "ListTables(%s)\n", sn)

	var ids []types.Identifier
	for tn := range tx.tables {
		if tn.Database == sn.Database && tn.Schema == sn.Schema {
			ids = append(ids, tn.Table)
		}
	}

	sort.Slice(ids,
		func(i, j int) bool {
			return strings.Compare(ids[i].String(), ids[j].String()) < 0
		})
	return ids, nil
}

func (tx *evalTx) CreateIndex(ctx context.Context, tn types.TableName, in types.Identifier,
	key []types.ColumnKey) error {

	fmt.Fprintf(tx.trace, "CreateIndex(%s, %s, %v)\n", tn, in, key)

	tbl, ok := tx.tables[tn]
	if !ok {
		return fmt.Errorf("create index: table not found: %s", tn)
	}
	if _, ok := tbl.indexes[in]; ok {
		return fmt.Errorf("create index: index already exists: %s: %s", tn, in)
	}
	tbl.indexes[in] = &evalIndex{
		name: in,
		key:  key,
	}
	return nil
}

func (tx *evalTx) DropIndex(ctx context.Context, tn types.TableName,
	in types.Identifier) error {

	fmt.Fprintf(tx.trace, "DropIndex(%s, %s)\n", tn, in)

	tbl, ok := tx.tables[tn]
	if !ok {
		return fmt.Errorf("drop index: table not found: %s", tn)
	}
	if _, ok := tbl.indexes[in]; !ok {
		return fmt.Errorf("drop index: index not found: %s: %s", tn, in)
	}
	delete(tbl.indexes, in)
	return nil
}

func (tbl *evalTable) Name() types.TableName {
	return tbl.name
}

func (tbl *evalTable) Type() engine.TableType {
	return tbl
}

func (tbl *evalTable) Version() uint32 {
	return tbl.version
}

func (tbl *evalTable) ColumnNames() []types.Identifier {
	return tbl.colNames
}

func (tbl *evalTable) ColumnTypes() []types.ColumnType {
	return tbl.colTypes
}

func (tbl *evalTable) Key() []types.ColumnKey {
	return tbl.primary
}

func (tbl *evalTable) ColumnDefaults() sql.Expr {
	return nil // XXX
}

func (tbl *evalTable) Indexes() []engine.IndexType {
	indexes := make([]engine.IndexType, 0, len(tbl.indexes))
	for _, it := range tbl.indexes {
		indexes = append(indexes, it)
	}

	sort.Slice(indexes,
		func(i, j int) bool {
			return strings.Compare(indexes[i].Name().String(), indexes[j].Name().String()) < 0
		})
	return indexes
}

func (it *evalIndex) Name() types.Identifier {
	return it.name
}

func (it *evalIndex) Key() []types.ColumnKey {
	return it.key
}
