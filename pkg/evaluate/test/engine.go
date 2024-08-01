package test

import (
	"context"
	"fmt"
	"io"
	"slices"

	"github.com/leftmike/maho/pkg/engine"
	"github.com/leftmike/maho/pkg/storage"
	"github.com/leftmike/maho/pkg/types"
)

type Engine struct {
	trace     io.Writer
	databases map[types.Identifier]struct{}
	schemas   map[types.SchemaName]struct{}
	tables    map[types.TableName]*table
	active    bool
}

type table struct {
	name      types.TableName
	tableType *tableType
}

type tableType struct {
	version  uint32
	colNames []types.Identifier
	colTypes []types.ColumnType
	primary  []types.ColumnKey
}

type transaction struct {
	eng *Engine
}

func NewEngine(trace io.Writer) *Engine {
	return &Engine{
		trace:     trace,
		databases: map[types.Identifier]struct{}{},
		schemas:   map[types.SchemaName]struct{}{},
		tables:    map[types.TableName]*table{},
	}
}

func (eng *Engine) CreateDatabase(dn types.Identifier, opts storage.OptionsMap) error {
	if _, ok := eng.databases[dn]; ok {
		return fmt.Errorf("engine: create database: database already exists: %s", dn)
	}

	if eng.trace != nil {
		fmt.Fprintf(eng.trace, "CreateDatabase(%s, %s)\n", dn, opts)
	}

	eng.databases[dn] = struct{}{}
	return nil
}

func (eng *Engine) DropDatabase(dn types.Identifier, ifExists bool) error {
	if _, ok := eng.databases[dn]; !ok {
		if ifExists {
			return nil
		}

		return fmt.Errorf("engine: drop database: database not found: %s", dn)
	}

	if eng.trace != nil {
		fmt.Fprintf(eng.trace, "DropDatabase(%s, %v)\n", dn, ifExists)
	}

	delete(eng.databases, dn)
	return nil
}

func (eng *Engine) Begin() engine.Transaction {
	if eng.trace != nil {
		fmt.Fprintln(eng.trace, "Begin()")
	}

	if eng.active {
		panic("test engine only allows one active transaction")
	}

	eng.active = true
	return &transaction{
		eng: eng,
	}
}

func (tx *transaction) Commit(ctx context.Context) error {
	if tx.eng.trace != nil {
		fmt.Fprintln(tx.eng.trace, "Commit()")
	}

	tx.eng.active = false
	return nil
}

func (tx *transaction) Rollback() error {
	if tx.eng.trace != nil {
		fmt.Fprintln(tx.eng.trace, "Rollback()")
	}

	tx.eng.active = false
	return nil
}

func (tx *transaction) CreateSchema(ctx context.Context, sn types.SchemaName) error {
	if _, ok := tx.eng.schemas[sn]; ok {
		return fmt.Errorf("engine: create schema: schema already exists: %s", sn)
	}

	if tx.eng.trace != nil {
		fmt.Fprintf(tx.eng.trace, "CreateSchema(%s)\n", sn)
	}

	tx.eng.schemas[sn] = struct{}{}
	return nil
}

func (tx *transaction) DropSchema(ctx context.Context, sn types.SchemaName, ifExists bool) error {
	if _, ok := tx.eng.schemas[sn]; !ok {
		if ifExists {
			return nil
		}
		return fmt.Errorf("engine: drop schema: schema not found: %s", sn)
	}

	if tx.eng.trace != nil {
		fmt.Fprintf(tx.eng.trace, "DropSchema(%s, %v)\n", sn, ifExists)
	}

	delete(tx.eng.schemas, sn)
	return nil
}

func (tx *transaction) ListSchemas(ctx context.Context, dn types.Identifier) ([]types.Identifier,
	error) {

	var ids []types.Identifier
	for sn := range tx.eng.schemas {
		if sn.Database == dn {
			ids = append(ids, sn.Schema)
		}
	}

	if tx.eng.trace != nil {
		fmt.Fprintf(tx.eng.trace, "ListSchemas(%s)\n", dn)
	}

	return ids, nil
}

func (tx *transaction) LookupTable(ctx context.Context, tn types.TableName) (engine.Table, error) {
	tbl, ok := tx.eng.tables[tn]
	if !ok {
		return nil, fmt.Errorf("engine: lookup table: table not found: %s", tn)
	}

	if tx.eng.trace != nil {
		fmt.Fprintf(tx.eng.trace, "LookupTable(%s)\n", tn)
	}

	return tbl, nil
}

func (tx *transaction) CreateTable(ctx context.Context, tn types.TableName,
	colNames []types.Identifier, colTypes []types.ColumnType, primary []types.ColumnKey) error {

	if _, ok := tx.eng.tables[tn]; ok {
		return fmt.Errorf("engine: create table: table already exists: %s", tn)
	}

	if tx.eng.trace != nil {
		fmt.Fprintf(tx.eng.trace, "CreateTable(%s, %v, %v, %v)\n", tn, colNames, colTypes, primary)
	}

	tx.eng.tables[tn] = &table{
		name: tn,
		tableType: &tableType{
			version:  1,
			colNames: slices.Clone(colNames),
			colTypes: slices.Clone(colTypes),
			primary:  slices.Clone(primary),
		},
	}
	return nil
}

func (tx *transaction) DropTable(ctx context.Context, tn types.TableName) error {
	if _, ok := tx.eng.tables[tn]; !ok {
		return fmt.Errorf("engine: drop table: table not found: %s", tn)
	}

	if tx.eng.trace != nil {
		fmt.Fprintf(tx.eng.trace, "DropTable(%s)\n", tn)
	}

	delete(tx.eng.tables, tn)
	return nil
}

func (tx *transaction) ListTables(ctx context.Context, sn types.SchemaName) ([]types.Identifier,
	error) {

	var ids []types.Identifier
	for tn := range tx.eng.tables {
		if tn.Database == sn.Database && tn.Schema == sn.Schema {
			ids = append(ids, tn.Table)
		}
	}

	if tx.eng.trace != nil {
		fmt.Fprintf(tx.eng.trace, "ListTables(%s)\n", sn)
	}

	return ids, nil
}

func (tbl *table) Name() types.TableName {
	return tbl.name
}

func (tbl *table) Type() engine.TableType {
	return tbl.tableType
}

func (tt *tableType) Version() uint32 {
	return tt.version
}

func (tt *tableType) ColumnNames() []types.Identifier {
	return tt.colNames
}

func (tt *tableType) ColumnTypes() []types.ColumnType {
	return tt.colTypes
}

func (tt *tableType) Key() []types.ColumnKey {
	return tt.primary
}

func (tt *tableType) Indexes() []engine.IndexType {
	return nil
}
