package test

import (
	"context"
	"fmt"
	"io"

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
	// XXX
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
	if eng.trace != nil {
		fmt.Fprintf(eng.trace, "CreateDatabase(%s, %s)\n", dn, opts)
	}

	if _, ok := eng.databases[dn]; ok {
		return fmt.Errorf("engine: create database: database already exists: %s", dn)
	}

	eng.databases[dn] = struct{}{}
	return nil
}

func (eng *Engine) DropDatabase(dn types.Identifier, ifExists bool) error {
	if eng.trace != nil {
		fmt.Fprintf(eng.trace, "DropDatabase(%s, %v)\n", dn, ifExists)
	}

	if _, ok := eng.databases[dn]; !ok {
		if ifExists {
			return nil
		}

		return fmt.Errorf("engine: drop database: database not found: %s", dn)
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
	if tx.eng.trace != nil {
		fmt.Fprintf(tx.eng.trace, "CreateSchema(%s)\n", sn)
	}

	if _, ok := tx.eng.schemas[sn]; ok {
		return fmt.Errorf("engine: create schema: schema already exists: %s", sn)
	}

	tx.eng.schemas[sn] = struct{}{}
	return nil
}

func (tx *transaction) DropSchema(ctx context.Context, sn types.SchemaName, ifExists bool) error {
	if tx.eng.trace != nil {
		fmt.Fprintf(tx.eng.trace, "DropSchema(%s, %v)\n", sn, ifExists)
	}

	if _, ok := tx.eng.schemas[sn]; !ok {
		if ifExists {
			return nil
		}
		return fmt.Errorf("engine: drop schema: schema not found: %s", sn)
	}

	delete(tx.eng.schemas, sn)
	return nil
}

func (tx *transaction) ListSchemas(ctx context.Context, dn types.Identifier) ([]types.Identifier,
	error) {

	if tx.eng.trace != nil {
		fmt.Fprintf(tx.eng.trace, "ListSchemas(%s)\n", dn)
	}

	var ids []types.Identifier
	for sn := range tx.eng.schemas {
		if sn.Database == dn {
			ids = append(ids, sn.Schema)
		}
	}
	return ids, nil
}

func (tx *transaction) LookupTable(ctx context.Context, tn types.TableName) (engine.Table, error) {
	if tx.eng.trace != nil {
		fmt.Fprintf(tx.eng.trace, "LookupTable(%s)\n", tn)
	}

	tbl, ok := tx.eng.tables[tn]
	if !ok {
		return nil, fmt.Errorf("engine: lookup table: table not found: %s", tn)
	}

	return tbl, nil
}

func (tx *transaction) CreateTable(ctx context.Context, tn types.TableName,
	colNames []types.Identifier, colTypes []types.ColumnType) error {

	if tx.eng.trace != nil {
		fmt.Fprintf(tx.eng.trace, "CreateTable(%s, %v, %v)\n", tn, colNames, colTypes)
	}

	if _, ok := tx.eng.tables[tn]; ok {
		return fmt.Errorf("engine: create table: table already exists: %s", tn)
	}

	tx.eng.tables[tn] = &table{}
	return nil
}

func (tx *transaction) DropTable(ctx context.Context, tn types.TableName) error {
	if tx.eng.trace != nil {
		fmt.Fprintf(tx.eng.trace, "DropTable(%s)\n", tn)
	}

	if _, ok := tx.eng.tables[tn]; !ok {
		return fmt.Errorf("engine: drop table: table not found: %s", tn)
	}

	delete(tx.eng.tables, tn)
	return nil
}

func (tx *transaction) ListTables(ctx context.Context, sn types.SchemaName) ([]types.Identifier,
	error) {

	if tx.eng.trace != nil {
		fmt.Fprintf(tx.eng.trace, "ListTables(%s)\n", sn)
	}

	var ids []types.Identifier
	for tn := range tx.eng.tables {
		if tn.Database == sn.Database && tn.Schema == sn.Schema {
			ids = append(ids, tn.Table)
		}
	}
	return ids, nil
}
