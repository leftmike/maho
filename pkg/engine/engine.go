package engine

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"io"

	"github.com/leftmike/maho/pkg/parser/sql"
	"github.com/leftmike/maho/pkg/storage"
	"github.com/leftmike/maho/pkg/types"
)

type Engine interface {
	CreateDatabase(dn types.Identifier, opts storage.OptionsMap) error
	DropDatabase(dn types.Identifier, ifExists bool) error
	Begin() Transaction
}

type Transaction interface {
	Commit(ctx context.Context) error
	Rollback() error

	CreateSchema(ctx context.Context, sn types.SchemaName) error
	DropSchema(ctx context.Context, sn types.SchemaName, ifExists bool) error
	ListSchemas(ctx context.Context, dn types.Identifier) ([]types.Identifier, error)

	OpenTable(ctx context.Context, tn types.TableName) (Table, error)
	CreateTable(ctx context.Context, tn types.TableName, colNames []types.Identifier,
		colTypes []types.ColumnType, primary []types.ColumnKey) error
	DropTable(ctx context.Context, tn types.TableName) error
	ListTables(ctx context.Context, sn types.SchemaName) ([]types.Identifier, error)

	CreateIndex(ctx context.Context, tn types.TableName, in types.Identifier,
		key []types.ColumnKey) error
	DropIndex(ctx context.Context, tn types.TableName, in types.Identifier) error
}

type Table interface {
	Name() types.TableName
	Type() *TableType
	// XXX
}

type TableType struct {
	Version        uint32
	ColumnNames    []types.Identifier
	ColumnTypes    []types.ColumnType
	Key            []types.ColumnKey
	ColumnDefaults []sql.Expr
	Indexes        []IndexType
}

type IndexType struct {
	Name types.Identifier
	Key  []types.ColumnKey
}

var (
	errTransactionComplete = errors.New("engine: transaction already completed")
)

type engine struct {
	store storage.Store
}

type transaction struct {
	tx storage.Transaction
}

const (
	sequencesTableId storage.TableId = iota + storage.EngineTableId
	databasesTableId
	schemasTableId
	tablesTableId

	maxReservedTableId storage.TableId = 511
)

func NewEngine(store storage.Store) Engine {
	return &engine{
		store: store,
	}
}

var (
	sequencesTableName = types.TableName{types.SYSTEM, types.INFO, types.SEQUENCES}
	sequencesTypedInfo = MakeTypedInfo(sequencesTableId, sequencesTableName, sequencesRow{})
	databasesTableName = types.TableName{types.SYSTEM, types.INFO, types.DATABASES}
	databasesTypedInfo = MakeTypedInfo(databasesTableId, databasesTableName, databasesRow{})
	schemasTableName   = types.TableName{types.SYSTEM, types.INFO, types.SCHEMAS}
	schemasTypedInfo   = MakeTypedInfo(schemasTableId, schemasTableName, schemasRow{})
	tablesTableName    = types.TableName{types.SYSTEM, types.INFO, types.TABLES}
	tablesTypedInfo    = MakeTypedInfo(tablesTableId, tablesTableName, tablesRow{})
)

type sequencesRow struct {
	Sequence string `maho:"size=128,primary"`
	Current  int64
}

type databasesRow struct {
	Database string `maho:"size=128,primary"`
}

type schemasRow struct {
	Database string `maho:"size=128,primary"`
	Schema   string `maho:"size=128,primary"`
}

type tablesRow struct {
	Database string `maho:"size=128,primary"`
	Schema   string `maho:"size=128,primary"`
	Table    string `maho:"size=128,primary"`
	TableId  int64  // storage.TableId
	Type     []byte `maho:"size=8192,notnull"`
}

func (eng *engine) CreateDatabase(dn types.Identifier, opts storage.OptionsMap) error {
	for key := range opts {
		return fmt.Errorf("engine: unexpected option: %s", key)
	}

	tx := eng.store.Begin()
	ctx := context.Background()
	err := TypedTableInsert(ctx, tx, databasesTypedInfo,
		&databasesRow{
			Database: dn.String(),
		})
	if err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit(ctx)
}

func (eng *engine) DropDatabase(dn types.Identifier, ifExists bool) error {
	// XXX
	return nil
}

// XXX: ListDatabases

func (eng *engine) Begin() Transaction {
	return &transaction{
		tx: eng.store.Begin(),
	}
}

func (tx *transaction) Commit(ctx context.Context) error {
	if tx.tx == nil {
		return errTransactionComplete
	}
	err := tx.tx.Commit(ctx)
	tx.tx = nil
	return err
}

func (tx *transaction) Rollback() error {
	if tx.tx == nil {
		return errTransactionComplete
	}
	err := tx.tx.Rollback()
	tx.tx = nil
	return err
}

func (tx *transaction) CreateSchema(ctx context.Context, sn types.SchemaName) error {
	err := TypedTableLookup(ctx, tx.tx, databasesTypedInfo,
		&databasesRow{
			Database: sn.Database.String(),
		})
	if err == io.EOF {
		return fmt.Errorf("engine: database not found: %s", sn.Database)
	} else if err != nil {
		return err
	}

	err = TypedTableInsert(ctx, tx.tx, schemasTypedInfo,
		&schemasRow{
			Database: sn.Database.String(),
			Schema:   sn.Schema.String(),
		})
	if err != nil {
		return err
	}

	return nil
}

func (tx *transaction) DropSchema(ctx context.Context, sn types.SchemaName, ifExists bool) error {
	// XXX
	return nil
}

func (tx *transaction) ListSchemas(ctx context.Context, dn types.Identifier) ([]types.Identifier,
	error) {

	err := TypedTableLookup(ctx, tx.tx, databasesTypedInfo,
		&databasesRow{
			Database: dn.String(),
		})
	if err == io.EOF {
		return nil, fmt.Errorf("engine: database not found: %s", dn)
	} else if err != nil {
		return nil, err
	}

	var schemas []types.Identifier
	err = TypedTableSelect(ctx, tx.tx, schemasTypedInfo,
		&schemasRow{
			Database: dn.String(),
		}, nil, func(row types.Row) error {
			var sr schemasRow
			schemasTypedInfo.RowToStruct(row, &sr)

			if sr.Database != dn.String() {
				return io.EOF
			}

			schemas = append(schemas, types.ID(sr.Schema, true))
			return nil
		})
	if err != nil {
		return nil, err
	}
	return schemas, nil
}

func (tx *transaction) OpenTable(ctx context.Context, tn types.TableName) (Table, error) {
	// XXX
	return nil, nil
}

func (tt *TableType) Encode() ([]byte, error) {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(tt)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func DecodeTableType(buf []byte) (*TableType, error) {
	var tt TableType
	err := gob.NewDecoder(bytes.NewReader(buf)).Decode(&tt)
	if err != nil {
		return nil, err
	}

	return &tt, nil
}

func (tx *transaction) CreateTable(ctx context.Context, tn types.TableName,
	colNames []types.Identifier, colTypes []types.ColumnType, primary []types.ColumnKey) error {

	// XXX
	return nil
}

func (tx *transaction) DropTable(ctx context.Context, tn types.TableName) error {
	// XXX
	return nil
}

func (tx *transaction) ListTables(ctx context.Context, sn types.SchemaName) ([]types.Identifier,
	error) {

	// XXX
	return nil, nil
}

func (tx *transaction) CreateIndex(ctx context.Context, tn types.TableName, in types.Identifier,
	key []types.ColumnKey) error {

	// XXX
	return nil
}

func (tx *transaction) DropIndex(ctx context.Context, tn types.TableName,
	in types.Identifier) error {

	// XXX
	return nil
}
