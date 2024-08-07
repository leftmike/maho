package engine

import (
	"context"

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
	Type() TableType
	// XXX
}

type TableType interface {
	Version() uint32
	ColumnNames() []types.Identifier
	ColumnTypes() []types.ColumnType
	Key() []types.ColumnKey
	ColumnDefaults() sql.Expr
	Indexes() []IndexType
}

type IndexType interface {
	Name() types.Identifier
	Key() []types.ColumnKey
}

type engine struct {
	st storage.Store
}

type transaction struct{}

func NewEngine(st storage.Store) Engine {
	return &engine{
		st: st,
	}
}

func (eng *engine) CreateDatabase(dn types.Identifier, opts storage.OptionsMap) error {
	// XXX
	return nil
}

func (eng *engine) DropDatabase(dn types.Identifier, ifExists bool) error {

	// XXX
	return nil
}

func (eng *engine) Begin() Transaction {
	return &transaction{}
}

func (tx *transaction) Commit(ctx context.Context) error {
	// XXX
	return nil
}

func (tx *transaction) Rollback() error {
	// XXX
	return nil
}

func (tx *transaction) CreateSchema(ctx context.Context, sn types.SchemaName) error {
	// XXX
	return nil
}

func (tx *transaction) DropSchema(ctx context.Context, sn types.SchemaName, ifExists bool) error {
	// XXX
	return nil
}

func (tx *transaction) ListSchemas(ctx context.Context, dn types.Identifier) ([]types.Identifier,
	error) {

	// XXX
	return nil, nil
}

func (tx *transaction) OpenTable(ctx context.Context, tn types.TableName) (Table, error) {
	// XXX
	return nil, nil
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
