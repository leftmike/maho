package engine

import (
	"context"

	"github.com/leftmike/maho/pkg/storage"
	"github.com/leftmike/maho/pkg/types"
)

type Engine interface {
	CreateDatabase(dbn types.Identifier, opts storage.OptionsMap) error
	DropDatabase(dbn types.Identifier, ifExists bool, opts storage.OptionsMap) error
	Begin() Transaction
}

type Transaction interface {
	Commit(ctx context.Context) error
	Rollback() error
}

type engine struct {
	st storage.Store
}

type transaction struct {
}

func NewEngine(st storage.Store) Engine {
	return &engine{
		st: st,
	}
}

// XXX: Should CreateDatabase and DropDatabase take a transaction?
func (eng *engine) CreateDatabase(dbn types.Identifier, opts storage.OptionsMap) error {
	// XXX
	return nil
}

func (eng *engine) DropDatabase(dbn types.Identifier, ifExists bool,
	opts storage.OptionsMap) error {

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

/*
	CreateSchema(ctx context.Context, sn types.SchemaName) error
	DropSchema(ctx context.Context, ifExists bool, sn types.SchemaName) error
	ListSchemas(ctx context.Context, dbn types.Identifier) ([]types.Identifier, error)

	LookupTable(ctx context.Context, tn types.TableName) (Table, error)
	CreateTable(ctx context.Context, tn types.TableName, colNames []types.Identifier,
		colTypes []types.ColumnType) error
	DropTable(ctx context.Context, tn types.TableName) error
	ListTables(ctx context.Context, sn types.SchemaName) ([]types.Identifier, error)
*/
