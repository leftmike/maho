package engine

import (
	"github.com/leftmike/maho/pkg/storage"
	"github.com/leftmike/maho/pkg/types"
)

type engine struct {
}

func NewEngine(st storage.Store) *engine {
	return &engine{}
}

// XXX: Should LookupDatabase, CreateDatabase, DropDatabase, and ListDatabases take a transaction?
func (eng *engine) LookupDatabase(dbn types.Identifier) (bool, error) {
	// XXX
	return false, nil
}

func (eng *engine) CreateDatabase(dbn types.Identifier, opts storage.OptionsMap) error {
	// XXX
	return nil
}

func (eng *engine) DropDatabase(dbn types.Identifier, ifExists bool,
	opts storage.OptionsMap) error {

	// XXX
	return nil
}

func (eng *engine) ListDatabases() ([]types.Identifier, error) {
	// XXX
	return nil, nil
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
