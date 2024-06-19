package storage

import (
	"context"

	"github.com/leftmike/maho/pkg/types"
)

type OptionsMap map[types.Identifier]string

type Store interface {
	Name() string
	Begin() Transaction
}

type TableId uint32
type IndexId uint32

type Transaction interface {
	OpenTable(ctx context.Context, tid TableId) (Table, error)
	CreateTable(ctx context.Context, tid TableId, colNames []types.Identifier,
		colTypes []types.ColumnType) error
	DropTable(ctx context.Context, tid TableId) error

	Commit(ctx context.Context) error
	Rollback() error
	NextStmt()
}

type PredicateFn func(row []types.Value) (bool, error)
type RowId interface{}

type Table interface {
	// XXX: TypeVersion() uint32
	// XXX: ColumnNames() []types.Identifier
	// XXX: ColumnTypes() []types.ColumnType

	// XXX: AddColumn, DropColumn, UpdateColumn
	// XXX: AddIndex, RemoveIndex

	Rows(ctx context.Context, cols []types.ColumnNum, minRow, maxRow []types.Value,
		pred PredicateFn) (Rows, error)
	Update(ctx context.Context, rid RowId, cols []types.ColumnNum, vals []types.Value) error
	Delete(ctx context.Context, rid RowId) error
	Insert(ctx context.Context, rows [][]types.Value) error
}

type Rows interface {
	Next(ctx context.Context, row []types.Value) ([]types.Value, error)
	Current(ctx context.Context) (RowId, error)
	Close(ctx context.Context) error
}
