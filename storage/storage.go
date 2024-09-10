package storage

import (
	"context"

	"github.com/leftmike/maho/types"
)

type OptionsMap map[types.Identifier]string

type Store interface {
	Name() string
	SetupColumns(colNames []types.Identifier, colTypes []types.ColumnType,
		primary []types.ColumnKey) ([]types.Identifier, []types.ColumnType, []types.ColumnKey)
	Begin() Transaction
}

type TableId uint32
type IndexId uint32

const (
	EngineTableId TableId = 16
)

type Transaction interface {
	Store() Store
	OpenTable(ctx context.Context, tid TableId, tn types.TableName, colNames []types.Identifier,
		colTypes []types.ColumnType, primary []types.ColumnKey) (Table, error)
	CreateTable(ctx context.Context, tid TableId, tn types.TableName,
		colNames []types.Identifier, colTypes []types.ColumnType, primary []types.ColumnKey) error
	DropTable(ctx context.Context, tid TableId) error

	Commit(ctx context.Context) error
	Rollback() error
	NextStmt()
}

type Predicate interface {
	Column() types.ColumnNum
}

type BoolPredicate interface {
	BoolPred(b types.BoolValue) bool
}

type StringPredicate interface {
	StringPred(s types.StringValue) bool
}

type BytesPredicate interface {
	BytesPred(b types.BytesValue) bool
}

type Float64Predicate interface {
	Float64Pred(f types.Float64Value) bool
}

type Int64Predicate interface {
	Int64Pred(i types.Int64Value) bool
}

type Table interface {
	TID() TableId
	Name() types.TableName

	Version() uint32
	ColumnNames() []types.Identifier
	ColumnTypes() []types.ColumnType
	Key() []types.ColumnKey

	// XXX: AddColumn, DropColumn, UpdateColumn
	// XXX: CreateIndex, DropIndex

	Rows(ctx context.Context, cols []types.ColumnNum, minRow, maxRow types.Row,
		pred Predicate) (Rows, error)
	Insert(ctx context.Context, rows []types.Row) error
}

type Rows interface {
	Next(ctx context.Context) (types.Row, error)
	Current() (RowRef, error)
	Close(ctx context.Context) error
}

type RowRef interface {
	Update(ctx context.Context, cols []types.ColumnNum, vals []types.Value) error
	Delete(ctx context.Context) error
}
