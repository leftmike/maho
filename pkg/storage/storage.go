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

const (
	EngineTableId TableId = 16
)

type Transaction interface {
	OpenTable(ctx context.Context, tid TableId) (Table, error)
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
	Predicate(b types.BoolValue) bool
}

type StringPredicate interface {
	Predicate(s types.StringValue) bool
}

type BytesPredicate interface {
	Predicate(b types.BytesValue) bool
}

type Float64Predicate interface {
	Predicate(f types.Float64Value) bool
}

type Int64Predicate interface {
	Predicate(i types.Int64Value) bool
}

type RowId interface{}

type Table interface {
	TID() TableId
	Version() uint32
	Name() types.TableName
	ColumnNames() []types.Identifier
	ColumnTypes() []types.ColumnType
	Primary() []types.ColumnKey

	// XXX: AddColumn, DropColumn, UpdateColumn
	// XXX: AddIndex, RemoveIndex

	Rows(ctx context.Context, cols []types.ColumnNum, minRow, maxRow types.Row,
		pred Predicate) (Rows, error)
	Update(ctx context.Context, rid RowId, cols []types.ColumnNum, vals []types.Value) error
	Delete(ctx context.Context, rid RowId) error
	Insert(ctx context.Context, rows []types.Row) error
}

type Rows interface {
	Next(ctx context.Context) (types.Row, error)
	Current() (RowId, error)
	Close(ctx context.Context) error
}
