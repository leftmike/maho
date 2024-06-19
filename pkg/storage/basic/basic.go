package basic

import (
	"context"
	"sync"

	"github.com/leftmike/maho/pkg/storage"
	"github.com/leftmike/maho/pkg/types"
)

type store struct {
	mutex sync.Mutex
}

type transaction struct {
}

type table struct {
}

type rows struct {
}

func NewStore(dataDir string) (storage.Store, error) {
	// XXX
	return &store{}, nil
}

func (_ *store) Name() string {
	return "basic"
}

func (bst *store) Begin() storage.Transaction {
	// XXX
	return &transaction{}
}

func (btx *transaction) OpenTable(ctx context.Context, tid storage.TableId) (storage.Table,
	error) {

	// XXX
	return nil, nil
}

func (btx *transaction) CreateTable(ctx context.Context, tid storage.TableId,
	colNames []types.Identifier, colTypes []types.ColumnType) error {

	// XXX
	return nil
}

func (btx *transaction) DropTable(ctx context.Context, tid storage.TableId) error {
	// XXX
	return nil
}

func (btx *transaction) Commit(ctx context.Context) error {
	// XXX
	return nil
}

func (btx *transaction) Rollback() error {
	// XXX
	return nil
}

func (btx *transaction) NextStmt() {
	// XXX
}

func (bt *table) Rows(ctx context.Context, cols []types.ColumnNum, minRow, maxRow []types.Value,
	pred storage.PredicateFn) (storage.Rows, error) {

	// XXX
	return &rows{}, nil
}

func (bt *table) Update(ctx context.Context, rid storage.RowId, cols []types.ColumnNum,
	vals []types.Value) error {

	// XXX
	return nil
}

func (bt *table) Delete(ctx context.Context, rid storage.RowId) error {
	// XXX
	return nil
}

func (bt *table) Insert(ctx context.Context, rows [][]types.Value) error {
	// XXX
	return nil
}

func (br *rows) Next(ctx context.Context, row []types.Value) ([]types.Value, error) {
	// XXX
	return nil, nil
}

func (br *rows) Current(ctx context.Context) (storage.RowId, error) {
	// XXX
	return nil, nil
}

func (br *rows) Close(ctx context.Context) error {
	// XXX
	return nil
}
