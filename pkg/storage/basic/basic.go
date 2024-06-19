package basic

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"sync"

	"github.com/google/btree"
	"github.com/leftmike/maho/pkg/storage"
	"github.com/leftmike/maho/pkg/types"
)

type store struct {
	mutex sync.Mutex
	tree  *btree.BTreeG[item]
}

type transaction struct {
	st   *store
	tree *btree.BTreeG[item]
}

type tableType struct {
	Version uint32
	// Name string
	ColumnNames []types.Identifier
	ColumnTypes []types.ColumnType
	// PrimaryKey []types.ColumnKey
}

type table struct {
	tx *transaction
	tt *tableType
}

type rows struct {
}

func NewStore(dataDir string) (storage.Store, error) {
	return &store{
		tree: newBTree(),
	}, nil
}

func (_ *store) Name() string {
	return "basic"
}

func (st *store) Begin() storage.Transaction {
	st.mutex.Lock()
	return &transaction{
		st:   st,
		tree: st.tree,
	}
}

var (
	tableTypesTID storage.TableId = 0
	tableTypesIID storage.IndexId = 0
	tableTypesKey                 = []types.ColumnKey{types.MakeColumnKey(0, false)}
)

func (tx *transaction) getTableType(tid storage.TableId) *tableType {
	it := toItem(tableTypesTID, tableTypesIID, tableTypesKey, types.Row{types.Int64Value(tid)})
	it, ok := tx.tree.Get(it)
	if !ok {
		return nil
	}

	if len(it.row) == 2 {
		if b, ok := it.row[1].(types.BytesValue); ok {
			var tt tableType
			err := gob.NewDecoder(bytes.NewReader(b)).Decode(&tt)
			if err != nil {
				panic(fmt.Sprintf("basic: unabled to decode table type: %s: %v", err, b))
			}
			return &tt
		}
	}

	panic(fmt.Sprintf("basic: table types row must by integer and bytes columns: %s", it.row))
}

func (tx *transaction) setTableType(tid storage.TableId, tt *tableType) {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(tt)
	if err != nil {
		panic(fmt.Sprintf("basic: unable able encode type table: %s", err))
	}

	it := toItem(tableTypesTID, tableTypesIID, tableTypesKey,
		types.Row{types.Int64Value(tid), types.BytesValue(buf.Bytes())})
	tx.tree.ReplaceOrInsert(it)
}

func (tx *transaction) OpenTable(ctx context.Context, tid storage.TableId) (storage.Table,
	error) {

	tt := tx.getTableType(tid)
	if tt == nil {
		panic(fmt.Sprintf("basic: table not found: %d", tid))
	}

	return &table{
		tx: tx,
		tt: tt,
	}, nil
}

// XXX: add optional primary key
func (tx *transaction) CreateTable(ctx context.Context, tid storage.TableId,
	colNames []types.Identifier, colTypes []types.ColumnType) error {

	if tx.getTableType(tid) != nil {
		panic(fmt.Sprintf("basic: table already exists: %d", tid))
	}

	// XXX: if no primary key, add a unique column and use that for the primary key
	tx.forWrite()
	tx.setTableType(tid, &tableType{
		Version:     1,
		ColumnNames: colNames,
		ColumnTypes: colTypes,
		// PrimaryKey: pkey,
		// HasPrimaryKey:
	})
	return nil
}

func (tx *transaction) DropTable(ctx context.Context, tid storage.TableId) error {
	// XXX
	return nil
}

func (tx *transaction) Commit(ctx context.Context) error {
	if tx.st == nil {
		return errors.New("basic: transaction already completed")
	}

	tx.st.tree = tx.tree
	tx.st.mutex.Unlock()
	tx.st = nil
	tx.tree = nil
	return nil
}

func (tx *transaction) Rollback() error {
	if tx.st == nil {
		return errors.New("basic: transaction already completed")
	}

	tx.st.mutex.Unlock()
	tx.st = nil
	tx.tree = nil
	return nil
}

func (tx *transaction) NextStmt() {
	// Nothing.
}

func (tx *transaction) forWrite() {
	if tx.tree == tx.st.tree {
		tx.tree = tx.st.tree.Clone()
	}
}

func (tbl *table) Rows(ctx context.Context, cols []types.ColumnNum, minRow, maxRow types.Row,
	pred storage.PredicateFn) (storage.Rows, error) {

	// XXX
	return &rows{}, nil
}

func (tbl *table) Update(ctx context.Context, rid storage.RowId, cols []types.ColumnNum,
	vals types.Row) error {

	// XXX
	return nil
}

func (tbl *table) Delete(ctx context.Context, rid storage.RowId) error {
	// XXX
	return nil
}

func (tbl *table) Insert(ctx context.Context, rows []types.Row) error {
	// XXX
	return nil
}

func (rs *rows) Next(ctx context.Context, row types.Row) (types.Row, error) {
	// XXX
	return nil, nil
}

func (rs *rows) Current(ctx context.Context) (storage.RowId, error) {
	// XXX
	return nil, nil
}

func (rs *rows) Close(ctx context.Context) error {
	// XXX
	return nil
}
