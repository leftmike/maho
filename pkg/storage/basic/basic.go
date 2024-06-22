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
	Version     uint32
	Name        types.TableName
	ColumnNames []types.Identifier
	ColumnTypes []types.ColumnType
	Primary     []types.ColumnKey
	HasPrimary  bool
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

func (tx *transaction) deleteTableType(tid storage.TableId) bool {
	it := toItem(tableTypesTID, tableTypesIID, tableTypesKey, types.Row{types.Int64Value(tid)})
	_, ok := tx.tree.Delete(it)
	return ok
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

func (tx *transaction) CreateTable(ctx context.Context, tid storage.TableId, tn types.TableName,
	colNames []types.Identifier, colTypes []types.ColumnType, primary []types.ColumnKey) error {

	if tid < storage.EngineTableId {
		panic(fmt.Sprintf("basic: tid too small: %d", tid))
	} else if tx.getTableType(tid) != nil {
		panic(fmt.Sprintf("basic: table already exists: %d", tid))
	} else if len(colNames) != len(colTypes) {
		panic(fmt.Sprintf("basic: column names doesn't match types: %#v %#v", colNames, colTypes))
	}

	hasPrimary := primary != nil
	if primary == nil {
		colNames = append(append(make([]types.Identifier, 0, len(colNames)+1), 0), colNames...)
		colTypes = append(append(make([]types.ColumnType, 0, len(colTypes)+1),
			types.Int64ColType), colTypes...)
		primary = []types.ColumnKey{types.MakeColumnKey(0, false)}
	} else {
		for _, ck := range primary {
			if int(ck.Column()) >= len(colNames) {
				panic(fmt.Sprintf("basic: primary key out of range: %d: %#v", ck.Column(),
					colNames))
			}
		}
	}

	tx.forWrite()
	tx.setTableType(tid, &tableType{
		Version:     1,
		Name:        tn,
		ColumnNames: colNames,
		ColumnTypes: colTypes,
		Primary:     primary,
		HasPrimary:  hasPrimary,
	})
	return nil
}

func (tx *transaction) DropTable(ctx context.Context, tid storage.TableId) error {
	if tx.getTableType(tid) == nil {
		panic(fmt.Sprintf("basic: table not found: %d", tid))
	}

	if !tx.deleteTableType(tid) {
		panic(fmt.Sprintf("basic: unable to delete table type: %d", tid))
	}

	// XXX: delete all rows in the table
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

func (tbl *table) Version() uint32 {
	return tbl.tt.Version
}

func (tbl *table) Name() types.TableName {
	return tbl.tt.Name
}

func (tbl *table) ColumnNames() []types.Identifier {
	return tbl.tt.ColumnNames
}

func (tbl *table) ColumnTypes() []types.ColumnType {
	return tbl.tt.ColumnTypes
}

func (tbl *table) Primary() []types.ColumnKey {
	return tbl.tt.Primary
}

func (tbl *table) Rows(ctx context.Context, cols []types.ColumnNum, minRow, maxRow types.Row,
	pred storage.Predicate) (storage.Rows, error) {

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
