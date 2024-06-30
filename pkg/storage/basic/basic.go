package basic

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/google/btree"
	"github.com/leftmike/maho/pkg/storage"
	"github.com/leftmike/maho/pkg/types"
)

const (
	primaryIndexId = 0
)

type store struct {
	mutex sync.Mutex
	tree  *btree.BTreeG[item]
}

type transaction struct {
	st        *store
	tree      *btree.BTreeG[item]
	rowsCount int
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
	tx  *transaction
	tid storage.TableId
	tt  *tableType
}

type rows struct {
	tbl   *table
	cols  []types.ColumnNum
	items []item
	next  int
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
	tableTypesRelation relationId = toRelationId(0, 0)
	tableTypesKey                 = []types.ColumnKey{types.MakeColumnKey(0, false)}
)

func (tx *transaction) getTableType(tid storage.TableId) *tableType {
	it := rowToItem(tableTypesRelation, tableTypesKey, types.Row{types.Int64Value(tid)})
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

	it := rowToItem(tableTypesRelation, tableTypesKey,
		types.Row{types.Int64Value(tid), types.BytesValue(buf.Bytes())})
	tx.tree.ReplaceOrInsert(it)
}

func (tx *transaction) deleteTableType(tid storage.TableId) bool {
	it := rowToItem(tableTypesRelation, tableTypesKey, types.Row{types.Int64Value(tid)})
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
		tx:  tx,
		tid: tid,
		tt:  tt,
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

	tx.forWrite()
	if !tx.deleteTableType(tid) {
		panic(fmt.Sprintf("basic: unable to delete table type: %d", tid))
	}

	// XXX: delete all rows in the table
	return nil
}

func (tx *transaction) Commit(ctx context.Context) error {
	if tx.st == nil {
		return errors.New("basic: transaction already completed")
	} else if tx.rowsCount != 0 {
		panic(fmt.Sprintf("basic: commit transaction has open rows: %d", tx.rowsCount))
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
	} else if tx.rowsCount != 0 {
		panic(fmt.Sprintf("basic: rollback transaction has open rows: %d", tx.rowsCount))
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

func (tbl *table) TID() storage.TableId {
	return tbl.tid
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

func predicateFunction(pred storage.Predicate, ct types.ColumnType) func(types.Value) bool {
	switch ct.Type {
	case types.UnknownType:
		panic("unexpected column type: unknown")
	case types.BoolType:
		boolPred := pred.(storage.BoolPredicate)
		return func(val types.Value) bool {
			return boolPred.BoolPred(val.(types.BoolValue))
		}
	case types.StringType:
		stringPred := pred.(storage.StringPredicate)
		return func(val types.Value) bool {
			return stringPred.StringPred(val.(types.StringValue))
		}
	case types.BytesType:
		bytesPred := pred.(storage.BytesPredicate)
		return func(val types.Value) bool {
			return bytesPred.BytesPred(val.(types.BytesValue))
		}
	case types.Float64Type:
		float64Pred := pred.(storage.Float64Predicate)
		return func(val types.Value) bool {
			return float64Pred.Float64Pred(val.(types.Float64Value))
		}
	case types.Int64Type:
		int64Pred := pred.(storage.Int64Predicate)
		return func(val types.Value) bool {
			return int64Pred.Int64Pred(val.(types.Int64Value))
		}
	default:
		panic(fmt.Sprintf("unexpected column type: %#v %d", ct, ct.Type))
	}

	return nil
}

func (tbl *table) Rows(ctx context.Context, cols []types.ColumnNum, minRow, maxRow types.Row,
	pred storage.Predicate) (storage.Rows, error) {

	rel := toRelationId(tbl.tid, primaryIndexId)

	var maxItem item
	if maxRow != nil {
		maxItem = rowToItem(rel, tbl.tt.Primary, maxRow)
	}

	var predFn func(types.Value) bool
	var predCol types.ColumnNum
	if pred != nil {
		predCol = pred.Column()
		predFn = predicateFunction(pred, tbl.tt.ColumnTypes[predCol])
	}

	var items []item
	tbl.tx.tree.AscendGreaterOrEqual(rowToItem(rel, tbl.tt.Primary, minRow),
		func(it item) bool {
			if it.rel != rel {
				return false
			}
			if maxRow != nil && lessItems(maxItem, it) {
				return false
			}

			if predFn != nil && !predFn(it.row[predCol]) {
				return true
			}

			items = append(items, it)
			return true
		})

	tbl.tx.rowsCount += 1
	return &rows{
		tbl:   tbl,
		cols:  cols,
		items: items,
	}, nil
}

func (tbl *table) Update(ctx context.Context, rid storage.RowId, cols []types.ColumnNum,
	vals []types.Value) error {

	if len(cols) != len(vals) {
		panic(fmt.Sprintf("basic: table %d: update len(cols) != len(vals): %d %d", tbl.tid,
			len(cols), len(vals)))
	}

	it, ok := tbl.tx.tree.Get(rowIdToItem(toRelationId(tbl.tid, primaryIndexId), rid))
	if !ok {
		panic(fmt.Sprintf("basic: table %d: missing item to update: %v", tbl.tid, rid))
	}
	row := append(make([]types.Value, 0, len(it.row)), it.row...)
	for idx, col := range cols {
		row[col] = vals[idx]
	}

	tbl.tx.forWrite()

	if types.ColumnKeyUpdated(tbl.tt.Primary, cols) {
		err := tbl.Delete(ctx, rid)
		if err != nil {
			return err
		}
		err = tbl.Insert(ctx, []types.Row{row})
		if err != nil {
			return err
		}
	} else {
		tbl.tx.tree.ReplaceOrInsert(
			rowToItem(toRelationId(tbl.tid, primaryIndexId), tbl.tt.Primary, row))
	}

	return nil
}

func (tbl *table) Delete(ctx context.Context, rid storage.RowId) error {
	tbl.tx.forWrite()

	_, ok := tbl.tx.tree.Delete(rowIdToItem(toRelationId(tbl.tid, primaryIndexId), rid))
	if !ok {
		panic(fmt.Sprintf("basic: table %d: missing item to delete: %v", tbl.tid, rid))
	}

	return nil
}

func (tbl *table) Insert(ctx context.Context, rows []types.Row) error {
	tbl.tx.forWrite()

	rel := toRelationId(tbl.tid, primaryIndexId)
	for _, row := range rows {
		row, err := types.ConvertRow(tbl.tt.ColumnTypes, row)
		if err != nil {
			return err
		}

		it := rowToItem(rel, tbl.tt.Primary, row)
		if tbl.tx.tree.Has(it) {
			return fmt.Errorf("basic: %s: primary index: existing row with duplicate key: %s",
				tbl.tt.Name, row)
		}

		tbl.tx.tree.ReplaceOrInsert(it)
	}

	return nil
}

func (rs *rows) Next(ctx context.Context) (types.Row, error) {
	if rs.next < 0 {
		panic(fmt.Sprintf("basic: next on closed rows for table %d", rs.tbl.tid))
	}

	if rs.next == len(rs.items) {
		return nil, io.EOF
	}

	rs.next += 1
	if rs.cols != nil {
		row := make([]types.Value, len(rs.cols))
		for idx, col := range rs.cols {
			row[idx] = rs.items[rs.next-1].row[col]
		}
		return row, nil
	}

	return rs.items[rs.next-1].row, nil
}

func (rs *rows) Current() (storage.RowId, error) {
	if rs.next <= 0 {
		panic(fmt.Sprintf("basic: missing current on rows for table %d", rs.tbl.tid))
	}

	return rs.items[rs.next-1].RowId(), nil
}

func (rs *rows) Close(ctx context.Context) error {
	if rs.next < 0 {
		panic(fmt.Sprintf("basic: close on closed rows for table %d", rs.tbl.tid))
	}

	rs.tbl.tx.rowsCount -= 1
	rs.next = -1
	return nil
}
