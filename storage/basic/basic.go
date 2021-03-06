package basic

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/google/btree"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/sql"
	"github.com/leftmike/maho/storage"
	"github.com/leftmike/maho/storage/encode"
)

var (
	errTransactionComplete = errors.New("basic: transaction already completed")
)

type basicStore struct {
	mutex sync.Mutex
	tree  *btree.BTree
}

type transaction struct {
	bst  *basicStore
	tree *btree.BTree
}

type table struct {
	bst *basicStore
	tl  *storage.TableLayout
	tn  sql.TableName
	tid int64
	tx  *transaction
}

type rowItem struct {
	rid int64
	key []byte
	row []sql.Value
}

type rows struct {
	tbl  *table
	idx  int
	rows [][]sql.Value
}

type indexRows struct {
	tbl  *table
	il   storage.IndexLayout
	idx  int
	rows [][]sql.Value
}

func NewStore(dataDir string) (*storage.Store, error) {
	bst := &basicStore{
		tree: btree.New(16),
	}
	return storage.NewStore("basic", bst, true)
}

func (_ *basicStore) Table(ctx context.Context, tx engine.Transaction, tn sql.TableName, tid int64,
	tt *engine.TableType, tl *storage.TableLayout) (storage.Table, error) {

	if len(tt.PrimaryKey()) == 0 {
		panic(fmt.Sprintf("basic: table %s: missing required primary key", tn))
	}

	etx := tx.(*transaction)
	return &table{
		bst: etx.bst,
		tl:  tl,
		tn:  tn,
		tid: tid,
		tx:  etx,
	}, nil
}

func (bst *basicStore) Begin(sesid uint64) engine.Transaction {
	bst.mutex.Lock()
	return &transaction{
		bst:  bst,
		tree: bst.tree,
	}
}

func (btx *transaction) Commit(ctx context.Context) error {
	if btx.bst == nil {
		return errTransactionComplete
	}

	btx.bst.tree = btx.tree
	btx.bst.mutex.Unlock()
	btx.bst = nil
	btx.tree = nil
	return nil
}

func (btx *transaction) Rollback() error {
	if btx.bst == nil {
		return errTransactionComplete
	}

	btx.bst.mutex.Unlock()
	btx.bst = nil
	btx.tree = nil
	return nil
}

func (_ *transaction) NextStmt() {}

func (btx *transaction) forWrite() {
	if btx.tree == btx.bst.tree {
		btx.tree = btx.bst.tree.Clone()
	}
}

func (bt *table) toItem(row []sql.Value) btree.Item {
	ri := rowItem{
		rid: (bt.tid << 16) | storage.PrimaryIID,
	}
	if row != nil {
		ri.key = encode.MakeKey(bt.tl.PrimaryKey(), row)
		ri.row = append(make([]sql.Value, 0, bt.tl.NumColumns()), row...)
	}
	return ri
}

func (bt *table) toIndexItem(row []sql.Value, il storage.IndexLayout) btree.Item {
	ri := rowItem{
		rid: (bt.tid << 16) | il.IID,
	}
	if row != nil {
		ri.row = il.RowToIndexRow(row)
		ri.key = il.MakeKey(encode.MakeKey(il.Key, ri.row), ri.row)
	}
	return ri
}

func (ri rowItem) Less(item btree.Item) bool {
	ri2 := item.(rowItem)
	if ri.rid < ri2.rid {
		return true
	}
	return ri.rid == ri2.rid && bytes.Compare(ri.key, ri2.key) < 0
}

func (bt *table) fetchRows(ctx context.Context, minRow, maxRow []sql.Value) [][]sql.Value {
	var maxItem btree.Item
	if maxRow != nil {
		maxItem = bt.toItem(maxRow)
	}

	var rows [][]sql.Value
	rid := (bt.tid << 16) | storage.PrimaryIID
	bt.tx.tree.AscendGreaterOrEqual(bt.toItem(minRow),
		func(item btree.Item) bool {
			if maxItem != nil && maxItem.Less(item) {
				return false
			}
			ri := item.(rowItem)
			if ri.rid != rid {
				return false
			}
			rows = append(rows, append(make([]sql.Value, 0, len(ri.row)), ri.row...))
			return true
		})

	return rows
}

func (bt *table) Rows(ctx context.Context, minRow, maxRow []sql.Value) (engine.Rows, error) {
	return &rows{
		tbl:  bt,
		idx:  0,
		rows: bt.fetchRows(ctx, minRow, maxRow),
	}, nil
}

func (bt *table) IndexRows(ctx context.Context, iidx int,
	minRow, maxRow []sql.Value) (engine.IndexRows, error) {

	indexes := bt.tl.Indexes()
	if iidx >= len(indexes) {
		panic(fmt.Sprintf("basic: table: %s: %d indexes: out of range: %d", bt.tn, len(indexes),
			iidx))
	}

	il := indexes[iidx]
	bir := &indexRows{
		tbl: bt,
		il:  il,
		idx: 0,
	}

	var maxItem btree.Item
	if maxRow != nil {
		maxItem = bt.toIndexItem(maxRow, il)
	}

	rid := (bt.tid << 16) | il.IID
	bt.tx.tree.AscendGreaterOrEqual(bt.toIndexItem(minRow, il),
		func(item btree.Item) bool {
			if maxItem != nil && maxItem.Less(item) {
				return false
			}
			ri := item.(rowItem)
			if ri.rid != rid {
				return false
			}
			bir.rows = append(bir.rows, append(make([]sql.Value, 0, len(ri.row)), ri.row...))
			return true
		})
	return bir, nil
}

func (bt *table) Insert(ctx context.Context, rows [][]sql.Value) error {
	bt.tx.forWrite()

	for _, row := range rows {
		item := bt.toItem(row)
		if bt.tx.tree.Has(item) {
			return fmt.Errorf("basic: %s: primary index: existing row with duplicate key", bt.tn)
		}
		bt.tx.tree.ReplaceOrInsert(item)

		for idx, il := range bt.tl.Indexes() {
			item := bt.toIndexItem(row, il)
			if bt.tx.tree.Has(item) {
				return fmt.Errorf("basic: %s: %s index: existing row with duplicate key", bt.tn,
					bt.tl.IndexName(idx))
			}
			bt.tx.tree.ReplaceOrInsert(item)
		}
	}

	return nil
}

func (bt *table) FillIndex(ctx context.Context, iidx int) error {
	indexes := bt.tl.Indexes()
	if iidx >= len(indexes) {
		panic(fmt.Sprintf("basic: table: %s: %d indexes: out of range: %d", bt.tn, len(indexes),
			iidx))
	}
	il := indexes[iidx]

	for _, row := range bt.fetchRows(ctx, nil, nil) {
		item := bt.toIndexItem(row, il)
		if bt.tx.tree.Has(item) {
			return fmt.Errorf("basic: %s: %s index: existing row with duplicate key", bt.tn,
				bt.tl.IndexName(iidx))
		}
		bt.tx.tree.ReplaceOrInsert(item)
	}

	return nil
}

func (br *rows) NumColumns() int {
	return br.tbl.tl.NumColumns()
}

func (br *rows) Close() error {
	br.tbl = nil
	br.rows = nil
	br.idx = 0
	return nil
}

func (br *rows) Next(ctx context.Context) ([]sql.Value, error) {
	if br.idx == len(br.rows) {
		return nil, io.EOF
	}

	br.idx += 1
	return br.rows[br.idx-1], nil
}

func (bt *table) deleteRow(ctx context.Context, row []sql.Value) error {
	if bt.tx.tree.Delete(bt.toItem(row)) == nil {
		return fmt.Errorf("basic: table %s: internal error: missing row to delete", bt.tn)
	}

	for idx, il := range bt.tl.Indexes() {
		if bt.tx.tree.Delete(bt.toIndexItem(row, il)) == nil {
			return fmt.Errorf("basic: table %s: %s index: internal error: missing row to delete",
				bt.tn, bt.tl.IndexName(idx))
		}
	}

	return nil
}

func (br *rows) Delete(ctx context.Context) error {
	br.tbl.tx.forWrite()

	if br.idx == 0 {
		panic(fmt.Sprintf("basic: table %s: no row to delete", br.tbl.tn))
	}

	return br.tbl.deleteRow(ctx, br.rows[br.idx-1])
}

func (bt *table) updateIndexes(ctx context.Context, updatedCols []int,
	row, updateRow []sql.Value) error {

	indexes, updated := bt.tl.IndexesUpdated(updatedCols)
	for idx := range indexes {
		il := indexes[idx]
		if updated[idx] {
			if bt.tx.tree.Delete(bt.toIndexItem(row, il)) == nil {
				return fmt.Errorf(
					"basic: table %s: %s index: internal error: missing row to delete",
					bt.tn, bt.tl.IndexName(idx))
			}

			item := bt.toIndexItem(updateRow, il)
			if bt.tx.tree.Has(item) {
				return fmt.Errorf("basic: %s: %s index: existing row with duplicate key",
					bt.tn, bt.tl.IndexName(idx))
			}
			bt.tx.tree.ReplaceOrInsert(item)
		} else {
			bt.tx.tree.ReplaceOrInsert(bt.toIndexItem(updateRow, il))
		}
	}
	return nil
}

func (bt *table) updateRow(ctx context.Context, updatedCols []int,
	row, updateRow []sql.Value) error {

	if bt.tl.PrimaryUpdated(updatedCols) {
		err := bt.deleteRow(ctx, row)
		if err != nil {
			return err
		}

		err = bt.Insert(ctx, [][]sql.Value{updateRow})
		if err != nil {
			return err
		}
	} else {
		bt.tx.tree.ReplaceOrInsert(bt.toItem(updateRow))
	}

	return bt.updateIndexes(ctx, updatedCols, row, updateRow)
}

func (br *rows) Update(ctx context.Context, updatedCols []int, updateRow []sql.Value) error {
	br.tbl.tx.forWrite()

	if br.idx == 0 {
		panic(fmt.Sprintf("basic: table %s no row to update", br.tbl.tn))
	}

	return br.tbl.updateRow(ctx, updatedCols, br.rows[br.idx-1], updateRow)
}

func (bir *indexRows) NumColumns() int {
	return len(bir.il.Columns)
}

func (bir *indexRows) Close() error {
	bir.tbl = nil
	bir.rows = nil
	bir.idx = 0
	return nil
}

func (bir *indexRows) Next(ctx context.Context) ([]sql.Value, error) {
	if bir.idx == len(bir.rows) {
		return nil, io.EOF
	}

	bir.idx += 1
	return bir.rows[bir.idx-1], nil
}

func (bir *indexRows) Delete(ctx context.Context) error {
	bir.tbl.tx.forWrite()

	if bir.idx == 0 {
		panic(fmt.Sprintf("basic: table %s no row to delete", bir.tbl.tn))
	}

	return bir.tbl.deleteRow(ctx, bir.getRow())
}

func (bir *indexRows) Update(ctx context.Context, updatedCols []int, updateRow []sql.Value) error {
	bir.tbl.tx.forWrite()

	if bir.idx == 0 {
		panic(fmt.Sprintf("basic: table %s no row to update", bir.tbl.tn))
	}

	return bir.tbl.updateRow(ctx, updatedCols, bir.getRow(), updateRow)
}

func (bir *indexRows) getRow() []sql.Value {
	row := make([]sql.Value, bir.tbl.tl.NumColumns())
	bir.il.IndexRowToRow(bir.rows[bir.idx-1], row)

	item := bir.tbl.tx.tree.Get(bir.tbl.toItem(row))
	if item == nil {
		panic(fmt.Sprintf("basic: table %s no row to get in tree", bir.tbl.tn))
	}

	ri := item.(rowItem)
	return ri.row
}

func (bir *indexRows) Row(ctx context.Context) ([]sql.Value, error) {
	if bir.idx == 0 {
		panic(fmt.Sprintf("basic: table %s no row to get", bir.tbl.tn))
	}

	return bir.getRow(), nil
}
