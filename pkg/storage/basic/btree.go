package basic

import (
	"bytes"
	"fmt"

	"github.com/google/btree"

	"github.com/leftmike/maho/pkg/encode"
	"github.com/leftmike/maho/pkg/storage"
	"github.com/leftmike/maho/pkg/types"
)

type relationId uint64
type item struct {
	rel relationId
	key []byte
	row types.Row
}

func lessItems(it1, it2 item) bool {
	if it1.rel < it2.rel {
		return true
	}
	return it1.rel == it2.rel && bytes.Compare(it1.key, it2.key) < 0
}

func toRelationId(tid storage.TableId, iid storage.IndexId) relationId {
	return relationId(uint64(tid)<<32 | uint64(iid))
}

func rowToItem(rel relationId, rowKey []types.ColumnKey,
	row types.Row) item {

	it := item{
		rel: rel,
		row: row,
	}
	if row != nil {
		it.key = encode.MakeKey(rowKey, row)
	}
	return it
}

func rowIdToItem(rel relationId, rid storage.RowId) item {
	return item{
		rel: rel,
		key: rid.([]byte),
	}
}

func (it item) String() string {
	return fmt.Sprintf("%d:%d %v %s", it.rel>>32, it.rel&0xFFFFFFFF, it.key, it.row)
}

func (it item) RowId() storage.RowId {
	return it.key
}

func newBTree() *btree.BTreeG[item] {
	return btree.NewG[item](8, lessItems)
}
