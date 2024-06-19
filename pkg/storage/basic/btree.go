package basic

import (
	"bytes"

	"github.com/google/btree"

	"github.com/leftmike/maho/pkg/encode"
	"github.com/leftmike/maho/pkg/storage"
	"github.com/leftmike/maho/pkg/types"
)

type item struct {
	rid uint64
	key []byte
	row []types.Value
}

func lessItems(it1, it2 item) bool {
	if it1.rid < it2.rid {
		return true
	}
	return it1.rid == it2.rid && bytes.Compare(it1.key, it2.key) < 0
}

func toItem(tid storage.TableId, iid storage.IndexId, rowKey []types.ColumnKey,
	row []types.Value) item {

	return item{
		rid: uint64(tid)<<32 | uint64(iid),
		key: encode.MakeKey(rowKey, row),
		row: row,
	}
}

func newBTree() *btree.BTreeG[item] {
	return btree.NewG[item](8, lessItems)
}

// XXX: Clone
// XXX: AscendGreaterOrEqual
// XXX: Has
// XXX: ReplaceOrInsert
// XXX: Delete
// XXX: Get
