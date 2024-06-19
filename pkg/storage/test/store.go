package test

import (
	"context"
	"testing"

	"github.com/leftmike/maho/pkg/storage"
	"github.com/leftmike/maho/pkg/types"
)

func StoreTest(t *testing.T, store string, newStore func(dataDir string) (storage.Store, error)) {
	ctx := context.Background()

	st, err := newStore(t.TempDir())
	if err != nil {
		t.Errorf("%s.NewStore() failed with %s", store, err)
	}

	tx := st.Begin()
	err = tx.CreateTable(ctx, 1,
		[]types.Identifier{types.ID("col1", false), types.ID("col2", false)},
		[]types.ColumnType{})
	if err != nil {
		t.Errorf("CreateTable(%d) failed with %s", 1, err)
	}

	tbl, err := tx.OpenTable(ctx, 1)
	if err != nil {
		t.Errorf("OpenTable(%d) failed with %s", 1, err)
	}
	_ = tbl
}
