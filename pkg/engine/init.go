package engine

import (
	"context"

	"github.com/leftmike/maho/pkg/storage"
)

func Init(store storage.Store) (err error) {
	ctx := context.Background()
	tx := store.Begin()
	defer func() {
		if err != nil {
			tx.Rollback()
		} else {
			err = tx.Commit(ctx)
		}
	}()

	err = createTypedTable(ctx, tx, sequencesTypedInfo)
	if err != nil {
		return err
	}
	err = createTypedTable(ctx, tx, databasesTypedInfo)
	if err != nil {
		return err
	}
	err = createTypedTable(ctx, tx, schemasTypedInfo)
	if err != nil {
		return err
	}
	err = createTypedTable(ctx, tx, tablesTypedInfo)
	if err != nil {
		return err
	}
	// XXX
	return nil
}
