package engine

import (
	"context"

	"github.com/leftmike/maho/pkg/storage"
	"github.com/leftmike/maho/pkg/types"
)

func initStore(ctx context.Context, tx storage.Transaction) error {
	err := createTypedTable(ctx, tx, sequencesTypedInfo)
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

	tt, err := openTypedTable(ctx, tx, databasesTypedInfo)
	if err != nil {
		return err
	}
	err = tt.insert(ctx,
		&databasesRow{
			Database: types.SYSTEM.String(),
		},
		&databasesRow{
			Database: "maho",
		},
	)
	if err != nil {
		return err
	}

	tt, err = openTypedTable(ctx, tx, schemasTypedInfo)
	if err != nil {
		return err
	}
	err = tt.insert(ctx,
		&schemasRow{
			Database: types.SYSTEM.String(),
			Schema:   types.INFO.String(),
		},
		&schemasRow{
			Database: "maho",
			Schema:   types.PUBLIC.String(),
		},
	)
	if err != nil {
		return err
	}

	// XXX: insert data for sequences, databases, schemas, and tables table

	return nil
}

func Init(store storage.Store) error {
	ctx := context.Background()
	tx := store.Begin()
	err := initStore(ctx, tx)
	if err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit(ctx)
}
