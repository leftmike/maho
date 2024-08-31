package engine

import (
	"context"

	"github.com/leftmike/maho/pkg/storage"
	"github.com/leftmike/maho/pkg/types"
)

var (
	initTables = []struct {
		ti      *typedInfo
		structs []interface{}
	}{
		{
			ti: sequencesTypedInfo,
			structs: []interface{}{
				&sequencesRow{
					Sequence: "next_table_id",
					Current:  int64(maxReservedTableId + 1),
				},
			},
		},
		{
			ti: databasesTypedInfo,
			structs: []interface{}{
				&databasesRow{
					Database: types.SYSTEM.String(),
				},
				&databasesRow{
					Database: "maho",
				},
			},
		},
		{
			ti: schemasTypedInfo,
			structs: []interface{}{
				&schemasRow{
					Database: types.SYSTEM.String(),
					Schema:   types.INFO.String(),
				},
				&schemasRow{
					Database: "maho",
					Schema:   types.PUBLIC.String(),
				},
			},
		},
		{
			ti: tablesTypedInfo,
		},
	}
)

func initStore(ctx context.Context, tx storage.Transaction) error {
	for _, it := range initTables {
		err := createTypedTable(ctx, tx, it.ti)
		if err != nil {
			return err
		}

		if len(it.structs) == 0 {
			continue
		}

		tt, err := openTypedTable(ctx, tx, it.ti)
		if err != nil {
			return err
		}
		err = tt.insert(ctx, it.structs...)
		if err != nil {
			return err
		}
	}

	tt, err := openTypedTable(ctx, tx, tablesTypedInfo)
	if err != nil {
		return err
	}
	for _, it := range initTables {
		buf, err := it.ti.toTableType().Encode()
		if err != nil {
			return err
		}
		err = tt.insert(ctx,
			&tablesRow{
				Database: it.ti.tn.Database.String(),
				Schema:   it.ti.tn.Schema.String(),
				Table:    it.ti.tn.Table.String(),
				TableID:  int64(it.ti.tid),
				Type:     buf,
			})
		if err != nil {
			return err
		}
	}

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
