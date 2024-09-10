package engine

import (
	"context"

	"github.com/leftmike/maho/storage"
	"github.com/leftmike/maho/types"
)

var (
	initTables = []struct {
		ti      *TypedInfo
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
					Database: types.MAHO.String(),
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
					Database: types.MAHO.String(),
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
		err := CreateTypedTable(ctx, tx, it.ti)
		if err != nil {
			return err
		}

		if len(it.structs) == 0 {
			continue
		}

		err = TypedTableInsert(ctx, tx, it.ti, it.structs...)
		if err != nil {
			return err
		}
	}

	var structs []interface{}
	for _, it := range initTables {
		buf, err := it.ti.TableType().Encode()
		if err != nil {
			return err
		}

		structs = append(structs,
			&tablesRow{
				Database: it.ti.tn.Database.String(),
				Schema:   it.ti.tn.Schema.String(),
				Table:    it.ti.tn.Table.String(),
				TableId:  int64(it.ti.tid),
				Type:     buf,
			})
	}

	return TypedTableInsert(ctx, tx, tablesTypedInfo, structs...)
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
