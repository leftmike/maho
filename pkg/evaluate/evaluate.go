package evaluate

import (
	"context"
	"fmt"
	"slices"

	"github.com/leftmike/maho/pkg/engine"
	"github.com/leftmike/maho/pkg/parser/sql"
	"github.com/leftmike/maho/pkg/types"
)

func Evaluate(ctx context.Context, tx engine.Transaction, stmt sql.Stmt) error {
	switch stmt := stmt.(type) {
	case *sql.Begin:
		panic("evaluate: begin unexpected")
	case *sql.Commit:
		panic("evaluate: commit unexpected")
	case *sql.CreateDatabase:
		panic("evaluate: create database unexpected")
	case *sql.CreateIndex:
		return EvaluateCreateIndex(ctx, tx, stmt)
	case *sql.CreateSchema:
		return tx.CreateSchema(ctx, stmt.Schema)
	case *sql.CreateTable:
		return EvaluateCreateTable(ctx, tx, stmt)
	case *sql.DropDatabase:
		panic("evaluate: drop database unexpected")
	case *sql.DropIndex:
		return EvaluateDropIndex(ctx, tx, stmt)
	case *sql.DropSchema:
		return tx.DropSchema(ctx, stmt.Schema, stmt.IfExists)
	case *sql.DropTable:
		return EvaluateDropTable(ctx, tx, stmt)
	case *sql.Rollback:
		panic("evaluate: rollback unexpected")
	case *sql.Set:
		panic("evaluate: set unexpected")
	}

	panic(fmt.Sprintf("evaluate: unexpected stmt: %#v", stmt))
}

func columnNumber(nam types.Identifier, colNames []types.Identifier) (types.ColumnNum, bool) {
	for num, col := range colNames {
		if nam == col {
			return types.ColumnNum(num), true
		}
	}
	return 0, false
}

func indexKeyToColumnKey(ik sql.IndexKey, colNames []types.Identifier) ([]types.ColumnKey,
	types.Identifier) {

	var key []types.ColumnKey
	for cdx, col := range ik.Columns {
		num, ok := columnNumber(col, colNames)
		if !ok {
			return nil, col
		}
		key = append(key, types.MakeColumnKey(num, ik.Reverse[cdx]))
	}

	return key, 0
}

func EvaluateCreateIndex(ctx context.Context, tx engine.Transaction, stmt *sql.CreateIndex) error {
	tbl, err := tx.OpenTable(ctx, stmt.Table)
	if err != nil {
		return err
	}

	tt := tbl.Type()
	if slices.ContainsFunc(tt.Indexes,
		func(it engine.IndexType) bool {
			return it.Name == stmt.Index
		}) {

		if stmt.IfNotExists {
			return nil
		}
		return fmt.Errorf("evaluate: create index: index already exists: %s: %s", stmt.Table,
			stmt.Index)
	}

	key, col := indexKeyToColumnKey(stmt.Key, tt.ColumnNames)
	if col != 0 {
		return fmt.Errorf("evaluate: create index: %s: %s: key: unknown column: %s",
			stmt.Table, stmt.Index, col)
	}

	return tx.CreateIndex(ctx, stmt.Table, stmt.Index, key)
}

func EvaluateCreateTable(ctx context.Context, tx engine.Transaction, stmt *sql.CreateTable) error {
	if _, err := tx.OpenTable(ctx, stmt.Table); err == nil {
		if stmt.IfNotExists {
			return nil
		}
		return fmt.Errorf("evaluate: create table: table already exists: %s", stmt.Table)
	}

	var primary []types.ColumnKey
	for _, con := range stmt.Constraints {
		if con.Type == sql.PrimaryConstraint {
			var col types.Identifier
			primary, col = indexKeyToColumnKey(con.Key, stmt.Columns)
			if col != 0 {
				return fmt.Errorf("evaluate: create table: %s: primary key: unknown column: %s",
					stmt.Table, col)
			}
			break
		}
	}

	err := tx.CreateTable(ctx, stmt.Table, stmt.Columns, stmt.ColumnTypes, primary)
	// XXX:	ColumnDefaults
	// XXX: Constraints
	// XXX: ForeignKeys
	return err
}

func EvaluateDropIndex(ctx context.Context, tx engine.Transaction,
	stmt *sql.DropIndex) error {

	// XXX: IfExists
	return tx.DropIndex(ctx, stmt.Table, stmt.Index)
}

func EvaluateDropTable(ctx context.Context, tx engine.Transaction,
	stmt *sql.DropTable) error {

	// XXX: IfExists
	// XXX: Cascade
	for _, tn := range stmt.Tables {
		err := tx.DropTable(ctx, tn)
		if err != nil {
			return err
		}
	}
	return nil
}
