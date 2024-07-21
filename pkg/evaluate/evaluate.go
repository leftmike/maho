package evaluate

import (
	"context"
	"fmt"

	"github.com/leftmike/maho/pkg/engine"
	"github.com/leftmike/maho/pkg/parser/sql"
)

func Evaluate(ctx context.Context, tx engine.Transaction, stmt sql.Stmt) error {
	switch stmt := stmt.(type) {
	case *sql.Begin:
		panic("evaluate: begin unexpected")
	case *sql.Commit:
		panic("evaluate: commit unexpected")
	case *sql.CreateDatabase:
		panic("evaluate: create database unexpected")
	case *sql.CreateSchema:
		return tx.CreateSchema(ctx, stmt.Schema)
	case *sql.CreateTable:
		return EvaluateCreateTable(ctx, tx, stmt)
	case *sql.DropDatabase:
		panic("evaluate: drop database unexpected")
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

func EvaluateCreateTable(ctx context.Context, tx engine.Transaction,
	stmt *sql.CreateTable) error {

	err := tx.CreateTable(ctx, stmt.Table, stmt.Columns, stmt.ColumnTypes)
	// XXX:	ColumnDefaults
	// XXX: IfNotExists
	// XXX: Constraints
	// XXX: ForeignKeys
	return err
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
