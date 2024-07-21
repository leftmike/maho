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
	case *sql.DropDatabase:
		panic("evaluate: drop database unexpected")
	case *sql.Rollback:
		panic("evaluate: rollback unexpected")
	case *sql.Set:
		panic("evaluate: set unexpected")
	default:
		panic(fmt.Sprintf("evaluate: unexpected stmt: %#v", stmt))
	}

	return nil
}
