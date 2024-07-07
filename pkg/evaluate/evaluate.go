package evaluate

import (
	"context"
	"fmt"

	"github.com/leftmike/maho/pkg/engine"
	"github.com/leftmike/maho/pkg/parser/sql"
)

func Evaluate(ctx context.Context, eng engine.Engine, tx engine.Transaction, stmt sql.Stmt) error {
	switch stmt := stmt.(type) {
	// XXX
	default:
		panic(fmt.Sprintf("evaluate: unexpected stmt: %#v", stmt))
	}

	return nil
}
