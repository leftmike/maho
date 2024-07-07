package evaluate

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/leftmike/maho/pkg/engine"
	"github.com/leftmike/maho/pkg/parser/sql"
	"github.com/leftmike/maho/pkg/types"
)

var (
	sessionId atomic.Uint64
)

type Session struct {
	eng             engine.Engine
	tx              engine.Transaction
	defaultDatabase types.Identifier
	defaultSchema   types.Identifier
	id              uint64
}

func NewSession(eng engine.Engine, defaultDatabase, defaultSchema types.Identifier) *Session {
	return &Session{
		eng:             eng,
		defaultDatabase: defaultDatabase,
		defaultSchema:   defaultSchema,
		id:              sessionId.Add(1),
	}
}

func (ses *Session) ResolveTable(tn types.TableName) types.TableName {
	if tn.Database == 0 {
		tn.Database = ses.defaultDatabase
		if tn.Schema == 0 {
			tn.Schema = ses.defaultSchema
		}
	}
	return tn
}

func (ses *Session) ResolveSchema(sn types.SchemaName) types.SchemaName {
	if sn.Database == 0 {
		sn.Database = ses.defaultDatabase
	}
	return sn
}

func (ses *Session) Evaluate(ctx context.Context, stmt sql.Stmt) error {
	stmt.Resolve(ses)

	switch stmt := stmt.(type) {
	case *sql.Begin:
		if ses.tx != nil {
			return fmt.Errorf("execute: begin: session %d already has active transaction", ses.id)
		}
		ses.tx = ses.eng.Begin()
		return nil
	case *sql.Commit:
		if ses.tx == nil {
			return fmt.Errorf("execute: commit: session %d does not have active transaction",
				ses.id)
		}
		err := ses.tx.Commit(ctx)
		ses.tx = nil
		return err
	case *sql.Rollback:
		if ses.tx == nil {
			return fmt.Errorf("execute: rollback: session %d does not have active transaction",
				ses.id)
		}
		err := ses.tx.Rollback()
		ses.tx = nil
		return err
	case *sql.Set:
		return ses.set(stmt.Variable, stmt.Value)
	default:
		return Evaluate(ctx, ses.eng, ses.tx, stmt)
	}

	return nil
}

func (ses *Session) set(id types.Identifier, val string) error {
	if id == types.DATABASE {
		ses.defaultDatabase = types.ID(val, false)
	} else if id == types.SCHEMA {
		ses.defaultSchema = types.ID(val, false)
	} else {
		return fmt.Errorf("evaluate: set: %s not found", id)
	}

	return nil
}
