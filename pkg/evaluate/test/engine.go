package test

import (
	"context"

	"github.com/leftmike/maho/pkg/engine"
	"github.com/leftmike/maho/pkg/storage"
	"github.com/leftmike/maho/pkg/types"
)

type Engine struct {
}

func (eng *Engine) CreateDatabase(dbn types.Identifier, opts storage.OptionsMap) error {
	// XXX
	return nil
}

func (eng *Engine) DropDatabase(dbn types.Identifier, ifExists bool,
	opts storage.OptionsMap) error {

	// XXX
	return nil
}

func (eng *Engine) Begin() engine.Transaction {
	// XXX
	return &Transaction{}
}

type Transaction struct {
}

func (tx *Transaction) Commit(ctx context.Context) error {
	// XXX
	return nil
}

func (tx *Transaction) Rollback() error {
	// XXX
	return nil
}
