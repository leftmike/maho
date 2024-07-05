package sql

import (
	"fmt"

	"github.com/leftmike/maho/pkg/types"
)

type Stmt interface {
	String() string
}

type Begin struct{}

func (_ *Begin) String() string {
	return "BEGIN"
}

type Commit struct{}

func (_ *Commit) String() string {
	return "COMMIT"
}

type Rollback struct{}

func (_ *Rollback) String() string {
	return "ROLLBACK"
}

type Set struct {
	Variable types.Identifier
	Value    string
}

func (stmt *Set) String() string {
	return fmt.Sprintf("SET %s TO %s", stmt.Variable, stmt.Value)
}

type Show struct {
	Variable types.Identifier
}

func (stmt *Show) String() string {
	return fmt.Sprintf("SHOW %s", stmt.Variable)
}

type Explain struct {
	Stmt    Stmt
	Verbose bool
}

func (stmt Explain) String() string {
	return fmt.Sprintf("EXPLAIN %s", stmt.Stmt)
}
