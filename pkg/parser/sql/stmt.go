package sql

import (
	"fmt"

	"github.com/leftmike/maho/pkg/types"
)

type Resolver interface {
	ResolveTable(tn types.TableName) types.TableName
	ResolveSchema(sn types.SchemaName) types.SchemaName
}

type Stmt interface {
	String() string
	Resolve(r Resolver)
}

type Begin struct{}

func (_ *Begin) String() string {
	return "BEGIN"
}

func (_ *Begin) Resolve(r Resolver) {}

type Commit struct{}

func (_ *Commit) String() string {
	return "COMMIT"
}

func (_ *Commit) Resolve(r Resolver) {}

type Rollback struct{}

func (_ *Rollback) String() string {
	return "ROLLBACK"
}

func (_ *Rollback) Resolve(r Resolver) {}

type Set struct {
	Variable types.Identifier
	Value    string
}

func (stmt *Set) String() string {
	return fmt.Sprintf("SET %s TO %s", stmt.Variable, stmt.Value)
}

func (_ *Set) Resolve(r Resolver) {}

type Show struct {
	Variable types.Identifier
}

func (stmt *Show) String() string {
	return fmt.Sprintf("SHOW %s", stmt.Variable)
}

func (_ *Show) Resolve(r Resolver) {}

type Explain struct {
	Stmt    Stmt
	Verbose bool
}

func (stmt *Explain) String() string {
	return fmt.Sprintf("EXPLAIN %s", stmt.Stmt)
}

func (stmt *Explain) Resolve(r Resolver) {
	stmt.Stmt.Resolve(r)
}
