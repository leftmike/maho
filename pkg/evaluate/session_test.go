package evaluate_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/leftmike/maho/pkg/engine"
	"github.com/leftmike/maho/pkg/evaluate"
	"github.com/leftmike/maho/pkg/parser/sql"
	"github.com/leftmike/maho/pkg/storage"
	"github.com/leftmike/maho/pkg/testutil"
	"github.com/leftmike/maho/pkg/types"
)

type evaluateCase struct {
	stmt     sql.Stmt
	panicked bool
	fail     bool
	trace    string
	fn       func(t *testing.T, tx engine.Transaction)
}

func evaluateTrace(cases []evaluateCase) string {
	var buf strings.Builder
	for _, c := range cases {
		if c.trace != "" {
			fmt.Fprintln(&buf, c.trace)
		}
	}

	return buf.String()
}

func TestSessionResolve(t *testing.T) {
	database := types.ID("maho", false)
	schema := types.PUBLIC

	var buf bytes.Buffer
	eng := sesEngine{
		trace: &buf,
	}
	ses := evaluate.NewSession(eng, database, schema)

	tn := types.TableName{Table: types.ID("tbl", false)}
	rtn := types.TableName{
		Database: database,
		Schema:   schema,
		Table:    types.ID("tbl", false),
	}
	if ses.ResolveTable(tn) != rtn {
		t.Errorf("ResolveTable(%s) got %s want %s", tn, ses.ResolveTable(tn), rtn)
	}

	sn := types.SchemaName{Schema: types.ID("scm", false)}
	rsn := types.SchemaName{
		Database: database,
		Schema:   types.ID("scm", false),
	}
	if ses.ResolveSchema(sn) != rsn {
		t.Errorf("ResolveSchema(%s) got %s want %s", sn, ses.ResolveSchema(sn), rsn)
	}

	ctx := context.Background()

	stmt := mustParse("set database = 'db'")
	err := ses.Evaluate(ctx, stmt)
	if err != nil {
		t.Errorf("Evaluate(%s) failed with %s", stmt, err)
	}

	stmt = mustParse("set schema = 'test'")
	err = ses.Evaluate(ctx, stmt)
	if err != nil {
		t.Errorf("Evaluate(%s) failed with %s", stmt, err)
	}

	tn = types.TableName{Table: types.ID("tbl", false)}
	rtn = types.TableName{
		Database: types.ID("db", false),
		Schema:   types.ID("test", false),
		Table:    types.ID("tbl", false),
	}
	if ses.ResolveTable(tn) != rtn {
		t.Errorf("ResolveTable(%s) got %s want %s", tn, ses.ResolveTable(tn), rtn)
	}
}

func TestSessionBegin(t *testing.T) {
	cases := []evaluateCase{
		{
			stmt:  mustParse("begin"),
			trace: "Begin()",
		},
		{
			stmt: mustParse("begin"),
			fail: true,
		},
		{
			stmt:  mustParse("commit"),
			trace: "Commit()",
		},
		{
			stmt: mustParse("commit"),
			fail: true,
		},
		{
			stmt: mustParse("rollback"),
			fail: true,
		},
		{
			stmt:  mustParse("begin"),
			trace: "Begin()",
		},
		{
			stmt:  mustParse("rollback"),
			trace: "Rollback()",
		},
		{
			stmt: mustParse("set database = 'db'"),
		},
		{
			stmt: mustParse("create schema sn"),
			trace: `Begin()
CreateSchema(db.sn)
Commit()`,
		},
		{
			stmt: mustParse("drop schema sn2"),
			fail: true,
			trace: `Begin()
DropSchema(db.sn2, false)
Rollback()`,
		},
	}

	var buf bytes.Buffer
	eng := sesEngine{
		trace: &buf,
	}
	ses := evaluate.NewSession(eng, types.ID("maho", false), types.PUBLIC)

	ctx := context.Background()
	for _, c := range cases {
		err, panicked := testutil.ErrorPanicked(func() error {
			return ses.Evaluate(ctx, c.stmt)
		})
		if panicked {
			if !c.panicked {
				t.Errorf("Evaluate(%s) panicked", c.stmt)
			}
		} else if c.panicked {
			t.Errorf("Evaluate(%s) did not panic", c.stmt)
		} else if err != nil {
			if !c.fail {
				t.Errorf("Evaluate(%s) failed with %s", c.stmt, err)
			}
		} else if c.fail {
			t.Errorf("Evaluate(%s) did not fail", c.stmt)
		}
	}

	got := buf.String()
	want := evaluateTrace(cases)
	if got != want {
		t.Errorf("Evaluate() got %s want %s", got, want)
	}
}

func TestSessionEvaluate(t *testing.T) {
	cases := []evaluateCase{
		{
			stmt:  mustParse("create database db with this=123 that 'abcdef'"),
			trace: "CreateDatabase(db, map[this:123 that:abcdef])",
		},
		{
			stmt:  mustParse("drop database if exists db"),
			trace: "DropDatabase(db, true)",
		},
		{
			stmt:  mustParse("begin"),
			trace: "Begin()",
		},
		{
			stmt: mustParse("create database db"),
			fail: true,
		},
		{
			stmt: mustParse("drop database db"),
			fail: true,
		},
	}

	var buf bytes.Buffer
	eng := sesEngine{
		trace: &buf,
	}
	ses := evaluate.NewSession(eng, types.ID("maho", false), types.PUBLIC)
	ctx := context.Background()

	for _, c := range cases {
		err, panicked := testutil.ErrorPanicked(func() error {
			return ses.Evaluate(ctx, c.stmt)
		})
		if panicked {
			if !c.panicked {
				t.Errorf("Evaluate(%s) panicked", c.stmt)
			}
		} else if c.panicked {
			t.Errorf("Evaluate(%s) did not panic", c.stmt)
		} else if err != nil {
			if !c.fail {
				t.Errorf("Evaluate(%s) failed with %s", c.stmt, err)
			}
		} else if c.fail {
			t.Errorf("Evaluate(%s) did not fail", c.stmt)
		}
	}

	got := buf.String()
	want := evaluateTrace(cases)
	if got != want {
		t.Errorf("Evaluate() got %s want %s", got, want)
	}
}

type sesEngine struct {
	trace io.Writer
}

type sesTx struct {
	trace io.Writer
}

func (eng sesEngine) CreateDatabase(dn types.Identifier, opts storage.OptionsMap) error {
	fmt.Fprintf(eng.trace, "CreateDatabase(%s, %s)\n", dn, opts)
	return nil
}

func (eng sesEngine) DropDatabase(dn types.Identifier, ifExists bool) error {
	fmt.Fprintf(eng.trace, "DropDatabase(%s, %v)\n", dn, ifExists)
	return nil
}

func (eng sesEngine) Begin() engine.Transaction {
	fmt.Fprintln(eng.trace, "Begin()")

	return sesTx{
		trace: eng.trace,
	}
}

func (tx sesTx) Commit(ctx context.Context) error {
	fmt.Fprintln(tx.trace, "Commit()")
	return nil
}

func (tx sesTx) Rollback() error {
	fmt.Fprintln(tx.trace, "Rollback()")
	return nil
}

func (tx sesTx) CreateSchema(ctx context.Context, sn types.SchemaName) error {
	fmt.Fprintf(tx.trace, "CreateSchema(%s)\n", sn)
	return nil
}

func (tx sesTx) DropSchema(ctx context.Context, sn types.SchemaName, ifExists bool) error {
	fmt.Fprintf(tx.trace, "DropSchema(%s, %v)\n", sn, ifExists)
	return errors.New("test engine: drop schema failed")
}

func (tx sesTx) ListSchemas(ctx context.Context, dn types.Identifier) ([]types.Identifier, error) {
	fmt.Fprintf(tx.trace, "ListSchemas(%s)\n", dn)
	return nil, nil
}

func (tx sesTx) OpenTable(ctx context.Context, tn types.TableName) (engine.Table, error) {
	fmt.Fprintf(tx.trace, "OpenTable(%s)\n", tn)
	return nil, nil
}

func (tx sesTx) CreateTable(ctx context.Context, tn types.TableName, colNames []types.Identifier,
	colTypes []types.ColumnType, primary []types.ColumnKey) error {

	fmt.Fprintf(tx.trace, "CreateTable(%s, %v, %v, %v)\n", tn, colNames, colTypes, primary)
	return nil
}

func (tx sesTx) DropTable(ctx context.Context, tn types.TableName) error {
	fmt.Fprintf(tx.trace, "DropTable(%s)\n", tn)
	return nil
}

func (tx sesTx) ListTables(ctx context.Context, sn types.SchemaName) ([]types.Identifier, error) {
	fmt.Fprintf(tx.trace, "ListTables(%s)\n", sn)
	return nil, nil
}

func (tx sesTx) CreateIndex(ctx context.Context, tn types.TableName, in types.Identifier,
	key []types.ColumnKey) error {

	fmt.Fprintf(tx.trace, "CreateIndex(%s, %s, %v)\n", tn, in, key)
	return nil
}

func (tx sesTx) DropIndex(ctx context.Context, tn types.TableName, in types.Identifier) error {
	fmt.Fprintf(tx.trace, "DropIndex(%s, %s)\n", tn, in)
	return nil
}
