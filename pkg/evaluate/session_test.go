package evaluate_test

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/leftmike/maho/pkg/evaluate"
	"github.com/leftmike/maho/pkg/evaluate/test"
	"github.com/leftmike/maho/pkg/parser/sql"
	"github.com/leftmike/maho/pkg/testutil"
	"github.com/leftmike/maho/pkg/types"
)

type evaluateCase struct {
	stmt     sql.Stmt
	panicked bool
	fail     bool
	trace    string
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

	ses := evaluate.NewSession(test.NewEngine(nil), database, schema)

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
			stmt: mustParse("create schema sn"),
			fail: true,
			trace: `Begin()
CreateSchema(db.sn)
Rollback()`,
		},
	}

	var buf bytes.Buffer
	eng := test.NewEngine(&buf)
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
	eng := test.NewEngine(&buf)
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
