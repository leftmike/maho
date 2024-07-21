package evaluate_test

import (
	"context"
	"testing"

	"github.com/leftmike/maho/pkg/evaluate"
	"github.com/leftmike/maho/pkg/evaluate/test"
	"github.com/leftmike/maho/pkg/parser/sql"
	"github.com/leftmike/maho/pkg/storage"
	"github.com/leftmike/maho/pkg/testutil"
	"github.com/leftmike/maho/pkg/types"
)

type evaluateCase struct {
	stmt     sql.Stmt
	panicked bool
	fail     bool
	expect   []interface{}
}

func evaluateExpect(cases []evaluateCase) []interface{} {
	var expect []interface{}
	for _, c := range cases {
		if c.expect != nil {
			expect = append(expect, c.expect...)
		}
	}

	return expect
}

func TestSessionResolve(t *testing.T) {
	database := types.ID("maho", false)
	schema := types.PUBLIC

	ses := evaluate.NewSession(test.NewMockEngine(t, nil), database, schema)

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
			stmt:   mustParse("begin"),
			expect: []interface{}{test.Begin{}},
		},
		{
			stmt: mustParse("begin"),
			fail: true,
		},
		{
			stmt:   mustParse("commit"),
			expect: []interface{}{test.Commit{}},
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
			stmt:   mustParse("begin"),
			expect: []interface{}{test.Begin{}},
		},
		{
			stmt:   mustParse("rollback"),
			expect: []interface{}{test.Rollback{}},
		},
		{
			stmt: mustParse("set database = 'db'"),
		},
		{
			stmt: mustParse("create schema sn"),
			expect: []interface{}{
				test.Begin{},
				test.CreateSchema{
					Schema: types.SchemaName{types.ID("db", false), types.ID("sn", false)},
				},
				test.Commit{},
			},
		},
		{
			stmt: mustParse("create schema sn"),
			fail: true,
			expect: []interface{}{
				test.Begin{},
				test.CreateSchema{
					Schema: types.SchemaName{types.ID("db", false), types.ID("sn", false)},
					Fail:   true,
				},
				test.Rollback{},
			},
		},
	}

	eng := test.NewMockEngine(t, evaluateExpect(cases))
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
}

func TestSessionEvaluate(t *testing.T) {
	cases := []evaluateCase{
		{
			stmt: mustParse("create database db with this=123 that 'abcdef'"),
			expect: []interface{}{
				test.CreateDatabase{
					Database: types.ID("db", false),
					Options: storage.OptionsMap{
						types.ID("this", false): "123",
						types.ID("that", false): "abcdef",
					},
				},
			},
		},
		{
			stmt: mustParse("drop database if exists db"),
			expect: []interface{}{
				test.DropDatabase{
					Database: types.ID("db", false),
					IfExists: true,
				},
			},
		},
		{
			stmt:   mustParse("begin"),
			expect: []interface{}{test.Begin{}},
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

	eng := test.NewMockEngine(t, evaluateExpect(cases))
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
}
