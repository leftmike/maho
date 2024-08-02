package evaluate_test

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/leftmike/maho/pkg/evaluate"
	"github.com/leftmike/maho/pkg/evaluate/test"
	"github.com/leftmike/maho/pkg/parser"
	"github.com/leftmike/maho/pkg/parser/sql"
	"github.com/leftmike/maho/pkg/testutil"
	"github.com/leftmike/maho/pkg/types"
)

func mustParse(s string) sql.Stmt {
	p := parser.NewParser(strings.NewReader(s), "")
	stmt, err := p.Parse()
	if err != nil {
		panic(fmt.Sprintf("must parse failed: %s: %s", err, s))
	}
	return stmt
}

func TestEvaluatePanic(t *testing.T) {
	cases := []struct {
		stmt     sql.Stmt
		panicked bool
		fail     bool
	}{
		{
			stmt:     mustParse("begin"),
			panicked: true,
		},
		{
			stmt:     mustParse("commit"),
			panicked: true,
		},
		{
			stmt:     mustParse("rollback"),
			panicked: true,
		},
		{
			stmt:     mustParse("set database = 'test'"),
			panicked: true,
		},
		{
			stmt:     mustParse("create database db"),
			panicked: true,
		},
		{
			stmt:     mustParse("drop database db"),
			panicked: true,
		},
	}

	ctx := context.Background()
	for _, c := range cases {
		err, panicked := testutil.ErrorPanicked(func() error {
			return evaluate.Evaluate(ctx, nil, c.stmt)
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

func TestEvaluate(t *testing.T) {
	cases := []evaluateCase{
		{
			trace: "Begin()",
		},
		{
			stmt:  mustParse("create schema sn"),
			trace: "CreateSchema(db.sn)",
		},
		{
			stmt:  mustParse("create table t1 (c1 int, c2 bool)"),
			trace: "CreateTable(db.sn.t1, [c1 c2], [INT BOOL], [])",
		},
		{
			stmt:  mustParse("create table t1 (c1 int, c2 bool)"),
			trace: "OpenTable(db.sn.t1)",
			fail:  true,
		},
		{
			stmt:  mustParse("create table t2 (c1 int primary key, c2 bool)"),
			trace: "CreateTable(db.sn.t2, [c1 c2], [INT BOOL], [1])",
		},
		{
			stmt:  mustParse("create table t3 (c1 int, c2 bool, primary key(c2, c1))"),
			trace: "CreateTable(db.sn.t3, [c1 c2], [INT BOOL], [2 1])",
		},
		{
			stmt:  mustParse("create table t4 (c1 int, c2 bool, primary key(c2, c1 desc))"),
			trace: "CreateTable(db.sn.t4, [c1 c2], [INT BOOL], [2 -1])",
		},
		{
			stmt: mustParse("create table tf (c1 int, c2 bool, primary key(c2, c3))"),
			fail: true,
		},
	}

	var buf bytes.Buffer
	eng := test.NewEngine(&buf)
	tx := eng.Begin()
	r := test.Resolver{
		Database: types.ID("db", false),
		Schema:   types.ID("sn", false),
	}

	ctx := context.Background()
	for _, c := range cases {
		if c.stmt == nil {
			continue
		}

		c.stmt.Resolve(r)

		err := evaluate.Evaluate(ctx, tx, c.stmt)
		if err != nil {
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
