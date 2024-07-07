package evaluate_test

import (
	"context"
	"testing"

	"github.com/leftmike/maho/pkg/evaluate"
	"github.com/leftmike/maho/pkg/evaluate/test"
	"github.com/leftmike/maho/pkg/parser/sql"
	"github.com/leftmike/maho/pkg/testutil"
	"github.com/leftmike/maho/pkg/types"
)

func TestSession(t *testing.T) {
	database := types.ID("maho", false)
	schema := types.PUBLIC

	var eng test.Engine
	ses := evaluate.NewSession(&eng, database, schema)

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

	cases := []struct {
		stmt     sql.Stmt
		panicked bool
		fail     bool
	}{
		{
			stmt: mustParse("begin"),
		},
		{
			stmt: mustParse("begin"),
			fail: true,
		},
		{
			stmt: mustParse("commit"),
		},
		{
			stmt: mustParse("commit"),
			fail: true,
		},
		{
			stmt: mustParse("rollback"),
			fail: true,
		},

		// Keep as last cases.
		{
			stmt: mustParse("set database = 'db'"),
		},
		{
			stmt: mustParse("set schema = 'test'"),
		},
	}

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
