package evaluate_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/leftmike/maho/pkg/engine"
	"github.com/leftmike/maho/pkg/evaluate"
	"github.com/leftmike/maho/pkg/parser"
	"github.com/leftmike/maho/pkg/parser/sql"
	"github.com/leftmike/maho/pkg/testutil"
)

func mustParse(s string) sql.Stmt {
	p := parser.NewParser(strings.NewReader(s), "")
	stmt, err := p.Parse()
	if err != nil {
		panic(fmt.Sprintf("must parse failed: %s: %s", err, s))
	}
	return stmt
}

func TestEvaluate(t *testing.T) {
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
	}

	ctx := context.Background()
	var eng engine.Engine
	var tx engine.Transaction

	for _, c := range cases {
		err, panicked := testutil.ErrorPanicked(func() error {
			return evaluate.Evaluate(ctx, eng, tx, c.stmt)
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
