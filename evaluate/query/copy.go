package query

import (
	"context"
	"fmt"
	"io"

	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/evaluate/copy"
	"github.com/leftmike/maho/sql"
)

type Copy struct {
	Table     sql.TableName
	Columns   []sql.Identifier
	From      io.RuneReader
	FromLine  int
	Delimiter rune
}

func (stmt *Copy) String() string {
	s := fmt.Sprintf("COPY %s (", stmt.Table)
	for i, col := range stmt.Columns {
		if i > 0 {
			s += ", "
		}
		s += col.String()
	}
	s += ") "

	s += "FROM STDIN"

	if stmt.Delimiter != '\t' {
		s += fmt.Sprintf(" DELIMITER '%c'", stmt.Delimiter)
	}
	return s
}

func (stmt *Copy) Plan(ctx context.Context, pctx evaluate.PlanContext,
	tx sql.Transaction, cctx sql.CompileContext) (evaluate.Plan, error) {

	tn := pctx.ResolveTableName(stmt.Table)
	tt, err := tx.LookupTableType(ctx, tn)
	if err != nil {
		return nil, err
	}

	cols := tt.Columns()
	colDefaults := tt.ColumnDefaults()

	defaultRow := make([]sql.CExpr, len(cols))
	cmap := map[sql.Identifier]int{}
	for cdx, cn := range cols {
		cmap[cn] = cdx
		defaultRow[cdx] = colDefaults[cdx].Default
	}

	fromToRow := make([]int, len(stmt.Columns))
	for fdx, cn := range stmt.Columns {
		cdx, ok := cmap[cn]
		if !ok {
			return nil, fmt.Errorf("engine: %s: column not found: %s", tn, cn)
		}
		fromToRow[fdx] = cdx
		defaultRow[cdx] = nil
	}

	allNil := true
	for _, ce := range defaultRow {
		if ce != nil {
			allNil = false
			break
		}
	}
	if allNil {
		defaultRow = nil
	}

	return &copyPlan{
		tn:         tn,
		ttVer:      tt.Version(),
		cols:       cols,
		from:       copy.NewReader("stdin", stmt.From, stmt.FromLine),
		fromToRow:  fromToRow,
		defaultRow: defaultRow,
		delimiter:  stmt.Delimiter,
	}, nil
}

type copyPlan struct {
	tn         sql.TableName
	ttVer      int64
	cols       []sql.Identifier
	from       *copy.Reader
	fromToRow  []int
	defaultRow []sql.CExpr
	delimiter  rune
}

func (_ *copyPlan) Tag() string {
	return "COPY"
}

func (plan *copyPlan) Execute(ctx context.Context, tx sql.Transaction) (int64, error) {
	tbl, err := tx.LookupTable(ctx, plan.tn, plan.ttVer)
	if err != nil {
		return -1, err
	}

	var cnt int64
	rows := make([][]sql.Value, 0, 128)
	err = copy.CopyFromText(plan.from, len(plan.fromToRow), plan.delimiter,
		func(vals []sql.Value) error {
			row := make([]sql.Value, len(plan.cols))
			for cdx, ce := range plan.defaultRow {
				if ce == nil {
					continue
				}

				var err error
				row[cdx], err = ce.Eval(ctx, tx, nil)
				if err != nil {
					return err
				}
			}

			for fdx, val := range vals {
				row[plan.fromToRow[fdx]] = val
			}
			cnt += 1

			if len(rows) == cap(rows) {
				err := tbl.Insert(ctx, rows)
				if err != nil {
					return err
				}
				if len(rows) < 1024 {
					rows = make([][]sql.Value, 0, 1024)
				} else {
					rows = rows[:0]
				}
			}

			rows = append(rows, row)
			return nil
		})
	if err != nil {
		return -1, err
	}

	if len(rows) > 0 {
		err := tbl.Insert(ctx, rows)
		if err != nil {
			return -1, err
		}
	}
	return cnt, nil
}
