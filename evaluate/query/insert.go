package query

import (
	"context"
	"fmt"

	"github.com/leftmike/maho/evaluate"
	"github.com/leftmike/maho/evaluate/expr"
	"github.com/leftmike/maho/sql"
)

type InsertValues struct {
	Table   sql.TableName
	Columns []sql.Identifier
	Rows    [][]expr.Expr
}

func (stmt *InsertValues) String() string {
	s := fmt.Sprintf("INSERT INTO %s ", stmt.Table)
	if stmt.Columns != nil {
		s += "("
		for i, col := range stmt.Columns {
			if i > 0 {
				s += ", "
			}
			s += col.String()
		}
		s += ") "
	}

	s += "VALUES"

	for i, r := range stmt.Rows {
		if i > 0 {
			s += ", ("
		} else {
			s += " ("
		}

		for j, v := range r {
			if j > 0 {
				s += ", "
			}
			if v == nil {
				s += "NULL"
			} else {
				s += v.String()
			}
		}

		s += ")"
	}

	return s
}

func (stmt *InsertValues) Plan(ctx context.Context, pctx evaluate.PlanContext,
	tx sql.Transaction, cctx sql.CompileContext) (evaluate.Plan, error) {

	tn := pctx.ResolveTableName(stmt.Table)
	tt, err := tx.LookupTableType(ctx, tn)
	if err != nil {
		return nil, err
	}

	cols := tt.Columns()
	colDefaults := tt.ColumnDefaults()
	mv := len(cols)
	c2v := make([]int, mv) // column number to value number
	if stmt.Columns == nil {
		for c := range c2v {
			c2v[c] = c
		}
	} else {
		for c := range c2v {
			c2v[c] = len(c2v)
		}

		var cmap = make(map[sql.Identifier]int)
		for i, cn := range cols {
			cmap[cn] = i
		}

		mv = len(stmt.Columns)
		for v, nam := range stmt.Columns {
			c, ok := cmap[nam]
			if !ok {
				return nil, fmt.Errorf("engine: %s: column not found: %s", tn, nam)
			}
			c2v[c] = v
		}
	}

	var rows [][]sql.CExpr
	for _, r := range stmt.Rows {
		if len(r) > mv {
			return nil, fmt.Errorf("engine: %s: too many values", tn)
		}
		row := make([]sql.CExpr, len(cols))
		for i, cd := range colDefaults {
			var e expr.Expr
			if c2v[i] < len(r) {
				e = r[c2v[i]]
			}

			var ce sql.CExpr
			if e != nil {
				ce, _, err = expr.Compile(ctx, pctx, tx, nil, e)
				if err != nil {
					return nil, err
				}
			} else {
				ce = cd.Default
			}
			row[i] = ce
		}

		rows = append(rows, row)
	}

	return &insertValuesPlan{tn, tt.Version(), cols, rows}, nil
}

type insertValuesPlan struct {
	tn    sql.TableName
	ttVer int64
	cols  []sql.Identifier
	rows  [][]sql.CExpr
}

func (_ *insertValuesPlan) Tag() string {
	return "INSERT"
}

func (plan *insertValuesPlan) Execute(ctx context.Context, tx sql.Transaction) (int64, error) {
	tbl, err := tx.LookupTable(ctx, plan.tn, plan.ttVer)
	if err != nil {
		return -1, err
	}

	rows := make([][]sql.Value, 0, len(plan.rows))
	for _, r := range plan.rows {
		row := make([]sql.Value, len(plan.cols))

		for i, ce := range r {
			var v sql.Value

			if ce != nil {
				var err error
				v, err = ce.Eval(ctx, tx, nil)
				if err != nil {
					return -1, err
				}
			}

			row[i] = v
		}

		rows = append(rows, row)
	}

	err = tbl.Insert(ctx, rows)
	if err != nil {
		return -1, err
	}

	return int64(len(rows)), nil
}
