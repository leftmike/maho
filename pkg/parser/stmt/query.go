package stmt

import (
	"fmt"
	"io"

	"github.com/leftmike/maho/pkg/parser/expr"
	"github.com/leftmike/maho/pkg/types"
)

type FromItem interface {
	String() string
}

type FromTableAlias struct {
	types.TableName
	Alias types.Identifier
}

func (fta FromTableAlias) String() string {
	s := fta.TableName.String()
	if fta.Alias != 0 {
		s += fmt.Sprintf(" AS %s", fta.Alias)
	}
	return s
}

type FromIndexAlias struct {
	types.TableName
	Index types.Identifier
	Alias types.Identifier
}

func (fia FromIndexAlias) String() string {
	s := fmt.Sprintf("%s@%s", fia.TableName, fia.Index)
	if fia.Alias != 0 {
		s += fmt.Sprintf(" AS %s", fia.Alias)
	}
	return s
}

type FromStmt struct {
	Stmt          Stmt
	Alias         types.Identifier
	ColumnAliases []types.Identifier
}

func (fs FromStmt) String() string {
	s := fmt.Sprintf("(%s) AS %s", fs.Stmt, fs.Alias)
	if fs.ColumnAliases != nil {
		s += " ("
		for i, col := range fs.ColumnAliases {
			if i > 0 {
				s += ", "
			}
			s += col.String()
		}
		s += ")"
	}
	return s
}

type Copy struct {
	Table     types.TableName
	Columns   []types.Identifier
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

type Delete struct {
	Table types.TableName
	Where expr.Expr
}

func (stmt *Delete) String() string {
	s := fmt.Sprintf("DELETE FROM %s", stmt.Table)
	if stmt.Where != nil {
		s += fmt.Sprintf(" WHERE %s", stmt.Where)
	}
	return s
}

type InsertValues struct {
	Table   types.TableName
	Columns []types.Identifier
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

type ColumnUpdate struct {
	Column types.Identifier
	Expr   expr.Expr
}

type Update struct {
	Table         types.TableName
	ColumnUpdates []ColumnUpdate
	Where         expr.Expr
}

func (stmt *Update) String() string {
	s := fmt.Sprintf("UPDATE %s SET ", stmt.Table)
	for i, cu := range stmt.ColumnUpdates {
		if i > 0 {
			s += ", "
		}
		s += fmt.Sprintf("%s = %s", cu.Column, cu.Expr)
	}
	if stmt.Where != nil {
		s += fmt.Sprintf(" WHERE %s", stmt.Where)
	}
	return s
}

type Values struct {
	Expressions [][]expr.Expr
}

func (stmt *Values) String() string {
	s := "VALUES"
	for i, r := range stmt.Expressions {
		if i > 0 {
			s += ", ("
		} else {
			s += " ("
		}

		for j, v := range r {
			if j > 0 {
				s += ", "
			}
			s += v.String()
		}

		s += ")"
	}

	return s
}

type SelectResult interface {
	String() string
}

type TableResult struct {
	Table types.Identifier
}

type ExprResult struct {
	Expr  expr.Expr
	Alias types.Identifier
}

type OrderBy struct {
	Expr    expr.Expr
	Reverse bool
}

type Select struct {
	Results []SelectResult
	From    FromItem
	Where   expr.Expr
	GroupBy []expr.Expr
	Having  expr.Expr
	OrderBy []OrderBy
}

func (tr TableResult) String() string {
	return fmt.Sprintf("%s.*", tr.Table)
}

func (er ExprResult) String() string {
	s := er.Expr.String()
	if er.Alias != 0 {
		s += fmt.Sprintf(" AS %s", er.Alias)
	}
	return s
}

func (stmt *Select) String() string {
	s := "SELECT "
	if stmt.Results == nil {
		s += "*"
	} else {
		for i, sr := range stmt.Results {
			if i > 0 {
				s += ", "
			}
			s += sr.String()
		}
	}
	s += fmt.Sprintf(" FROM %s", stmt.From)
	if stmt.Where != nil {
		s += fmt.Sprintf(" WHERE %s", stmt.Where)
	}
	if stmt.GroupBy != nil {
		s += " GROUP BY "
		for i, e := range stmt.GroupBy {
			if i > 0 {
				s += ", "
			}
			s += e.String()
		}
		if stmt.Having != nil {
			s += fmt.Sprintf(" HAVING %s", stmt.Having)
		}
	}
	if stmt.OrderBy != nil {
		s += " ORDER BY "
		for i, by := range stmt.OrderBy {
			if i > 0 {
				s += ", "
			}
			s += by.Expr.String()
			if by.Reverse {
				s += " DESC"
			} else {
				s += " ASC"
			}
		}
	}
	return s
}

type JoinType int

const (
	NoJoin JoinType = iota

	Join      // INNER JOIN
	LeftJoin  // LEFT OUTER JOIN
	RightJoin // RIGHT OUTER JOIN
	FullJoin  // FULL OUTER JOIN
	CrossJoin
)

var joinType = map[JoinType]string{
	Join:      "JOIN",
	LeftJoin:  "LEFT JOIN",
	RightJoin: "RIGHT JOIN",
	FullJoin:  "FULL JOIN",
	CrossJoin: "CROSS JOIN",
}

type FromJoin struct {
	Left  FromItem
	Right FromItem
	Type  JoinType
	On    expr.Expr
	Using []types.Identifier
}

func (jt JoinType) String() string {
	return joinType[jt]
}

func (fj FromJoin) String() string {
	s := fmt.Sprintf("%s %s %s", fj.Left, fj.Type.String(), fj.Right)
	if fj.On != nil {
		s += fmt.Sprintf(" ON %s", fj.On.String())
	}
	if len(fj.Using) > 0 {
		s += " USING ("
		for i, id := range fj.Using {
			if i > 0 {
				s += ", "
			}
			s += id.String()
		}
		s += ")"
	}
	return s
}
