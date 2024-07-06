package sql

import (
	"fmt"
	"io"
	"strings"

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
	if fta.Alias != 0 {
		return fmt.Sprintf("%s AS %s", fta.TableName, fta.Alias)
	}
	return fta.TableName.String()
}

type FromIndexAlias struct {
	types.TableName
	Index types.Identifier
	Alias types.Identifier
}

func (fia FromIndexAlias) String() string {
	if fia.Alias != 0 {
		return fmt.Sprintf("%s@%s AS %s", fia.TableName, fia.Index, fia.Alias)
	}
	return fmt.Sprintf("%s@%s", fia.TableName, fia.Index)
}

type FromStmt struct {
	Stmt          Stmt
	Alias         types.Identifier
	ColumnAliases []types.Identifier
}

func (fs FromStmt) String() string {
	var buf strings.Builder
	fmt.Fprintf(&buf, "(%s) AS %s", fs.Stmt, fs.Alias)
	if fs.ColumnAliases != nil {
		buf.WriteString(" (")
		for i, col := range fs.ColumnAliases {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(col.String())
		}
		buf.WriteRune(')')
	}
	return buf.String()
}

type Copy struct {
	Table     types.TableName
	Columns   []types.Identifier
	From      io.RuneReader
	FromLine  int
	Delimiter rune
}

func (stmt *Copy) String() string {
	var buf strings.Builder
	fmt.Fprintf(&buf, "COPY %s (", stmt.Table)
	for i, col := range stmt.Columns {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(col.String())
	}
	buf.WriteString(") FROM STDIN")

	if stmt.Delimiter != '\t' {
		fmt.Fprintf(&buf, " DELIMITER '%c'", stmt.Delimiter)
	}
	return buf.String()
}

type Delete struct {
	Table types.TableName
	Where Expr
}

func (stmt *Delete) String() string {
	if stmt.Where != nil {
		return fmt.Sprintf("DELETE FROM %s WHERE %s", stmt.Table, stmt.Where)
	}
	return fmt.Sprintf("DELETE FROM %s", stmt.Table)
}

type InsertValues struct {
	Table   types.TableName
	Columns []types.Identifier
	Rows    [][]Expr
}

func (stmt *InsertValues) String() string {
	var buf strings.Builder
	fmt.Fprintf(&buf, "INSERT INTO %s ", stmt.Table)

	if stmt.Columns != nil {
		buf.WriteRune('(')
		for i, col := range stmt.Columns {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(col.String())
		}
		buf.WriteString(") ")
	}

	buf.WriteString("VALUES")

	for i, r := range stmt.Rows {
		if i > 0 {
			buf.WriteString(", (")
		} else {
			buf.WriteString(" (")
		}

		for j, v := range r {
			if j > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(v.String())
		}

		buf.WriteRune(')')
	}

	return buf.String()
}

type ColumnUpdate struct {
	Column types.Identifier
	Expr   Expr
}

type Update struct {
	Table         types.TableName
	ColumnUpdates []ColumnUpdate
	Where         Expr
}

func (stmt *Update) String() string {
	var buf strings.Builder
	fmt.Fprintf(&buf, "UPDATE %s SET ", stmt.Table)
	for i, cu := range stmt.ColumnUpdates {
		if i > 0 {
			buf.WriteString(", ")
		}
		fmt.Fprintf(&buf, "%s = %s", cu.Column, cu.Expr)
	}
	if stmt.Where != nil {
		fmt.Fprintf(&buf, " WHERE %s", stmt.Where)
	}
	return buf.String()
}

type Values struct {
	Expressions [][]Expr
}

func (stmt *Values) String() string {
	var buf strings.Builder
	buf.WriteString("VALUES")
	for i, r := range stmt.Expressions {
		if i > 0 {
			buf.WriteString(", (")
		} else {
			buf.WriteString(" (")
		}

		for j, v := range r {
			if j > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(v.String())
		}

		buf.WriteRune(')')
	}
	return buf.String()
}

type SelectResult interface {
	String() string
}

type TableResult struct {
	Table types.Identifier
}

type ExprResult struct {
	Expr  Expr
	Alias types.Identifier
}

type OrderBy struct {
	Expr    Expr
	Reverse bool
}

type Select struct {
	Results []SelectResult
	From    FromItem
	Where   Expr
	GroupBy []Expr
	Having  Expr
	OrderBy []OrderBy
}

func (tr TableResult) String() string {
	return fmt.Sprintf("%s.*", tr.Table)
}

func (er ExprResult) String() string {
	if er.Alias != 0 {
		return fmt.Sprintf("%s AS %s", er.Expr, er.Alias)
	}
	return er.Expr.String()
}

func (stmt *Select) String() string {
	var buf strings.Builder
	buf.WriteString("SELECT ")
	if stmt.Results == nil {
		buf.WriteRune('*')
	} else {
		for i, sr := range stmt.Results {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(sr.String())
		}
	}
	fmt.Fprintf(&buf, " FROM %s", stmt.From)
	if stmt.Where != nil {
		fmt.Fprintf(&buf, " WHERE %s", stmt.Where)
	}
	if stmt.GroupBy != nil {
		buf.WriteString(" GROUP BY ")
		for i, e := range stmt.GroupBy {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(e.String())
		}
		if stmt.Having != nil {
			buf.WriteString(fmt.Sprintf(" HAVING %s", stmt.Having))
		}
	}
	if stmt.OrderBy != nil {
		buf.WriteString(" ORDER BY ")
		for i, by := range stmt.OrderBy {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(by.Expr.String())
			if by.Reverse {
				buf.WriteString(" DESC")
			} else {
				buf.WriteString(" ASC")
			}
		}
	}
	return buf.String()
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
	On    Expr
	Using []types.Identifier
}

func (jt JoinType) String() string {
	return joinType[jt]
}

func (fj FromJoin) String() string {
	var buf strings.Builder
	fmt.Fprintf(&buf, "(%s %s %s", fj.Left, fj.Type.String(), fj.Right)
	if fj.On != nil {
		fmt.Fprintf(&buf, " ON %s", fj.On.String())
	}
	if len(fj.Using) > 0 {
		buf.WriteString(" USING (")
		for i, id := range fj.Using {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(id.String())
		}
		buf.WriteRune(')')
	}
	buf.WriteRune(')')
	return buf.String()
}
