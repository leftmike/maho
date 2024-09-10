package sql_test

import (
	"testing"

	"github.com/leftmike/maho/parser/sql"
	"github.com/leftmike/maho/types"
)

func stringLiteral(s string) sql.Literal {
	return sql.Literal{types.StringValue(s)}
}

func int64Literal(i int64) sql.Literal {
	return sql.Literal{types.Int64Value(i)}
}

func TestInsert(t *testing.T) {
	tn := types.TableName{Table: types.ID("t", false)}

	cols1 := []types.Identifier{types.ID("c1", false)}
	cols2 := []types.Identifier{types.ID("c1", false), types.ID("c2", false)}
	cols4 := []types.Identifier{
		types.ID("c1", false),
		types.ID("c2", false),
		types.ID("c3", false),
		types.ID("c4", false),
	}

	cases := []struct {
		stmt sql.InsertValues
		s    string
	}{
		{
			stmt: sql.InsertValues{
				Table:   tn,
				Columns: cols4,
				Rows: [][]sql.Expr{
					{sql.Literal{nil}, sql.Literal{nil}, sql.Literal{nil}, sql.Literal{nil}}},
			},
			s: "INSERT INTO t (c1, c2, c3, c4) VALUES (NULL, NULL, NULL, NULL)",
		},
		{
			stmt: sql.InsertValues{
				Table:   tn,
				Columns: nil,
				Rows:    [][]sql.Expr{{sql.Literal{nil}, sql.Literal{nil}, sql.Literal{nil}, sql.Literal{nil}}},
			},
			s: "INSERT INTO t VALUES (NULL, NULL, NULL, NULL)",
		},
		{
			stmt: sql.InsertValues{
				Table:   tn,
				Columns: nil,
				Rows: [][]sql.Expr{
					{
						sql.Literal{types.BoolValue(true)},
						sql.Literal{types.StringValue("abcd")},
						sql.Literal{types.Float64Value(123.456)},
						sql.Literal{types.Int64Value(789)},
					},
				},
			},
			s: "INSERT INTO t VALUES (true, 'abcd', 123.456, 789)",
		},
		{
			stmt: sql.InsertValues{
				Table:   tn,
				Columns: cols2,
				Rows: [][]sql.Expr{
					{sql.Literal{types.BoolValue(false)}, sql.Literal{types.Int64Value(123)}},
					{sql.Literal{nil}, sql.Literal{types.Int64Value(456)}},
				},
			},
			s: "INSERT INTO t (c1, c2) VALUES (false, 123), (NULL, 456)",
		},
		{
			stmt: sql.InsertValues{
				Table:   tn,
				Columns: cols4,
				Rows: [][]sql.Expr{
					{
						sql.Literal{types.BoolValue(false)},
						sql.Literal{types.StringValue("efghi")},
						sql.Literal{types.Float64Value(987.654)},
						sql.Literal{types.Int64Value(321)},
					},
				},
			},
			s: "INSERT INTO t (c1, c2, c3, c4) VALUES (false, 'efghi', 987.654, 321)",
		},
		{
			stmt: sql.InsertValues{
				Table:   tn,
				Columns: cols1,
				Rows:    [][]sql.Expr{{sql.Literal{types.StringValue("123")}}},
			},
			s: "INSERT INTO t (c1) VALUES ('123')",
		},
		{
			stmt: sql.InsertValues{
				Table:   tn,
				Columns: cols1,
				Rows:    [][]sql.Expr{{sql.Literal{types.StringValue("123.456")}}},
			},
			s: "INSERT INTO t (c1) VALUES ('123.456')",
		},
		{
			stmt: sql.InsertValues{
				Table:   tn,
				Columns: cols2,
				Rows: [][]sql.Expr{
					{sql.Literal{nil}, sql.Literal{types.Float64Value(123.456)}},
				},
			},
			s: "INSERT INTO t (c1, c2) VALUES (NULL, 123.456)",
		},
		{
			stmt: sql.InsertValues{
				Table:   tn,
				Columns: nil,
				Rows: [][]sql.Expr{
					{sql.Literal{types.BoolValue(true)}, sql.Literal{types.BoolValue(false)}},
				},
			},
			s: "INSERT INTO t VALUES (true, false)",
		},
	}

	for _, c := range cases {
		s := c.stmt.String()
		if s != c.s {
			t.Errorf("%#v.String() got %s want %s", c.stmt, s, c.s)
		}
	}
}

func TestValues(t *testing.T) {
	cases := []struct {
		stmt sql.Values
		s    string
	}{
		{
			stmt: sql.Values{
				Expressions: [][]sql.Expr{
					{
						sql.Literal{types.Int64Value(1)},
						sql.Literal{types.StringValue("abc")},
						sql.Literal{types.BoolValue(true)},
						sql.Literal{nil},
					},
				},
			},
			s: "VALUES (1, 'abc', true, NULL)",
		},
		{
			stmt: sql.Values{
				Expressions: [][]sql.Expr{
					{
						sql.Literal{types.Int64Value(1)},
						sql.Literal{types.StringValue("abc")},
						sql.Literal{types.BoolValue(true)},
					},
					{
						sql.Literal{types.Int64Value(2)},
						sql.Literal{types.StringValue("def")},
						sql.Literal{types.BoolValue(false)},
					},
					{
						sql.Literal{types.Int64Value(3)},
						sql.Literal{types.StringValue("ghi")},
						sql.Literal{types.BoolValue(true)},
					},
					{
						sql.Literal{types.Int64Value(4)},
						sql.Literal{types.StringValue("jkl")},
						sql.Literal{types.BoolValue(false)},
					},
				},
			},
			s: "VALUES (1, 'abc', true), (2, 'def', false), (3, 'ghi', true), (4, 'jkl', false)",
		},
	}

	for _, c := range cases {
		s := c.stmt.String()
		if s != c.s {
			t.Errorf("%#v.String() got %s want %s", c.stmt, s, c.s)
		}
	}
}

func TestSelect(t *testing.T) {
	cases := []struct {
		stmt sql.Select
		s    string
	}{
		{
			stmt: sql.Select{
				From: &sql.FromTableAlias{
					TableName: types.TableName{
						Database: types.ID("db", false),
						Schema:   types.ID("sc", false),
						Table:    types.ID("tbl", false),
					},
					Alias: types.ID("alias", false),
				},
			},
			s: "SELECT * FROM db.sc.tbl AS alias",
		},
		{
			stmt: sql.Select{
				From: sql.FromStmt{
					Stmt: &sql.Values{
						Expressions: [][]sql.Expr{
							{
								sql.Literal{types.Int64Value(1)},
								sql.Literal{types.StringValue("abc")},
								sql.Literal{types.BoolValue(true)},
								sql.Literal{nil},
							},
						},
					},
					Alias: types.ID("vals", false),
					ColumnAliases: []types.Identifier{
						types.ID("c1", false),
						types.ID("c2", false),
						types.ID("c3", false),
						types.ID("c4", false),
					},
				},
			},
			s: "SELECT * FROM (VALUES (1, 'abc', true, NULL)) AS vals (c1, c2, c3, c4)",
		},
		{
			stmt: sql.Select{
				From: sql.FromStmt{
					Stmt: &sql.Values{
						Expressions: [][]sql.Expr{
							{
								sql.Literal{types.Int64Value(1)},
								sql.Literal{types.StringValue("abc")},
								sql.Literal{types.BoolValue(true)},
							},
							{
								sql.Literal{types.Int64Value(2)},
								sql.Literal{types.StringValue("def")},
								sql.Literal{types.BoolValue(false)},
							},
							{
								sql.Literal{types.Int64Value(3)},
								sql.Literal{types.StringValue("ghi")},
								sql.Literal{types.BoolValue(true)},
							},
							{
								sql.Literal{types.Int64Value(4)},
								sql.Literal{types.StringValue("jkl")},
								sql.Literal{types.BoolValue(false)},
							},
						},
					},
					Alias: types.ID("vals", false),
					ColumnAliases: []types.Identifier{
						types.ID("idx", false), types.ID("name", false), types.ID("flag", false)},
				},
			},
			s: "SELECT * FROM (VALUES (1, 'abc', true), (2, 'def', false), (3, 'ghi', true), (4, 'jkl', false)) AS vals (idx, name, flag)",
		},
		{
			stmt: sql.Select{
				From: sql.FromStmt{
					Stmt: &sql.Values{
						Expressions: [][]sql.Expr{
							{sql.Literal{nil}, sql.Literal{nil}, sql.Literal{nil}},
						},
					},
					Alias: types.ID("vals", false),
					ColumnAliases: []types.Identifier{
						types.ID("c1", false),
						types.ID("c2", false),
						types.ID("c3", false),
					},
				},
				Where: sql.Literal{types.BoolValue(false)},
			},
			s: "SELECT * FROM (VALUES (NULL, NULL, NULL)) AS vals (c1, c2, c3) WHERE false",
		},
		{
			stmt: sql.Select{
				From: &sql.FromIndexAlias{
					TableName: types.TableName{Table: types.ID("t", false)},
					Index:     types.ID("i", false),
				},
			},
			s: "SELECT * FROM t@i",
		},
		{
			stmt: sql.Select{
				From: &sql.FromTableAlias{TableName: types.TableName{Table: types.ID("t", false)}},
				Results: []sql.SelectResult{
					sql.TableResult{Table: types.ID("t", false)},
					sql.ExprResult{Expr: sql.Ref{types.ID("c1", false)}},
					sql.ExprResult{
						Expr:  sql.Ref{types.ID("c2", false)},
						Alias: types.ID("a2", false),
					},
				},
			},
			s: "SELECT t.*, c1, c2 AS a2 FROM t",
		},
		{
			stmt: sql.Select{
				From: &sql.FromTableAlias{TableName: types.TableName{Table: types.ID("t", false)}},
				Results: []sql.SelectResult{
					sql.ExprResult{Expr: sql.Ref{types.ID("c", false)}},
				},
				GroupBy: []sql.Expr{sql.Ref{types.ID("c", false)}},
			},
			s: "SELECT c FROM t GROUP BY c",
		},
		{
			stmt: sql.Select{
				From: &sql.FromTableAlias{TableName: types.TableName{Table: types.ID("t", false)}},
				Results: []sql.SelectResult{
					sql.ExprResult{Expr: sql.Ref{types.ID("c", false)}},
				},
				GroupBy: []sql.Expr{sql.Ref{types.ID("c", false)}, sql.Ref{types.ID("d", false)},
					&sql.BinaryExpr{Op: sql.AddOp, Left: sql.Ref{types.ID("e", false)},
						Right: sql.Ref{types.ID("f", false)}},
				},
			},
			s: "SELECT c FROM t GROUP BY c, d, (e + f)",
		},
		{
			stmt: sql.Select{
				From: &sql.FromTableAlias{TableName: types.TableName{Table: types.ID("t", false)}},
				Results: []sql.SelectResult{
					sql.ExprResult{Expr: sql.Ref{types.ID("c", false)}},
				},
				GroupBy: []sql.Expr{sql.Ref{types.ID("c", false)}},
				Having: &sql.BinaryExpr{Op: sql.GreaterThanOp, Left: sql.Ref{types.ID("c", false)},
					Right: int64Literal(1)},
			},
			s: "SELECT c FROM t GROUP BY c HAVING (c > 1)",
		},
		{
			stmt: sql.Select{
				From: &sql.FromTableAlias{TableName: types.TableName{Table: types.ID("t", false)}},
				OrderBy: []sql.OrderBy{
					{Expr: sql.Ref{types.ID("c1", false)}},
					{Expr: sql.Ref{types.ID("c2", false)}, Reverse: true},
				},
			},
			s: "SELECT * FROM t ORDER BY c1 ASC, c2 DESC",
		},
		{
			stmt: sql.Select{
				From: sql.FromJoin{
					Left: &sql.FromTableAlias{
						TableName: types.TableName{Table: types.ID("t1", false)},
					},
					Right: &sql.FromTableAlias{
						TableName: types.TableName{Table: types.ID("t2", false)},
					},
					Type: sql.CrossJoin,
				},
				Results: []sql.SelectResult{
					sql.ExprResult{Expr: sql.Ref{types.ID("t1", false), types.ID("c1", false)}},
					sql.ExprResult{Expr: sql.Ref{types.ID("t2", false), types.ID("c2", false)}},
				},
			},
			s: "SELECT t1.c1, t2.c2 FROM (t1 CROSS JOIN t2)",
		},
		{
			stmt: sql.Select{
				From: sql.FromJoin{
					Left: sql.FromJoin{
						Left: &sql.FromTableAlias{
							TableName: types.TableName{Table: types.ID("t1", false)},
						},
						Right: &sql.FromTableAlias{
							TableName: types.TableName{Table: types.ID("t2", false)},
						},
						Type: sql.CrossJoin,
					},
					Right: &sql.FromTableAlias{
						TableName: types.TableName{Table: types.ID("t3", false)},
					},
					Type: sql.CrossJoin,
				},
			},
			s: "SELECT * FROM ((t1 CROSS JOIN t2) CROSS JOIN t3)",
		},
		{
			stmt: sql.Select{
				From: sql.FromJoin{
					Left: sql.FromJoin{
						Left: &sql.FromTableAlias{
							TableName: types.TableName{Table: types.ID("t1", false)},
						},
						Right: &sql.FromTableAlias{
							TableName: types.TableName{Table: types.ID("t2", false)},
						},
						Type:  sql.Join,
						Using: []types.Identifier{types.ID("c1", false)},
					},
					Right: &sql.FromTableAlias{
						TableName: types.TableName{Table: types.ID("t3", false)},
					},
					Type: sql.CrossJoin,
				},
			},
			s: "SELECT * FROM ((t1 JOIN t2 USING (c1)) CROSS JOIN t3)",
		},
		{
			stmt: sql.Select{
				From: sql.FromJoin{
					Left: sql.FromJoin{
						Left: &sql.FromTableAlias{
							TableName: types.TableName{Table: types.ID("t1", false)},
						},
						Right: &sql.FromTableAlias{
							TableName: types.TableName{Table: types.ID("t2", false)},
						},
						Type: sql.CrossJoin,
					},
					Right: &sql.FromTableAlias{
						TableName: types.TableName{Table: types.ID("t3", false)},
					},
					Type:  sql.RightJoin,
					Using: []types.Identifier{types.ID("c1", false)},
				},
			},
			s: "SELECT * FROM ((t1 CROSS JOIN t2) RIGHT JOIN t3 USING (c1))",
		},
		{
			stmt: sql.Select{
				From: sql.FromJoin{
					Left: &sql.FromTableAlias{
						TableName: types.TableName{Table: types.ID("t1", false)},
					},
					Right: &sql.FromTableAlias{
						TableName: types.TableName{Table: types.ID("t2", false)},
					},
					Type: sql.Join,
					On: &sql.BinaryExpr{Op: sql.GreaterThanOp,
						Left: sql.Ref{types.ID("c1", false)}, Right: int64Literal(5)},
				},
			},
			s: "SELECT * FROM (t1 JOIN t2 ON (c1 > 5))",
		},
		{
			stmt: sql.Select{
				From: sql.FromJoin{
					Left: &sql.FromTableAlias{
						TableName: types.TableName{Table: types.ID("t1", false)},
					},
					Right: &sql.FromTableAlias{
						TableName: types.TableName{Table: types.ID("t2", false)},
					},
					Type: sql.Join,
					Using: []types.Identifier{
						types.ID("c1", false),
						types.ID("c2", false),
						types.ID("c3", false),
					},
				},
			},
			s: "SELECT * FROM (t1 JOIN t2 USING (c1, c2, c3))",
		},
	}

	for _, c := range cases {
		s := c.stmt.String()
		if s != c.s {
			t.Errorf("%#v.String() got %s want %s", c.stmt, s, c.s)
		}
	}
}

func TestDelete(t *testing.T) {
	cases := []struct {
		stmt sql.Delete
		s    string
	}{
		{
			stmt: sql.Delete{
				Table: types.TableName{Table: types.ID("t", false)},
			},
			s: "DELETE FROM t",
		},
		{
			stmt: sql.Delete{
				Table: types.TableName{Table: types.ID("t", false)},
				Where: sql.Literal{types.BoolValue(true)},
			},
			s: "DELETE FROM t WHERE true",
		},
	}

	for _, c := range cases {
		s := c.stmt.String()
		if s != c.s {
			t.Errorf("%#v.String() got %s want %s", c.stmt, s, c.s)
		}
	}
}

func TestUpdate(t *testing.T) {
	cases := []struct {
		stmt sql.Update
		s    string
	}{
		{
			stmt: sql.Update{
				Table: types.TableName{Table: types.ID("t", false)},
				ColumnUpdates: []sql.ColumnUpdate{
					{types.ID("c1", false), sql.Literal{types.Int64Value(1)}},
					{types.ID("c2", false), sql.Literal{types.Int64Value(2)}},
					{types.ID("c3", false), sql.Literal{types.Int64Value(3)}},
				},
			},
			s: "UPDATE t SET c1 = 1, c2 = 2, c3 = 3",
		},
		{
			stmt: sql.Update{
				Table: types.TableName{Table: types.ID("t", false)},
				ColumnUpdates: []sql.ColumnUpdate{
					{types.ID("c1", false), sql.Literal{types.Int64Value(1)}},
				},
				Where: sql.Literal{types.BoolValue(true)},
			},
			s: "UPDATE t SET c1 = 1 WHERE true",
		},
	}

	for _, c := range cases {
		s := c.stmt.String()
		if s != c.s {
			t.Errorf("%#v.String() got %s want %s", c.stmt, s, c.s)
		}
	}
}
