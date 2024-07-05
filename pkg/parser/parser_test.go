package parser

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/leftmike/maho/pkg/parser/sql"
	"github.com/leftmike/maho/pkg/parser/token"
	"github.com/leftmike/maho/pkg/types"
)

func stringLiteral(s string) sql.Literal {
	return sql.Literal{types.StringValue(s)}
}

func int64Literal(i int64) sql.Literal {
	return sql.Literal{types.Int64Value(i)}
}

var (
	trueLiteral  = sql.Literal{types.BoolValue(true)}
	falseLiteral = sql.Literal{types.BoolValue(false)}
	nilLiteral   = sql.Literal{nil}
)

func TestScan(t *testing.T) {
	s := `create foobar * 123 (,) 'string' "identifier" ; 456.789`
	tokens := []rune{token.Reserved, token.Identifier, token.Star, token.Integer, token.LParen,
		token.Comma, token.RParen, token.String, token.Identifier, token.EndOfStatement,
		token.Float, token.EOF}
	p := NewParser(strings.NewReader(s), "scan")
	for _, e := range tokens {
		r := p.scan()
		if e != r {
			t.Errorf("scan(%q) got %s want %s", s, token.Format(r), token.Format(e))
		}
	}

	p = NewParser(strings.NewReader(s), "scan")
	for i := 0; i < len(tokens); i++ {
		if i >= lookBackAmount {
			for j := 0; j < lookBackAmount; j++ {
				p.unscan()
			}
			for j := lookBackAmount; j > 0; j-- {
				r := p.scan()
				if tokens[i-j] != r {
					t.Errorf("scan(%q) got %s want %s", s, token.Format(r),
						token.Format(tokens[i-j]))
				}
			}
		}

		r := p.scan()
		if tokens[i] != r {
			t.Errorf("scan(%q) got %s want %s", s, token.Format(r), token.Format(tokens[i]))
		}
	}
}

func TestParse(t *testing.T) {
	failed := []string{
		"create foobar",
		"create temp index",
		"create unique table",
		"create table if not my-table",
		"create table (my-table)",
		"create table .my-table",
		"create table my-schema.",
	}

	for i, f := range failed {
		p := NewParser(strings.NewReader(f), fmt.Sprintf("failed[%d]", i))
		stmt, err := p.Parse()
		if stmt != nil || err == nil {
			t.Errorf("Parse(%q) did not fail", f)
		}
	}
}

func TestCreateTable(t *testing.T) {
	cases := []struct {
		s    string
		stmt sql.CreateTable
		fail bool
	}{
		{s: "create temp table t (c int)", fail: true},
		{s: "create temporary table t (c int)", fail: true},
		{s: "create table test ()", fail: true},
		{s: "create table test (c)", fail: true},
		{s: "create table (c int)", fail: true},
		{s: "create table t (c int, c bool)", fail: true},
		{s: "create table t (c int, d bool, c char(1))", fail: true},
		{s: "create table t (c int) default", fail: true},
		{s: "create table . (c int)", fail: true},
		{s: "create table .t (c int)", fail: true},
		{s: "create table d. (c int)", fail: true},
		{s: "create table t (c int, )", fail: true},
		{s: "create table t (c bool())", fail: true},
		{s: "create table t (c bool(1))", fail: true},
		{s: "create table t (c double())", fail: true},
		{s: "create table t (c double(1,2,3))", fail: true},
		{s: "create table t (c double(0))", fail: true},
		{s: "create table t (c double(256))", fail: true},
		{s: "create table t (c double(0,15))", fail: true},
		{s: "create table t (c double(256,15))", fail: true},
		{s: "create table t (c double(123,-1))", fail: true},
		{s: "create table t (c double(123,31))", fail: true},
		{s: "create table t (c int())", fail: true},
		{s: "create table t (c int(1,2))", fail: true},
		{s: "create table t (c int(0))", fail: true},
		{s: "create table t (c int(256))", fail: true},
		{s: "create table t (c char(1,2))", fail: true},
		{s: "create table t (c char(-1))", fail: true},
		{s: "create table t (c blob binary)", fail: true},
		{s: "create table t (c int binary)", fail: true},
		{s: "create table t (c bool binary)", fail: true},
		{s: "create table t (c char(123) binary)", fail: true},
		{s: "create table t (c double binary)", fail: true},
		{s: "create table t (c char null)", fail: true},
		{s: "create table t (c char null, d int)", fail: true},
		{s: "create table t (c char not null not null)", fail: true},
		{s: "create table t (c char default)", fail: true},
		{s: "create table t (c char default, d int)", fail: true},
		{s: "create table t (c int default 0 default 1)", fail: true},
		{
			s: "create table t (c1 int2, c2 smallint, c3 int4, c4 integer, c5 bigint, c6 int8)",
			stmt: sql.CreateTable{
				Table: types.TableName{Table: types.ID("t", false)},
				Columns: []types.Identifier{
					types.ID("c1", false),
					types.ID("c2", false),
					types.ID("c3", false),
					types.ID("c4", false),
					types.ID("c5", false),
					types.ID("c6", false),
				},
				ColumnTypes: []types.ColumnType{
					{Type: types.Int64Type, Size: 2},
					{Type: types.Int64Type, Size: 2},
					{Type: types.Int64Type, Size: 4},
					{Type: types.Int64Type, Size: 4},
					{Type: types.Int64Type, Size: 8},
					{Type: types.Int64Type, Size: 8},
				},
				ColumnDefaults: []sql.Expr{nil, nil, nil, nil, nil, nil},
			},
		},
		{
			s: "create table if not exists t (c int)",
			stmt: sql.CreateTable{
				Table:          types.TableName{Table: types.ID("t", false)},
				Columns:        []types.Identifier{types.ID("c", false)},
				ColumnTypes:    []types.ColumnType{{Type: types.Int64Type, Size: 4}},
				ColumnDefaults: []sql.Expr{nil},
				IfNotExists:    true,
			},
		},
		{
			s: "create table t (b1 bool, b2 boolean, d1 double, d2 double)",
			stmt: sql.CreateTable{
				Table: types.TableName{Table: types.ID("t", false)},
				Columns: []types.Identifier{
					types.ID("b1", false),
					types.ID("b2", false),
					types.ID("d1", false),
					types.ID("d2", false),
				},
				ColumnTypes: []types.ColumnType{
					{Type: types.BoolType, Size: 1},
					{Type: types.BoolType, Size: 1},
					{Type: types.Float64Type, Size: 8},
					{Type: types.Float64Type, Size: 8},
				},
				ColumnDefaults: []sql.Expr{nil, nil, nil, nil},
			},
		},
		{
			s: "create table t (b1 binary, b2 varbinary(123), b3 blob, b4 bytes, b5 bytea)",
			stmt: sql.CreateTable{
				Table: types.TableName{Table: types.ID("t", false)},
				Columns: []types.Identifier{
					types.ID("b1", false),
					types.ID("b2", false),
					types.ID("b3", false),
					types.ID("b4", false),
					types.ID("b5", false),
				},
				ColumnTypes: []types.ColumnType{
					{Type: types.BytesType, Fixed: true, Size: 1},
					{Type: types.BytesType, Fixed: false, Size: 123},
					{Type: types.BytesType, Fixed: false, Size: types.MaxColumnSize},
					{Type: types.BytesType, Fixed: false, Size: types.MaxColumnSize},
					{Type: types.BytesType, Fixed: false, Size: types.MaxColumnSize},
				},
				ColumnDefaults: []sql.Expr{nil, nil, nil, nil, nil},
			},
		},
		{
			s: "create table t (b1 binary(123), b2 varbinary(456), b3 blob(789))",
			stmt: sql.CreateTable{
				Table: types.TableName{Table: types.ID("t", false)},
				Columns: []types.Identifier{
					types.ID("b1", false),
					types.ID("b2", false),
					types.ID("b3", false),
				},
				ColumnTypes: []types.ColumnType{
					{Type: types.BytesType, Fixed: true, Size: 123},
					{Type: types.BytesType, Fixed: false, Size: 456},
					{Type: types.BytesType, Fixed: false, Size: 789},
				},
				ColumnDefaults: []sql.Expr{nil, nil, nil},
			},
		},
		{
			s: "create table t (b1 bytea(456), b2 bytes(789))",
			stmt: sql.CreateTable{
				Table:   types.TableName{Table: types.ID("t", false)},
				Columns: []types.Identifier{types.ID("b1", false), types.ID("b2", false)},
				ColumnTypes: []types.ColumnType{
					{Type: types.BytesType, Fixed: false, Size: 456},
					{Type: types.BytesType, Fixed: false, Size: 789},
				},
				ColumnDefaults: []sql.Expr{nil, nil},
			},
		},
		{
			s: "create table t (c1 char, c2 varchar(123), c3 text)",
			stmt: sql.CreateTable{
				Table: types.TableName{Table: types.ID("t", false)},
				Columns: []types.Identifier{
					types.ID("c1", false),
					types.ID("c2", false),
					types.ID("c3", false),
				},
				ColumnTypes: []types.ColumnType{
					{Type: types.StringType, Fixed: true, Size: 1},
					{Type: types.StringType, Fixed: false, Size: 123},
					{Type: types.StringType, Fixed: false, Size: types.MaxColumnSize},
				},
				ColumnDefaults: []sql.Expr{nil, nil, nil},
			},
		},
		{
			s: "create table t (c1 char(123), c2 varchar(456), c3 text(789))",
			stmt: sql.CreateTable{
				Table: types.TableName{Table: types.ID("t", false)},
				Columns: []types.Identifier{
					types.ID("c1", false),
					types.ID("c2", false),
					types.ID("c3", false),
				},
				ColumnTypes: []types.ColumnType{
					{Type: types.StringType, Fixed: true, Size: 123},
					{Type: types.StringType, Fixed: false, Size: 456},
					{Type: types.StringType, Fixed: false, Size: 789},
				},
				ColumnDefaults: []sql.Expr{nil, nil, nil},
			},
		},
		{
			s: "create table t (c1 varchar(64) default 'abcd', c2 int default 123)",
			stmt: sql.CreateTable{
				Table:   types.TableName{Table: types.ID("t", false)},
				Columns: []types.Identifier{types.ID("c1", false), types.ID("c2", false)},
				ColumnTypes: []types.ColumnType{
					{Type: types.StringType, Fixed: false, Size: 64},
					{Type: types.Int64Type, Size: 4},
				},
				ColumnDefaults: []sql.Expr{stringLiteral("abcd"), int64Literal(123)},
			},
		},
		{
			s: "create table t (c1 boolean default true, c2 boolean not null)",
			stmt: sql.CreateTable{
				Table:   types.TableName{Table: types.ID("t", false)},
				Columns: []types.Identifier{types.ID("c1", false), types.ID("c2", false)},
				ColumnTypes: []types.ColumnType{
					{Type: types.BoolType, Size: 1},
					{Type: types.BoolType, Size: 1, NotNull: true},
				},
				ColumnDefaults: []sql.Expr{trueLiteral, nil},
			},
		},
		{
			s: `create table t (c1 boolean default true not null,
c2 boolean not null default true)`,
			stmt: sql.CreateTable{
				Table:   types.TableName{Table: types.ID("t", false)},
				Columns: []types.Identifier{types.ID("c1", false), types.ID("c2", false)},
				ColumnTypes: []types.ColumnType{
					{Type: types.BoolType, Size: 1, NotNull: true},
					{Type: types.BoolType, Size: 1, NotNull: true},
				},
				ColumnDefaults: []sql.Expr{trueLiteral, trueLiteral},
			},
		},
		{s: "create table t (c1 int primary, c2 bool)", fail: true},
		{s: "create table t (c1 int unique primary key, c2 bool)", fail: true},
		{s: "create table t (c1 int, c2 bool, primary)", fail: true},
		{s: "create table t (c1 int, c2 bool, primary key)", fail: true},
		{s: "create table t (c1 int, c2 bool, primary key ())", fail: true},
		{s: "create table t (c1 int primary key, c2 bool, primary key (c1))", fail: true},
		{s: "create table t (c1 int, c2 bool primary key, primary key (c1))", fail: true},
		{
			s: "create table t (c1 int primary key, c2 bool)",
			stmt: sql.CreateTable{
				Table:   types.TableName{Table: types.ID("t", false)},
				Columns: []types.Identifier{types.ID("c1", false), types.ID("c2", false)},
				ColumnTypes: []types.ColumnType{
					{Type: types.Int64Type, Size: 4},
					{Type: types.BoolType, Size: 1},
				},
				ColumnDefaults: []sql.Expr{nil, nil},
				Constraints: []sql.Constraint{
					{
						Type:   sql.PrimaryConstraint,
						Name:   types.ID("c1_primary", false),
						ColNum: -1,
						Key: sql.IndexKey{
							Unique:  true,
							Columns: []types.Identifier{types.ID("c1", false)},
							Reverse: []bool{false},
						},
					},
				},
			},
		},
		{
			s: "create table t (c1 int unique, c2 bool)",
			stmt: sql.CreateTable{
				Table:   types.TableName{Table: types.ID("t", false)},
				Columns: []types.Identifier{types.ID("c1", false), types.ID("c2", false)},
				ColumnTypes: []types.ColumnType{
					{Type: types.Int64Type, Size: 4},
					{Type: types.BoolType, Size: 1},
				},
				ColumnDefaults: []sql.Expr{nil, nil},
				Constraints: []sql.Constraint{
					{
						Type:   sql.UniqueConstraint,
						Name:   types.ID("c1_unique", false),
						ColNum: -1,
						Key: sql.IndexKey{
							Unique:  true,
							Columns: []types.Identifier{types.ID("c1", false)},
							Reverse: []bool{false},
						},
					},
				},
			},
		},
		{
			s: "create table t (c1 int, c2 bool, primary key (c1))",
			stmt: sql.CreateTable{
				Table:   types.TableName{Table: types.ID("t", false)},
				Columns: []types.Identifier{types.ID("c1", false), types.ID("c2", false)},
				ColumnTypes: []types.ColumnType{
					{Type: types.Int64Type, Size: 4},
					{Type: types.BoolType, Size: 1},
				},
				ColumnDefaults: []sql.Expr{nil, nil},
				Constraints: []sql.Constraint{
					{
						Type:   sql.PrimaryConstraint,
						Name:   types.ID("c1_primary", false),
						ColNum: -1,
						Key: sql.IndexKey{
							Unique:  true,
							Columns: []types.Identifier{types.ID("c1", false)},
							Reverse: []bool{false},
						},
					},
				},
			},
		},
		{
			s: "create table t (c1 int, c2 bool, primary key (c1 desc))",
			stmt: sql.CreateTable{
				Table:   types.TableName{Table: types.ID("t", false)},
				Columns: []types.Identifier{types.ID("c1", false), types.ID("c2", false)},
				ColumnTypes: []types.ColumnType{
					{Type: types.Int64Type, Size: 4},
					{Type: types.BoolType, Size: 1},
				},
				ColumnDefaults: []sql.Expr{nil, nil},
				Constraints: []sql.Constraint{
					{
						Type:   sql.PrimaryConstraint,
						Name:   types.ID("c1_primary", false),
						ColNum: -1,
						Key: sql.IndexKey{
							Unique:  true,
							Columns: []types.Identifier{types.ID("c1", false)},
							Reverse: []bool{true},
						},
					},
				},
			},
		},
		{
			s: "create table t (c1 int unique, c2 bool unique, primary key (c1 desc, c2 asc))",
			stmt: sql.CreateTable{
				Table:   types.TableName{Table: types.ID("t", false)},
				Columns: []types.Identifier{types.ID("c1", false), types.ID("c2", false)},
				ColumnTypes: []types.ColumnType{
					{Type: types.Int64Type, Size: 4},
					{Type: types.BoolType, Size: 1},
				},
				ColumnDefaults: []sql.Expr{nil, nil},
				Constraints: []sql.Constraint{
					{
						Type:   sql.UniqueConstraint,
						Name:   types.ID("c1_unique", false),
						ColNum: -1,
						Key: sql.IndexKey{
							Unique:  true,
							Columns: []types.Identifier{types.ID("c1", false)},
							Reverse: []bool{false},
						},
					},
					{
						Type:   sql.UniqueConstraint,
						Name:   types.ID("c2_unique", false),
						ColNum: -1,
						Key: sql.IndexKey{
							Unique:  true,
							Columns: []types.Identifier{types.ID("c2", false)},
							Reverse: []bool{false},
						},
					},
					{
						Type:   sql.PrimaryConstraint,
						Name:   types.ID("c1_c2_primary", false),
						ColNum: -1,
						Key: sql.IndexKey{
							Unique: true,
							Columns: []types.Identifier{
								types.ID("c1", false),
								types.ID("c2", false),
							},
							Reverse: []bool{true, false},
						},
					},
				},
			},
		},
		{
			s: "create table t (c1 int primary key, c2 bool, unique (c2, c1))",
			stmt: sql.CreateTable{
				Table:   types.TableName{Table: types.ID("t", false)},
				Columns: []types.Identifier{types.ID("c1", false), types.ID("c2", false)},
				ColumnTypes: []types.ColumnType{
					{Type: types.Int64Type, Size: 4},
					{Type: types.BoolType, Size: 1},
				},
				ColumnDefaults: []sql.Expr{nil, nil},
				Constraints: []sql.Constraint{
					{
						Type:   sql.PrimaryConstraint,
						Name:   types.ID("c1_primary", false),
						ColNum: -1,
						Key: sql.IndexKey{
							Unique:  true,
							Columns: []types.Identifier{types.ID("c1", false)},
							Reverse: []bool{false},
						},
					},
					{
						Type:   sql.UniqueConstraint,
						Name:   types.ID("c2_c1_unique", false),
						ColNum: -1,
						Key: sql.IndexKey{
							Unique: true,
							Columns: []types.Identifier{
								types.ID("c2", false),
								types.ID("c1", false),
							},
							Reverse: []bool{false, false},
						},
					},
				},
			},
		},
		{
			s: `create table t (c1 int constraint con1 primary key, c2 bool,
constraint con2 unique (c2, c1))`,
			stmt: sql.CreateTable{
				Table:   types.TableName{Table: types.ID("t", false)},
				Columns: []types.Identifier{types.ID("c1", false), types.ID("c2", false)},
				ColumnTypes: []types.ColumnType{
					{Type: types.Int64Type, Size: 4},
					{Type: types.BoolType, Size: 1},
				},
				ColumnDefaults: []sql.Expr{nil, nil},
				Constraints: []sql.Constraint{
					{
						Type:   sql.PrimaryConstraint,
						Name:   types.ID("con1", false),
						ColNum: -1,
						Key: sql.IndexKey{
							Unique:  true,
							Columns: []types.Identifier{types.ID("c1", false)},
							Reverse: []bool{false},
						},
					},
					{
						Type:   sql.UniqueConstraint,
						Name:   types.ID("con2", false),
						ColNum: -1,
						Key: sql.IndexKey{
							Unique: true,
							Columns: []types.Identifier{
								types.ID("c2", false),
								types.ID("c1", false),
							},
							Reverse: []bool{false, false},
						},
					},
				},
			},
		},
		{
			s: `create table t (c1 int constraint not_null not null,
c2 bool constraint dflt default true)`,
			stmt: sql.CreateTable{
				Table:   types.TableName{Table: types.ID("t", false)},
				Columns: []types.Identifier{types.ID("c1", false), types.ID("c2", false)},
				ColumnTypes: []types.ColumnType{
					{Type: types.Int64Type, Size: 4, NotNull: true},
					{Type: types.BoolType, Size: 1},
				},
				ColumnDefaults: []sql.Expr{nil, trueLiteral},
				Constraints: []sql.Constraint{
					{Type: sql.NotNullConstraint, Name: types.ID("not_null", false), ColNum: 0},
					{Type: sql.DefaultConstraint, Name: types.ID("dflt", false), ColNum: 1},
				},
			},
		},
		{
			s:    "create table t (c1 int not null constraint not_null, c2 bool)",
			fail: true,
		},
		{
			s:    "create table t (c1 int, c2 bool not null constraint not_null)",
			fail: true,
		},
		{
			s:    "create table t (c1 int constraint c1 constraint c1 not null, c2 bool)",
			fail: true,
		},
		{
			s:    "create table t (c1 int constraint c1, c2 bool)",
			fail: true,
		},
		{
			s: `create table t (c1 int constraint c2_c1_unique primary key, c2 bool,
unique (c2, c1))`,
			fail: true,
		},
		{
			s: `create table t (c1 int primary key, c2 bool,
constraint c1_primary unique (c2, c1))`,
			fail: true,
		},
		{
			s: "create table t (c1 int check(c1 > 1), check(c1 < c2), c2 int check(c2 > 2))",
			stmt: sql.CreateTable{
				Table:   types.TableName{Table: types.ID("t", false)},
				Columns: []types.Identifier{types.ID("c1", false), types.ID("c2", false)},
				ColumnTypes: []types.ColumnType{
					{Type: types.Int64Type, Size: 4},
					{Type: types.Int64Type, Size: 4},
				},
				ColumnDefaults: []sql.Expr{nil, nil},
				Constraints: []sql.Constraint{
					{
						Type:   sql.CheckConstraint,
						ColNum: 0,
						Check: &sql.BinaryExpr{
							Op:    sql.GreaterThanOp,
							Left:  sql.Ref{types.ID("c1", false)},
							Right: int64Literal(1),
						},
					},
					{
						Type:   sql.CheckConstraint,
						ColNum: -1,
						Check: &sql.BinaryExpr{
							Op:    sql.LessThanOp,
							Left:  sql.Ref{types.ID("c1", false)},
							Right: sql.Ref{types.ID("c2", false)},
						},
					},
					{
						Type:   sql.CheckConstraint,
						ColNum: 1,
						Check: &sql.BinaryExpr{
							Op:    sql.GreaterThanOp,
							Left:  sql.Ref{types.ID("c2", false)},
							Right: int64Literal(2),
						},
					},
				},
			},
		},
		{
			s: `create table t (c1 int constraint check_1 not null constraint check_2 default 1,
c2 int check(true))`,
			stmt: sql.CreateTable{
				Table:   types.TableName{Table: types.ID("t", false)},
				Columns: []types.Identifier{types.ID("c1", false), types.ID("c2", false)},
				ColumnTypes: []types.ColumnType{
					{Type: types.Int64Type, Size: 4, NotNull: true},
					{Type: types.Int64Type, Size: 4},
				},
				ColumnDefaults: []sql.Expr{int64Literal(1), nil},
				Constraints: []sql.Constraint{
					{
						Type:   sql.NotNullConstraint,
						Name:   types.ID("check_1", false),
						ColNum: 0,
					},
					{
						Type:   sql.DefaultConstraint,
						Name:   types.ID("check_2", false),
						ColNum: 0,
					},
					{
						Type:   sql.CheckConstraint,
						ColNum: 1,
						Check:  trueLiteral,
					},
				},
			},
		},
		{
			s: `create table t (c1 int references t2 on update cascade,
c2 int references t3 (p1) on update set default on delete set null)`,
			stmt: sql.CreateTable{
				Table:   types.TableName{Table: types.ID("t", false)},
				Columns: []types.Identifier{types.ID("c1", false), types.ID("c2", false)},
				ColumnTypes: []types.ColumnType{
					{Type: types.Int64Type, Size: 4},
					{Type: types.Int64Type, Size: 4},
				},
				ColumnDefaults: []sql.Expr{nil, nil},
				ForeignKeys: []*sql.ForeignKey{
					&sql.ForeignKey{
						FKCols:   []types.Identifier{types.ID("c1", false)},
						RefTable: types.TableName{Table: types.ID("t2", false)},
						OnUpdate: sql.Cascade,
					},
					&sql.ForeignKey{
						FKCols:   []types.Identifier{types.ID("c2", false)},
						RefTable: types.TableName{Table: types.ID("t3", false)},
						RefCols:  []types.Identifier{types.ID("p1", false)},
						OnDelete: sql.SetNull,
						OnUpdate: sql.SetDefault,
					},
				},
			},
		},
		{
			s: `create table t (c1 int, c2 int, c3 int, c4 int constraint foreign_1 not null,
foreign key (c1, c2) references t2 on delete cascade,
constraint fkey foreign key (c3, c4, c2) references t3 (p1, p2, p3) on update no action)`,
			stmt: sql.CreateTable{
				Table: types.TableName{Table: types.ID("t", false)},
				Columns: []types.Identifier{
					types.ID("c1", false),
					types.ID("c2", false),
					types.ID("c3", false),
					types.ID("c4", false),
				},
				ColumnTypes: []types.ColumnType{
					{Type: types.Int64Type, Size: 4},
					{Type: types.Int64Type, Size: 4},
					{Type: types.Int64Type, Size: 4},
					{Type: types.Int64Type, Size: 4, NotNull: true},
				},
				ColumnDefaults: []sql.Expr{nil, nil, nil, nil},
				Constraints: []sql.Constraint{
					{
						Type:   sql.NotNullConstraint,
						Name:   types.ID("foreign_1", false),
						ColNum: 3,
					},
				},
				ForeignKeys: []*sql.ForeignKey{
					&sql.ForeignKey{
						FKCols:   []types.Identifier{types.ID("c1", false), types.ID("c2", false)},
						RefTable: types.TableName{Table: types.ID("t2", false)},
						OnDelete: sql.Cascade,
					},
					&sql.ForeignKey{
						Name: types.ID("fkey", false),
						FKCols: []types.Identifier{
							types.ID("c3", false),
							types.ID("c4", false),
							types.ID("c2", false),
						},
						RefTable: types.TableName{Table: types.ID("t3", false)},
						RefCols: []types.Identifier{
							types.ID("p1", false),
							types.ID("p2", false),
							types.ID("p3", false),
						},
						OnUpdate: sql.NoAction,
					},
				},
			},
		},
		{
			s:    "create table t (c1 int, c2 int, c3 int, foreign key c1 references t2)",
			fail: true,
		},
		{
			s:    "create table t (c1 int, c2 int, c3 int, foreign key (c1,) references t2)",
			fail: true,
		},
		{
			s:    "create table t (c1 int, c2 int, c3 int, foreign key () references t2)",
			fail: true,
		},
		{
			s:    "create table t (c1 int, c2 int, c3 int, foreign key references t2)",
			fail: true,
		},
		{
			s: `create table t (c1 int, c2 int, c3 int,
foreign key (c1, c2) references t2 p1)`,
			fail: true,
		},
		{
			s: `create table t (c1 int, c2 int, c3 int,
foreign key (c1, c2) references t2 (p1,))`,
			fail: true,
		},
		{
			s: `create table t (c1 int, c2 int, c3 int,
foreign key (c1, c2) t2 (p1, p2))`,
			fail: true,
		},
		{
			s: `create table t (c1 int, c2 int, c3 int,
foreign (c1, c2) references t2 (p1, p2))`,
			fail: true,
		},
		{
			s:    "create table t (c1 int references t2 p1, c2 int, c3 int)",
			fail: true,
		},
		{
			s:    "create table t (c1 int references t2 (p1, p2), c2 int, c3 int)",
			fail: true,
		},
		{
			s: `create table t (c1 int, c2 int, c3 int,
foreign key (c1) references t2 delete restrict)`,
			fail: true,
		},
		{
			s: `create table t (c1 int, c2 int, c3 int,
foreign key (c1) references t2 on delete action)`,
			fail: true,
		},
		{
			s: `create table t (c1 int, c2 int, c3 int,
foreign key (c1) references t2 on delete restrict on delete no action)`,
			fail: true,
		},
		{
			s: `create table t (c1 int, c2 int, c3 int,
foreign key (c1) references t2 on delete restrict on update restrict on delete no action)`,
			fail: true,
		},
		{
			s: `create table t (c1 int, c2 int, c3 int,
foreign key (c1) references t2 on delete set on update cascade)`,
			fail: true,
		},
	}

	for i, c := range cases {
		p := NewParser(strings.NewReader(c.s), fmt.Sprintf("tests[%d]", i))
		cs, err := p.Parse()
		if c.fail {
			if err == nil {
				t.Errorf("Parse(%q) did not fail", c.s)
			}
		} else {
			if err != nil {
				t.Errorf("Parse(%q) failed with %s", c.s, err)
			} else if cs, ok := cs.(*sql.CreateTable); !ok ||
				!reflect.DeepEqual(&c.stmt, cs) {
				t.Errorf("Parse(%q) got %s want %s", c.s, cs.String(), c.stmt.String())
			}
		}
	}
}

func TestCreateIndex(t *testing.T) {
	cases := []struct {
		s    string
		stmt sql.CreateIndex
		fail bool
	}{
		{s: "create index unique idx on tbl (c1)", fail: true},
		{s: "create index idx tbl (c1)", fail: true},
		{s: "create index tbl (c1)", fail: true},
		{s: "create index idx on tbl using (c1 DESC, c2)", fail: true},
		{s: "create index idx on tbl using tree (c1 DESC, c2)", fail: true},
		{
			s: "create index idx on tbl (c1 DESC, c2)",
			stmt: sql.CreateIndex{
				Table: types.TableName{Table: types.ID("tbl", false)},
				Index: types.ID("idx", false),
				Key: sql.IndexKey{
					Columns: []types.Identifier{types.ID("c1", false), types.ID("c2", false)},
					Reverse: []bool{true, false},
				},
			},
		},
		{
			s: "create unique index if not exists idx on tbl using btree (c1)",
			stmt: sql.CreateIndex{
				Table: types.TableName{Table: types.ID("tbl", false)},
				Index: types.ID("idx", false),
				Key: sql.IndexKey{
					Unique:  true,
					Columns: []types.Identifier{types.ID("c1", false)},
					Reverse: []bool{false},
				},
				IfNotExists: true,
			},
		},
	}

	for i, c := range cases {
		p := NewParser(strings.NewReader(c.s), fmt.Sprintf("tests[%d]", i))
		cs, err := p.Parse()
		if c.fail {
			if err == nil {
				t.Errorf("Parse(%q) did not fail", c.s)
			}
		} else {
			if err != nil {
				t.Errorf("Parse(%q) failed with %s", c.s, err)
			} else if cs, ok := cs.(*sql.CreateIndex); !ok ||
				!reflect.DeepEqual(&c.stmt, cs) {
				t.Errorf("Parse(%q) got %s want %s", c.s, cs.String(), c.stmt.String())
			}
		}
	}
}

func TestInsertValues(t *testing.T) {
	cases := []struct {
		s    string
		stmt sql.InsertValues
		fail bool
	}{
		{s: "insert into t", fail: true},
		{s: "insert t values (1)", fail: true},
		{s: "insert into t (1)", fail: true},
		{s: "insert into t values (1", fail: true},
		{s: "insert into t values 1)", fail: true},
		{s: "insert into t values (1, )", fail: true},
		{s: "insert into t values (1, 2),", fail: true},
		{s: "insert into t values (1, 2) (3)", fail: true},
		{s: "insert into t () values (1, 2)", fail: true},
		{s: "insert into t (a values (1, 2)", fail: true},
		{s: "insert into t (a, ) values (1, 2)", fail: true},
		{s: "insert into t (a, a) values (1, 2)", fail: true},
		{s: "insert into t (a, b, a) values (1, 2)", fail: true},
		{
			s: "insert into t values (1, 'abc', true)",
			stmt: sql.InsertValues{
				Table: types.TableName{Table: types.ID("t", false)},
				Rows: [][]sql.Expr{
					{int64Literal(1), stringLiteral("abc"), trueLiteral},
				},
			},
		},
		{
			s: "insert into t values (1, 'abc', true), (2, 'def', false)",
			stmt: sql.InsertValues{
				Table: types.TableName{Table: types.ID("t", false)},
				Rows: [][]sql.Expr{
					{int64Literal(1), stringLiteral("abc"), trueLiteral},
					{int64Literal(2), stringLiteral("def"), falseLiteral},
				},
			},
		},
		{
			s: "insert into t values (NULL, 'abc', NULL)",
			stmt: sql.InsertValues{
				Table: types.TableName{Table: types.ID("t", false)},
				Rows: [][]sql.Expr{
					{nilLiteral, stringLiteral("abc"), nilLiteral},
				},
			},
		},
	}

	for i, c := range cases {
		p := NewParser(strings.NewReader(c.s), fmt.Sprintf("tests[%d]", i))
		is, err := p.Parse()
		if c.fail {
			if err == nil {
				t.Errorf("Parse(%q) did not fail", c.s)
			}
		} else {
			if err != nil {
				t.Errorf("Parse(%q) failed with %s", c.s, err)
			} else if is, ok := is.(*sql.InsertValues); !ok ||
				!reflect.DeepEqual(&c.stmt, is) {
				t.Errorf("Parse(%q) got %s want %s", c.s, is.String(), c.stmt.String())
			}
		}
	}
}

func TestParseExpr(t *testing.T) {
	cases := []struct {
		s    string
		expr string
	}{
		{"1 * 2 - 3 = 4", "(((1 * 2) - 3) == 4)"},
		{"1 / 2 * 3 - 5", "(((1 / 2) * 3) - 5)"},
		{"1 - 2 * (3 + 4) + 5", "((1 - (2 * (3 + 4))) + 5)"},
		{"1 - (2 + 3) * 4 + 5", "((1 - ((2 + 3) * 4)) + 5)"},
		{"1 + 2 = 3 * 4 - 5 * 6", "((1 + 2) == ((3 * 4) - (5 * 6)))"},
		{"NOT 12 AND 1 OR 3", "(((NOT 12) AND 1) OR 3)"},
		{"- 1 * 2 + 3", "(((- 1) * 2) + 3)"},
		{"- 1 * 2", "((- 1) * 2)"},
		{"12 % 34 + 56", "((12 % 34) + 56)"},
		{"12 == 34 OR 56 != 78", "((12 == 34) OR (56 != 78))"},
		{"12 + 34 << 56 + 78", "((12 + 34) << (56 + 78))"},
		{"abc", "abc"},
		{"abc.def", "abc.def"},
		{"abc. def . ghi .jkl", "abc.def.ghi.jkl"},
		{"abc(1 + 2)", "abc((1 + 2))"},
		{"abc()", "abc()"},
		{"abc(1 + 2, def() * 3)", "abc((1 + 2), (def() * 3))"},
		{"c1 * 10 = c2 AND c2 * 10 = c3", "(((c1 * 10) == c2) AND ((c2 * 10) == c3))"},
		{"1 + 2 - 3", "((1 + 2) - 3)"},
		{"12 / 4 * 3", "((12 / 4) * 3)"},
		{"1 - 2 + 3", "((1 - 2) + 3)"},
		{"12 * 4 / 3", "((12 * 4) / 3)"},
		{"- 2 * 3", "((- 2) * 3)"},
		{"- (2 * 3)", "(- (2 * 3))"},
		{"1 + 2 * 3", "(1 + (2 * 3))"},
		{"1 * 2 + 3 / - 4", "((1 * 2) + (3 / (- 4)))"},
		{"count(*)", "count_all()"},
		{"count(123)", "count(123)"},
		{"count(1,23,456)", "count(1, 23, 456)"},
		{"x AND y AND z", "((x AND y) AND z)"},
		{"x * y / z", "((x * y) / z)"},
		{"123 + (select * from t)", "(123 + (SELECT * FROM t))"},
		{"(values (1)) + (show schema)", "((VALUES (1)) + (SHOW SCHEMA))"},
		{"exists(show schema) and c1", "(EXISTS(SHOW SCHEMA) AND c1)"},
		{"c1 in (select * from tbl1)", "c1 == ANY(SELECT * FROM tbl1)"},
		{"(c1 + c2) not in (values (1), (2), (3))", "(c1 + c2) != ALL(VALUES (1), (2), (3))"},
		{"c1 > some(select * from t1)", "c1 > ANY(SELECT * FROM t1)"},
		{"c1 <= all(select c1 from t1)", "c1 <= ALL(SELECT c1 FROM t1)"},
	}

	for i, c := range cases {
		p := NewParser(strings.NewReader(c.s), fmt.Sprintf("cases[%d]", i))
		e, err := p.ParseExpr()
		if err != nil {
			t.Errorf("ParseExpr(%q) failed with %s", c.s, err)
		} else if c.expr != e.String() {
			t.Errorf("ParseExpr(%q) got %s want %s", c.s, e, c.expr)
		}
	}

	fails := []string{
		"1 *",
		"(1 * 2",
		"(*)",
		"abc.123",
		"((1 + 2) * 3",
		"abc(123,",
		"abc(*)",
		"exists()",
		"exists(1 + 2)",
		"exists(select * show schema)",
		"c1 in (1, 2, 3)",
		"c1 in (select * from tbl1, select * from tbl2)",
		"c1 not in (1, 2, 3)",
		"(c1 not (1, 2, 3))",
		"(c1 all = (select * from t1))",
		"(c1 + any(select c2 from t1)",
	}

	for i, f := range fails {
		p := NewParser(strings.NewReader(f), fmt.Sprintf("fails[%d]", i))
		e, err := p.ParseExpr()
		if err == nil {
			t.Errorf("ParseExpr(%q) did not fail, got %s", f, e)
		}
	}
}

func TestSelect(t *testing.T) {
	cases := []struct {
		s    string
		stmt sql.Select
		fail bool
	}{
		{s: "select", fail: true},
		{s: "select *, * from t", fail: true},
		{s: "select c, * from t", fail: true},
		{s: "select c, from t", fail: true},
		{s: "select t.c, c, * from t", fail: true},
		{
			s:    "select *",
			stmt: sql.Select{},
		},
		{
			s: "select * from t",
			stmt: sql.Select{
				From: &sql.FromTableAlias{TableName: types.TableName{Table: types.ID("t", false)}},
			},
		},
		{
			s: "select * from t where x > 1",
			stmt: sql.Select{
				From: &sql.FromTableAlias{TableName: types.TableName{Table: types.ID("t", false)}},
				Where: &sql.BinaryExpr{Op: sql.GreaterThanOp, Left: sql.Ref{types.ID("x", false)},
					Right: int64Literal(1)},
			},
		},
		{
			s: "select * from t@i",
			stmt: sql.Select{
				From: &sql.FromIndexAlias{
					TableName: types.TableName{Table: types.ID("t", false)},
					Index:     types.ID("i", false),
				},
			},
		},
		{
			s: "select * from t@i where x > 1",
			stmt: sql.Select{
				From: &sql.FromIndexAlias{
					TableName: types.TableName{Table: types.ID("t", false)},
					Index:     types.ID("i", false),
				},
				Where: &sql.BinaryExpr{Op: sql.GreaterThanOp, Left: sql.Ref{types.ID("x", false)},
					Right: int64Literal(1)},
			},
		},
		{
			s: "select * from t where x = (show schema)",
			stmt: sql.Select{
				From: &sql.FromTableAlias{TableName: types.TableName{Table: types.ID("t", false)}},
				Where: &sql.BinaryExpr{
					Op:   sql.EqualOp,
					Left: sql.Ref{types.ID("x", false)},
					Right: &sql.Subquery{
						Op: sql.Scalar,
						Stmt: &sql.Show{
							Variable: types.SCHEMA,
						},
					},
				},
			},
		},
		{
			s: "select * from (table t) as t",
			stmt: sql.Select{
				From: sql.FromStmt{
					Stmt: &sql.Select{
						From: &sql.FromTableAlias{
							TableName: types.TableName{Table: types.ID("t", false)},
						},
					},
					Alias: types.ID("t", false),
				},
			},
		},
		{
			s: "select c from t",
			stmt: sql.Select{
				From: &sql.FromTableAlias{TableName: types.TableName{Table: types.ID("t", false)}},
				Results: []sql.SelectResult{
					sql.ExprResult{Expr: sql.Ref{types.ID("c", false)}},
				},
			},
		},
		{
			s: "select c1, c2, t.c3 from t",
			stmt: sql.Select{
				From: &sql.FromTableAlias{TableName: types.TableName{Table: types.ID("t", false)}},
				Results: []sql.SelectResult{
					sql.ExprResult{Expr: sql.Ref{types.ID("c1", false)}},
					sql.ExprResult{Expr: sql.Ref{types.ID("c2", false)}},
					sql.ExprResult{Expr: sql.Ref{types.ID("t", false), types.ID("c3", false)}},
				},
			},
		},
		{
			s: "select t.*, c1, c2 from t",
			stmt: sql.Select{
				From: &sql.FromTableAlias{TableName: types.TableName{Table: types.ID("t", false)}},
				Results: []sql.SelectResult{
					sql.TableResult{Table: types.ID("t", false)},
					sql.ExprResult{Expr: sql.Ref{types.ID("c1", false)}},
					sql.ExprResult{Expr: sql.Ref{types.ID("c2", false)}},
				},
			},
		},
		{
			s: "select c1, t.*, c2 from t",
			stmt: sql.Select{
				From: &sql.FromTableAlias{TableName: types.TableName{Table: types.ID("t", false)}},
				Results: []sql.SelectResult{
					sql.ExprResult{Expr: sql.Ref{types.ID("c1", false)}},
					sql.TableResult{Table: types.ID("t", false)},
					sql.ExprResult{Expr: sql.Ref{types.ID("c2", false)}},
				},
			},
		},
		{
			s: "select c1, c2, t.* from t",
			stmt: sql.Select{
				From: &sql.FromTableAlias{TableName: types.TableName{Table: types.ID("t", false)}},
				Results: []sql.SelectResult{
					sql.ExprResult{Expr: sql.Ref{types.ID("c1", false)}},
					sql.ExprResult{Expr: sql.Ref{types.ID("c2", false)}},
					sql.TableResult{Table: types.ID("t", false)},
				},
			},
		},
		{
			s: "select t2.c1 as a1, c2 as a2 from t",
			stmt: sql.Select{
				From: &sql.FromTableAlias{TableName: types.TableName{Table: types.ID("t", false)}},
				Results: []sql.SelectResult{
					sql.ExprResult{
						Expr:  sql.Ref{types.ID("t2", false), types.ID("c1", false)},
						Alias: types.ID("a1", false),
					},
					sql.ExprResult{
						Expr:  sql.Ref{types.ID("c2", false)},
						Alias: types.ID("a2", false),
					},
				},
			},
		},
		{
			s: "select t2.c1 a1, c2 a2 from t",
			stmt: sql.Select{
				From: &sql.FromTableAlias{TableName: types.TableName{Table: types.ID("t", false)}},
				Results: []sql.SelectResult{
					sql.ExprResult{
						Expr:  sql.Ref{types.ID("t2", false), types.ID("c1", false)},
						Alias: types.ID("a1", false),
					},
					sql.ExprResult{
						Expr:  sql.Ref{types.ID("c2", false)},
						Alias: types.ID("a2", false),
					},
				},
			},
		},
		{
			s: "select c1 + c2 as a from t",
			stmt: sql.Select{
				From: &sql.FromTableAlias{TableName: types.TableName{Table: types.ID("t", false)}},
				Results: []sql.SelectResult{
					sql.ExprResult{
						Expr: &sql.BinaryExpr{Op: sql.AddOp,
							Left:  sql.Ref{types.ID("c1", false)},
							Right: sql.Ref{types.ID("c2", false)},
						},
						Alias: types.ID("a", false),
					},
				},
			},
		},
		{
			s: "select t1.c1, t2.c2 from t1, t2",
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
		},
		{
			s: "select * from t1, t2, t3",
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
		},
		{
			s: "select * from t1 join t2 using (c1), t3",
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
		},
		{
			s: "select * from (t1, t2) right join t3 using (c1)",
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
		},
		{
			s: "select * from t1 inner join t2 on c1 > 5",
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
		},
		{
			s: "select * from t1 inner join t2 using (c1, c2, c3)",
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
		},
		{s: "select * from t1, t2 full outer join t3", fail: true},
		{s: "select * from t1 inner join t2", fail: true},
		{s: "select * from t1 inner join t2", fail: true},
		{s: "select * from t1 inner join t2", fail: true},
		{s: "select * from t1 inner join t2 on c1 > 5 using (c1, c2)", fail: true},
		{s: "select * from t1 cross join t2 on c1 > 5", fail: true},
		{s: "select * from t1 cross join t2 using (c1, c2)", fail: true},
		{s: "select * from t1 inner join t2 using ()", fail: true},
		{s: "select * from t1 inner join t2 using (c1, c1)", fail: true},
		{
			s: "select * from (select * from t1) as s1 join t2 using (c1)",
			stmt: sql.Select{
				From: sql.FromJoin{
					Left: sql.FromStmt{
						Stmt: &sql.Select{
							From: &sql.FromTableAlias{
								TableName: types.TableName{Table: types.ID("t1", false)},
							},
						},
						Alias: types.ID("s1", false),
					},
					Right: &sql.FromTableAlias{
						TableName: types.TableName{Table: types.ID("t2", false)},
					},
					Type:  sql.Join,
					Using: []types.Identifier{types.ID("c1", false)},
				},
			},
		},
		{
			s: "select * from t2 join (values (1, 'abc', true)) as v1 using (c1, c2)",
			stmt: sql.Select{
				From: sql.FromJoin{
					Left: &sql.FromTableAlias{
						TableName: types.TableName{Table: types.ID("t2", false)},
					},
					Right: sql.FromStmt{
						Stmt: &sql.Values{
							Expressions: [][]sql.Expr{
								{int64Literal(1), stringLiteral("abc"), trueLiteral},
							},
						},
						Alias: types.ID("v1", false),
					},
					Type:  sql.Join,
					Using: []types.Identifier{types.ID("c1", false), types.ID("c2", false)},
				},
			},
		},
		{
			s: "select * from (select * from t1) s1 join (values (1, 'abc', true)) as v1 " +
				"using (c1, c2)",
			stmt: sql.Select{
				From: sql.FromJoin{
					Left: sql.FromStmt{
						Stmt: &sql.Select{
							From: &sql.FromTableAlias{
								TableName: types.TableName{Table: types.ID("t1", false)},
							},
						},
						Alias: types.ID("s1", false),
					},
					Right: sql.FromStmt{
						Stmt: &sql.Values{
							Expressions: [][]sql.Expr{
								{int64Literal(1), stringLiteral("abc"), trueLiteral},
							},
						},
						Alias: types.ID("v1", false),
					},
					Type:  sql.Join,
					Using: []types.Identifier{types.ID("c1", false), types.ID("c2", false)},
				},
			},
		},
		{s: "select * from (values (1, 'abc', true)) as v1 (", fail: true},
		{s: "select * from (values (1, 'abc', true)) as v1 )", fail: true},
		{s: "select * from (values (1, 'abc', true)) as v1 (,", fail: true},
		{s: "select * from (values (1, 'abc', true)) as v1 (a,)", fail: true},
		{s: "select * from (values (1, 'abc', true)) as v1 (a b)", fail: true},
		{
			s: "select * from (values (1, 'abc', true)) as v1",
			stmt: sql.Select{
				From: sql.FromStmt{
					Stmt: &sql.Values{
						Expressions: [][]sql.Expr{
							{int64Literal(1), stringLiteral("abc"), trueLiteral},
						},
					},
					Alias: types.ID("v1", false),
				},
			},
		},
		{
			s: "select * from (values (1, 'abc', true)) as v1 (c1, c2, c3)",
			stmt: sql.Select{
				From: sql.FromStmt{
					Stmt: &sql.Values{
						Expressions: [][]sql.Expr{
							{int64Literal(1), stringLiteral("abc"), trueLiteral},
						},
					},
					Alias: types.ID("v1", false),
					ColumnAliases: []types.Identifier{
						types.ID("c1", false),
						types.ID("c2", false),
						types.ID("c3", false),
					},
				},
			},
		},
		{s: "select * from (select * from t1) as s1 (", fail: true},
		{s: "select * from (select * from t1) as s1 )", fail: true},
		{s: "select * from (select * from t1) as s1 (,", fail: true},
		{s: "select * from (select * from t1) as s1 (a,)", fail: true},
		{s: "select * from (select * from t1) as s1 (a b)", fail: true},
		{
			s: "select * from (select * from t1) as s1",
			stmt: sql.Select{
				From: sql.FromStmt{
					Stmt: &sql.Select{
						From: &sql.FromTableAlias{
							TableName: types.TableName{Table: types.ID("t1", false)},
						},
					},
					Alias: types.ID("s1", false),
				},
			},
		},
		{
			s: "select * from (select * from t1) as s1 (c1)",
			stmt: sql.Select{
				From: sql.FromStmt{
					Stmt: &sql.Select{
						From: &sql.FromTableAlias{
							TableName: types.TableName{Table: types.ID("t1", false)},
						},
					},
					Alias:         types.ID("s1", false),
					ColumnAliases: []types.Identifier{types.ID("c1", false)},
				},
			},
		},
		{
			s: "select * from (select * from t1) as s1 (c1, c2)",
			stmt: sql.Select{
				From: sql.FromStmt{
					Stmt: &sql.Select{
						From: &sql.FromTableAlias{
							TableName: types.TableName{Table: types.ID("t1", false)},
						},
					},
					Alias: types.ID("s1", false),
					ColumnAliases: []types.Identifier{
						types.ID("c1", false),
						types.ID("c2", false),
					},
				},
			},
		},
		{
			s: "select * from (select * from t1) as s1 (c1, c2, c3)",
			stmt: sql.Select{
				From: sql.FromStmt{
					Stmt: &sql.Select{
						From: &sql.FromTableAlias{
							TableName: types.TableName{Table: types.ID("t1", false)},
						},
					},
					Alias: types.ID("s1", false),
					ColumnAliases: []types.Identifier{
						types.ID("c1", false),
						types.ID("c2", false),
						types.ID("c3", false),
					},
				},
			},
		},
		{s: "select c where c > 5 from t", fail: true},
		{s: "select c from t group", fail: true},
		{s: "select c from t group by", fail: true},
		{s: "select c from t group by c where c > 5", fail: true},
		{s: "select c from t group by c having", fail: true},
		{s: "select c from t group by c, d, having c > 5", fail: true},
		{
			s: "select c from t group by c",
			stmt: sql.Select{
				From: &sql.FromTableAlias{TableName: types.TableName{Table: types.ID("t", false)}},
				Results: []sql.SelectResult{
					sql.ExprResult{Expr: sql.Ref{types.ID("c", false)}},
				},
				GroupBy: []sql.Expr{sql.Ref{types.ID("c", false)}},
			},
		},
		{
			s: "select c from t group by c, d, e + f",
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
		},
		{
			s: "select c from t group by c having c > 1",
			stmt: sql.Select{
				From: &sql.FromTableAlias{TableName: types.TableName{Table: types.ID("t", false)}},
				Results: []sql.SelectResult{
					sql.ExprResult{Expr: sql.Ref{types.ID("c", false)}},
				},
				GroupBy: []sql.Expr{sql.Ref{types.ID("c", false)}},
				Having: &sql.BinaryExpr{Op: sql.GreaterThanOp, Left: sql.Ref{types.ID("c", false)},
					Right: int64Literal(1)},
			},
		},
	}

	for i, c := range cases {
		p := NewParser(strings.NewReader(c.s), fmt.Sprintf("tests[%d]", i))
		ss, err := p.Parse()
		if c.fail {
			if err == nil {
				t.Errorf("Parse(%q) did not fail", c.s)
			}
		} else {
			if err != nil {
				t.Errorf("Parse(%q) failed with %s", c.s, err)
			} else if ss, ok := ss.(*sql.Select); !ok || !reflect.DeepEqual(&c.stmt, ss) {
				t.Errorf("Parse(%q) got %s want %s", c.s, ss.String(), c.stmt.String())
			}
		}
	}
}

func TestValues(t *testing.T) {
	cases := []struct {
		s    string
		stmt sql.Values
		fail bool
	}{
		{s: "values", fail: true},
		{s: "values (", fail: true},
		{s: "values ()", fail: true},
		{s: "values (1", fail: true},
		{s: "values (1, 2", fail: true},
		{s: "values (1 2)", fail: true},
		{s: "values (1, 2), (3)", fail: true},
		{s: "values (1, 2, 3), (4, 5), (6, 7, 8)", fail: true},
		{
			s: "values (1, 'abc', true)",
			stmt: sql.Values{
				Expressions: [][]sql.Expr{
					{int64Literal(1), stringLiteral("abc"), trueLiteral},
				},
			},
		},
		{
			s: "values (1, 'abc', true), (2, 'def', false)",
			stmt: sql.Values{
				Expressions: [][]sql.Expr{
					{int64Literal(1), stringLiteral("abc"), trueLiteral},
					{int64Literal(2), stringLiteral("def"), falseLiteral},
				},
			},
		},
	}

	for i, c := range cases {
		p := NewParser(strings.NewReader(c.s), fmt.Sprintf("tests[%d]", i))
		vs, err := p.Parse()
		if c.fail {
			if err == nil {
				t.Errorf("Parse(%q) did not fail", c.s)
			}
		} else {
			if err != nil {
				t.Errorf("Parse(%q) failed with %s", c.s, err)
			} else if vs, ok := vs.(*sql.Values); !ok || !reflect.DeepEqual(&c.stmt, vs) {
				t.Errorf("Parse(%q) got %s want %s", c.s, vs.String(), c.stmt.String())
			}
		}
	}
}

func TestDelete(t *testing.T) {
	cases := []struct {
		s    string
		stmt sql.Delete
		fail bool
	}{
		{s: "delete", fail: true},
		{s: "delete t", fail: true},
		{s: "delete from", fail: true},
		{s: "delete from t1, t2", fail: true},
		{s: "delete from t where", fail: true},
		{
			s: "delete from t",
			stmt: sql.Delete{
				Table: types.TableName{Table: types.ID("t", false)},
			},
		},
		{
			s: "delete from t where c > 1",
			stmt: sql.Delete{
				Table: types.TableName{Table: types.ID("t", false)},
				Where: &sql.BinaryExpr{Op: sql.GreaterThanOp, Left: sql.Ref{types.ID("c", false)},
					Right: int64Literal(1)},
			},
		},
	}

	for i, c := range cases {
		p := NewParser(strings.NewReader(c.s), fmt.Sprintf("tests[%d]", i))
		ds, err := p.Parse()
		if c.fail {
			if err == nil {
				t.Errorf("Parse(%q) did not fail", c.s)
			}
		} else {
			if err != nil {
				t.Errorf("Parse(%q) failed with %s", c.s, err)
			} else if ds, ok := ds.(*sql.Delete); !ok || !reflect.DeepEqual(&c.stmt, ds) {
				t.Errorf("Parse(%q) got %s want %s", c.s, ds.String(), c.stmt.String())
			}
		}
	}
}

func TestUpdate(t *testing.T) {
	cases := []struct {
		s    string
		stmt sql.Update
		fail bool
	}{
		{s: "update", fail: true},
		{s: "update t", fail: true},
		{s: "update t set", fail: true},
		{s: "update set t c = 5", fail: true},
		{s: "update t c = 5", fail: true},
		{s: "update t set c = 5,", fail: true},
		{s: "update t set c = 5, where", fail: true},
		{s: "update t set c = 5 where", fail: true},
		{s: "update t set where c = 6", fail: true},
		{
			s: "update t set c = 5",
			stmt: sql.Update{
				Table: types.TableName{Table: types.ID("t", false)},
				ColumnUpdates: []sql.ColumnUpdate{
					{Column: types.ID("c", false), Expr: int64Literal(5)},
				},
			},
		},
		{
			s: "update t set c = 0 where c > 1",
			stmt: sql.Update{
				Table: types.TableName{Table: types.ID("t", false)},
				ColumnUpdates: []sql.ColumnUpdate{
					{Column: types.ID("c", false), Expr: int64Literal(0)},
				},
				Where: &sql.BinaryExpr{Op: sql.GreaterThanOp, Left: sql.Ref{types.ID("c", false)},
					Right: int64Literal(1)},
			},
		},
		{
			s: "update t set c = default where c > 1",
			stmt: sql.Update{
				Table: types.TableName{Table: types.ID("t", false)},
				ColumnUpdates: []sql.ColumnUpdate{
					{Column: types.ID("c", false), Expr: nil},
				},
				Where: &sql.BinaryExpr{Op: sql.GreaterThanOp, Left: sql.Ref{types.ID("c", false)},
					Right: int64Literal(1)},
			},
		},
		{
			s: "update t set c1 = 1, c2 = 2, c3 = 3",
			stmt: sql.Update{
				Table: types.TableName{Table: types.ID("t", false)},
				ColumnUpdates: []sql.ColumnUpdate{
					{Column: types.ID("c1", false), Expr: int64Literal(1)},
					{Column: types.ID("c2", false), Expr: int64Literal(2)},
					{Column: types.ID("c3", false), Expr: int64Literal(3)},
				},
			},
		},
	}

	for i, c := range cases {
		p := NewParser(strings.NewReader(c.s), fmt.Sprintf("tests[%d]", i))
		us, err := p.Parse()
		if c.fail {
			if err == nil {
				t.Errorf("Parse(%q) did not fail", c.s)
			}
		} else {
			if err != nil {
				t.Errorf("Parse(%q) failed with %s", c.s, err)
			} else if us, ok := us.(*sql.Update); !ok || !reflect.DeepEqual(&c.stmt, us) {
				t.Errorf("Parse(%q) got %s want %s", c.s, us.String(), c.stmt.String())
			}
		}
	}
}

func TestCreateDatabase(t *testing.T) {
	cases := []struct {
		s    string
		stmt sql.Stmt
		fail bool
	}{
		{s: "create database", fail: true},
		{
			s: "create database test",
			stmt: &sql.CreateDatabase{
				Database: types.ID("test", false),
			},
		},
		{s: "create database test with", fail: true},
		{s: "create database test with path", fail: true},
		{s: "create database test with path = ", fail: true},
		{s: "create database test with 'path' = value", fail: true},
		{s: "create database test with create = value", fail: true},
		{s: "create database test with path = 'string' engine", fail: true},
		{
			s: "create database test with path = 'string' engine 'fast'",
			stmt: &sql.CreateDatabase{
				Database: types.ID("test", false),
				Options: map[types.Identifier]string{
					types.ID("path", false):   "string",
					types.ID("engine", false): "fast",
				},
			},
		},
	}

	for i, c := range cases {
		p := NewParser(strings.NewReader(c.s), fmt.Sprintf("tests[%d]", i))
		cd, err := p.Parse()
		if c.fail {
			if err == nil {
				t.Errorf("Parse(%q) did not fail", c.s)
			}
		} else {
			if err != nil {
				t.Errorf("Parse(%q) failed with %s", c.s, err)
			} else {
				if !reflect.DeepEqual(c.stmt, cd) {
					t.Errorf("Parse(%q) got %s want %s", c.s, cd.String(), c.stmt.String())
				}
			}
		}
	}
}

func TestAlterTable(t *testing.T) {
	cases := []struct {
		s    string
		stmt sql.Stmt
		fail bool
	}{
		{s: "alter table tbl", fail: true},
		{s: "alter table exists tbl", fail: true},
		{
			s: "alter table tbl add foreign key (c1, c2) references rtbl",
			stmt: &sql.AlterTable{
				Table: types.TableName{Table: types.ID("tbl", false)},
				Actions: []sql.AlterAction{
					&sql.AddForeignKey{
						sql.ForeignKey{
							FKCols: []types.Identifier{
								types.ID("c1", false),
								types.ID("c2", false),
							},
							RefTable: types.TableName{Table: types.ID("rtbl", false)},
						},
					},
				},
			},
		},
		{
			s: "alter table tbl add constraint con foreign key (c1) references rtbl",
			stmt: &sql.AlterTable{
				Table: types.TableName{Table: types.ID("tbl", false)},
				Actions: []sql.AlterAction{
					&sql.AddForeignKey{
						sql.ForeignKey{
							Name:     types.ID("con", false),
							FKCols:   []types.Identifier{types.ID("c1", false)},
							RefTable: types.TableName{Table: types.ID("rtbl", false)},
						},
					},
				},
			},
		},
		{
			s: `alter table tbl add constraint con1 foreign key (c1) references rtbl,
add constraint con2 foreign key (c2) references tbl2`,
			stmt: &sql.AlterTable{
				Table: types.TableName{Table: types.ID("tbl", false)},
				Actions: []sql.AlterAction{
					&sql.AddForeignKey{
						sql.ForeignKey{
							Name:     types.ID("con1", false),
							FKCols:   []types.Identifier{types.ID("c1", false)},
							RefTable: types.TableName{Table: types.ID("rtbl", false)},
						},
					},
					&sql.AddForeignKey{
						sql.ForeignKey{
							Name:     types.ID("con2", false),
							FKCols:   []types.Identifier{types.ID("c2", false)},
							RefTable: types.TableName{Table: types.ID("tbl2", false)},
						},
					},
				},
			},
		},
		{
			s: `alter table tbl add constraint con1 foreign key (c1) references rtbl,
add constraint con2 foreign key (c2) references tbl2, fail`,
			fail: true,
		},
		{
			s: "alter table tbl drop constraint if exists con",
			stmt: &sql.AlterTable{
				Table: types.TableName{Table: types.ID("tbl", false)},
				Actions: []sql.AlterAction{
					&sql.DropConstraint{
						Name:     types.ID("con", false),
						IfExists: true,
					},
				},
			},
		},
		{
			s: `alter table tbl add constraint con foreign key (c1) references rtbl,
alter column c1 drop default, alter c2 drop not null, drop constraint con`,
			stmt: &sql.AlterTable{
				Table: types.TableName{Table: types.ID("tbl", false)},
				Actions: []sql.AlterAction{
					&sql.AddForeignKey{
						sql.ForeignKey{
							Name:     types.ID("con", false),
							FKCols:   []types.Identifier{types.ID("c1", false)},
							RefTable: types.TableName{Table: types.ID("rtbl", false)},
						},
					},
					&sql.DropConstraint{
						Column: types.ID("c1", false),
						Type:   sql.DefaultConstraint,
					},
					&sql.DropConstraint{
						Column: types.ID("c2", false),
						Type:   sql.NotNullConstraint,
					},
					&sql.DropConstraint{
						Name: types.ID("con", false),
					},
				},
			},
		},
		{
			s:    "alter table tbl drop con",
			fail: true,
		},
		{
			s:    "alter table tbl constraint if exists con",
			fail: true,
		},
		{
			s:    "alter table tbl drop constraint if con",
			fail: true,
		},
		{
			s:    "alter table tbl alter column drop default",
			fail: true,
		},
		{
			s:    "alter table tbl alter c1 default",
			fail: true,
		},
		{
			s:    "alter table tbl alter c1 drop null",
			fail: true,
		},
	}

	for i, c := range cases {
		p := NewParser(strings.NewReader(c.s), fmt.Sprintf("tests[%d]", i))
		cd, err := p.Parse()
		if c.fail {
			if err == nil {
				t.Errorf("Parse(%q) did not fail", c.s)
			}
		} else {
			if err != nil {
				t.Errorf("Parse(%q) failed with %s", c.s, err)
			} else {
				if !reflect.DeepEqual(c.stmt, cd) {
					t.Errorf("Parse(%q) got %s want %s", c.s, cd.String(), c.stmt.String())
				}
			}
		}
	}
}
