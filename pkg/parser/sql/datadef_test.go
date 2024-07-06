package sql_test

import (
	"testing"

	"github.com/leftmike/maho/pkg/parser/sql"
	"github.com/leftmike/maho/pkg/types"
)

func TestCreateTable(t *testing.T) {
	cases := []struct {
		stmt sql.CreateTable
		s    string
	}{
		{
			stmt: sql.CreateTable{
				Table: types.TableName{
					Database: types.ID("xyz", false),
					Schema:   types.ID("mno", false),
					Table:    types.ID("abc", false),
				},
			},
			s: "CREATE TABLE xyz.mno.abc ()",
		},
		{
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
				ColumnDefaults: make([]sql.Expr, 4),
				Constraints: []sql.Constraint{
					{
						Type:   sql.NotNullConstraint,
						Name:   types.ID("foreign_1", false),
						ColNum: 3,
					},
				},
				ForeignKeys: []*sql.ForeignKey{
					&sql.ForeignKey{
						Name:     types.ID("foreign_2", false),
						FKCols:   []types.Identifier{types.ID("c1", false), types.ID("c2", false)},
						RefTable: types.TableName{Table: types.ID("t2", false)},
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
					},
				},
			},
			s: "CREATE TABLE t (c1 INT, c2 INT, c3 INT, c4 INT NOT NULL, CONSTRAINT foreign_2 FOREIGN KEY (c1, c2) REFERENCES t2, CONSTRAINT fkey FOREIGN KEY (c3, c4, c2) REFERENCES t3 (p1, p2, p3))",
		},
	}

	for _, c := range cases {
		s := c.stmt.String()
		if s != c.s {
			t.Errorf("%#v.String() got %s want %s", c.stmt, s, c.s)
		}
	}
}

func TestDropTable(t *testing.T) {
	cases := []struct {
		stmt sql.DropTable
		s    string
	}{
		{
			stmt: sql.DropTable{
				IfExists: false,
				Tables: []types.TableName{
					{
						Database: types.ID("abc", false),
						Schema:   types.ID("def", false),
						Table:    types.ID("ghijk", false),
					},
				},
			},
			s: "DROP TABLE abc.def.ghijk",
		},
		{
			stmt: sql.DropTable{
				IfExists: true,
				Tables: []types.TableName{
					{
						Database: types.ID("abc", false),
						Schema:   types.ID("def", false),
						Table:    types.ID("ghijk", false),
					},
					{
						Table: types.ID("jkl", false),
					},
				},
			},
			s: "DROP TABLE IF EXISTS abc.def.ghijk, jkl",
		},
	}

	for _, c := range cases {
		s := c.stmt.String()
		if s != c.s {
			t.Errorf("%#v.String() got %s want %s", c.stmt, s, c.s)
		}
	}
}

func TestForeignKey(t *testing.T) {
	tn1 := types.TableName{
		Database: types.ID("db", false),
		Schema:   types.ID("sc", false),
		Table:    types.ID("tbl1", false),
	}
	tn2 := types.TableName{
		Database: types.ID("db", false),
		Schema:   types.ID("sc", false),
		Table:    types.ID("tbl2", false),
	}
	tn3 := types.TableName{
		Database: types.ID("db", false),
		Schema:   types.ID("sc", false),
		Table:    types.ID("tbl3", false),
	}

	cases := []struct {
		fk sql.ForeignKey
		s  string
	}{
		{
			fk: sql.ForeignKey{
				Name:     types.ID("fk_1", false),
				FKTable:  tn1,
				FKCols:   []types.Identifier{types.ID("c1", false)},
				RefTable: tn2,
			},
			s: "CONSTRAINT fk_1 FOREIGN KEY (c1) REFERENCES db.sc.tbl2",
		},
		{
			fk: sql.ForeignKey{
				Name:     types.ID("fk_1", false),
				FKTable:  tn1,
				FKCols:   []types.Identifier{types.ID("c1", false)},
				RefTable: tn2,
				RefCols:  []types.Identifier{types.ID("d2", false)},
			},
			s: "CONSTRAINT fk_1 FOREIGN KEY (c1) REFERENCES db.sc.tbl2 (d2)",
		},
		{
			fk: sql.ForeignKey{
				Name:     types.ID("fk_1", false),
				FKTable:  tn2,
				FKCols:   []types.Identifier{types.ID("c2", false), types.ID("b2", false)},
				RefTable: tn1,
			},
			s: "CONSTRAINT fk_1 FOREIGN KEY (c2, b2) REFERENCES db.sc.tbl1",
		},
		{
			fk: sql.ForeignKey{
				Name:     types.ID("fk_1", false),
				FKTable:  tn2,
				FKCols:   []types.Identifier{types.ID("c2", false), types.ID("b2", false)},
				RefTable: tn1,
				RefCols:  []types.Identifier{types.ID("a1", false), types.ID("c1", false)},
			},
			s: "CONSTRAINT fk_1 FOREIGN KEY (c2, b2) REFERENCES db.sc.tbl1 (a1, c1)",
		},
		{
			fk: sql.ForeignKey{
				Name:     types.ID("fk_1", false),
				FKTable:  tn2,
				FKCols:   []types.Identifier{types.ID("b2", false), types.ID("c2", false)},
				RefTable: tn1,
				RefCols:  []types.Identifier{types.ID("c1", false), types.ID("a1", false)},
			},
			s: "CONSTRAINT fk_1 FOREIGN KEY (b2, c2) REFERENCES db.sc.tbl1 (c1, a1)",
		},
		{
			fk: sql.ForeignKey{
				Name:    types.ID("fk_1", false),
				FKTable: tn3,
				FKCols: []types.Identifier{
					types.ID("a3", false),
					types.ID("b3", false),
					types.ID("d3", false),
				},
				RefTable: tn1,
				RefCols: []types.Identifier{
					types.ID("a1", false),
					types.ID("b1", false),
					types.ID("d1", false),
				},
			},
			s: "CONSTRAINT fk_1 FOREIGN KEY (a3, b3, d3) REFERENCES db.sc.tbl1 (a1, b1, d1)",
		},
		{
			fk: sql.ForeignKey{
				Name:    types.ID("fk_1", false),
				FKTable: tn3,
				FKCols: []types.Identifier{
					types.ID("a3", false),
					types.ID("b3", false),
					types.ID("d3", false),
				},
				RefTable: tn1,
				RefCols: []types.Identifier{
					types.ID("b1", false),
					types.ID("a1", false),
					types.ID("d1", false),
				},
			},
			s: "CONSTRAINT fk_1 FOREIGN KEY (a3, b3, d3) REFERENCES db.sc.tbl1 (b1, a1, d1)",
		},
		{
			fk: sql.ForeignKey{
				Name:    types.ID("fk_1", false),
				FKTable: tn3,
				FKCols: []types.Identifier{
					types.ID("b3", false),
					types.ID("d3", false),
					types.ID("a3", false),
				},
				RefTable: tn1,
				RefCols: []types.Identifier{
					types.ID("b1", false),
					types.ID("d1", false),
					types.ID("a1", false),
				},
			},
			s: "CONSTRAINT fk_1 FOREIGN KEY (b3, d3, a3) REFERENCES db.sc.tbl1 (b1, d1, a1)",
		},
		{
			fk: sql.ForeignKey{
				Name:    types.ID("fk_1", false),
				FKTable: tn3,
				FKCols: []types.Identifier{
					types.ID("b3", false),
					types.ID("c3", false),
					types.ID("d3", false),
				},
				RefTable: tn1,
				RefCols: []types.Identifier{
					types.ID("b1", false),
					types.ID("c1", false),
					types.ID("d1", false),
				},
			},
			s: "CONSTRAINT fk_1 FOREIGN KEY (b3, c3, d3) REFERENCES db.sc.tbl1 (b1, c1, d1)",
		},
	}

	for _, c := range cases {
		s := c.fk.String()
		if s != c.s {
			t.Errorf("%#v.String(): got %s want %s", c.fk, s, c.s)
		}
	}
}

func TestCreateIndex(t *testing.T) {
	cases := []struct {
		stmt sql.CreateIndex
		s    string
	}{
		{
			stmt: sql.CreateIndex{
				Index: types.ID("idx", false),
				Table: types.TableName{
					Schema: types.ID("s", false),
					Table:  types.ID("t", false),
				},
				Key: sql.IndexKey{
					Columns: []types.Identifier{types.ID("c1", false), types.ID("c2", false)},
					Reverse: []bool{false, true},
				},
			},
			s: "CREATE INDEX idx ON s.t (c1 ASC, c2 DESC)",
		},
		{
			stmt: sql.CreateIndex{
				Index: types.ID("idx", false),
				Table: types.TableName{
					Schema: types.ID("s", false),
					Table:  types.ID("t", false),
				},
				Key: sql.IndexKey{
					Columns: []types.Identifier{types.ID("c1", false)},
					Reverse: []bool{false},
					Unique:  true,
				},
				IfNotExists: true,
			},
			s: "CREATE UNIQUE INDEX IF NOT EXISTS idx ON s.t (c1 ASC)",
		},
	}

	for _, c := range cases {
		s := c.stmt.String()
		if s != c.s {
			t.Errorf("%#v.String() got %s want %s", c.stmt, s, c.s)
		}
	}
}

func TestCreateDatabase(t *testing.T) {
	cases := []struct {
		stmt sql.CreateDatabase
		s    string
	}{
		{
			stmt: sql.CreateDatabase{
				Database: types.ID("db", false),
				Options: map[types.Identifier]string{
					types.ID("option", false): "value",
				},
			},
			s: "CREATE DATABASE db WITH option = value",
		},
	}

	for _, c := range cases {
		s := c.stmt.String()
		if s != c.s {
			t.Errorf("%#v.String() got %s want %s", c.stmt, s, c.s)
		}
	}
}

func TestCreateSchema(t *testing.T) {
	cases := []struct {
		stmt sql.CreateSchema
		s    string
	}{
		{
			stmt: sql.CreateSchema{
				Schema: types.SchemaName{
					Database: types.ID("db", false),
					Schema:   types.ID("scm", false),
				},
			},
			s: "CREATE SCHEMA db.scm",
		},
	}

	for _, c := range cases {
		s := c.stmt.String()
		if s != c.s {
			t.Errorf("%#v.String() got %s want %s", c.stmt, s, c.s)
		}
	}
}

func TestAlterTable(t *testing.T) {
	cases := []struct {
		stmt sql.AlterTable
		s    string
	}{
		{
			stmt: sql.AlterTable{
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
			s: "ALTER TABLE tbl ADD CONSTRAINT FOREIGN KEY (c1, c2) REFERENCES rtbl",
		},
		{
			stmt: sql.AlterTable{
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
			s: "ALTER TABLE tbl ADD CONSTRAINT con FOREIGN KEY (c1) REFERENCES rtbl",
		},
		{
			stmt: sql.AlterTable{
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
			s: "ALTER TABLE tbl ADD CONSTRAINT con1 FOREIGN KEY (c1) REFERENCES rtbl, ADD CONSTRAINT con2 FOREIGN KEY (c2) REFERENCES tbl2",
		},
		{
			stmt: sql.AlterTable{
				Table: types.TableName{Table: types.ID("tbl", false)},
				Actions: []sql.AlterAction{
					&sql.DropConstraint{
						Name:     types.ID("con", false),
						IfExists: true,
					},
				},
			},
			s: "ALTER TABLE tbl DROP CONSTRAINT IF EXISTS con",
		},
		{
			stmt: sql.AlterTable{
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
			s: "ALTER TABLE tbl ADD CONSTRAINT con FOREIGN KEY (c1) REFERENCES rtbl, ALTER COLUMN c1 DROP DEFAULT, ALTER COLUMN c2 DROP NOT NULL, DROP CONSTRAINT con",
		},
	}

	for _, c := range cases {
		s := c.stmt.String()
		if s != c.s {
			t.Errorf("%#v.String() got %s want %s", c.stmt, s, c.s)
		}
	}
}

func TestDropIndex(t *testing.T) {
	cases := []struct {
		stmt sql.DropIndex
		s    string
	}{
		{
			stmt: sql.DropIndex{
				Table: types.TableName{
					Database: types.ID("db", false),
					Schema:   types.ID("scm", false),
					Table:    types.ID("tbl", false),
				},
				Index: types.ID("idx", false),
			},
			s: "DROP INDEX idx ON db.scm.tbl",
		},
		{
			stmt: sql.DropIndex{
				Table:    types.TableName{Table: types.ID("t", false)},
				IfExists: true,
				Index:    types.ID("idx", false),
			},
			s: "DROP INDEX IF EXISTS idx ON t",
		},
	}

	for _, c := range cases {
		s := c.stmt.String()
		if s != c.s {
			t.Errorf("%#v.String() got %s want %s", c.stmt, s, c.s)
		}
	}
}

func TestDropDatabase(t *testing.T) {
	cases := []struct {
		stmt sql.DropDatabase
		s    string
	}{
		{
			stmt: sql.DropDatabase{
				Database: types.ID("db", false),
			},
			s: "DROP DATABASE db",
		},
		{
			stmt: sql.DropDatabase{
				IfExists: true,
				Database: types.ID("db", false),
				Options: map[types.Identifier]string{
					types.ID("option", false): "value",
				},
			},
			s: "DROP DATABASE IF EXISTS db WITH option = value",
		},
	}

	for _, c := range cases {
		s := c.stmt.String()
		if s != c.s {
			t.Errorf("%#v.String() got %s want %s", c.stmt, s, c.s)
		}
	}
}

func TestDropSchema(t *testing.T) {
	cases := []struct {
		stmt sql.DropSchema
		s    string
	}{
		{
			stmt: sql.DropSchema{
				Schema: types.SchemaName{
					Database: types.ID("db", false),
					Schema:   types.ID("scm", false),
				},
			},
			s: "DROP SCHEMA db.scm",
		},
		{
			stmt: sql.DropSchema{
				IfExists: true,
				Schema: types.SchemaName{
					Schema: types.ID("scm", false),
				},
			},
			s: "DROP SCHEMA IF EXISTS scm",
		},
	}

	for _, c := range cases {
		s := c.stmt.String()
		if s != c.s {
			t.Errorf("%#v.String() got %s want %s", c.stmt, s, c.s)
		}
	}
}
