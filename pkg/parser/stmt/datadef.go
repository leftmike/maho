package stmt

import (
	"fmt"

	"github.com/leftmike/maho/pkg/parser/expr"
	"github.com/leftmike/maho/pkg/types"
)

type IndexKey struct {
	Unique  bool
	Columns []types.Identifier
	Reverse []bool // ASC = false, DESC = true
}

func (ik IndexKey) String() string {
	s := "("
	for i := range ik.Columns {
		if i > 0 {
			s += ", "
		}
		if ik.Reverse[i] {
			s += fmt.Sprintf("%s DESC", ik.Columns[i])
		} else {
			s += fmt.Sprintf("%s ASC", ik.Columns[i])
		}
	}
	s += ")"
	return s
}

func (ik IndexKey) Equal(oik IndexKey) bool {
	if len(ik.Columns) != len(oik.Columns) {
		return false
	}

	for cdx := range ik.Columns {
		if ik.Columns[cdx] != oik.Columns[cdx] || ik.Reverse[cdx] != oik.Reverse[cdx] {
			return false
		}
	}
	return true
}

type ConstraintType int

const (
	DefaultConstraint ConstraintType = iota + 1
	NotNullConstraint
	PrimaryConstraint
	UniqueConstraint
	CheckConstraint
)

func (ct ConstraintType) String() string {
	switch ct {
	case DefaultConstraint:
		return "DEFAULT"
	case NotNullConstraint:
		return "NOT NULL"
	case PrimaryConstraint:
		return "PRIMARY KEY"
	case UniqueConstraint:
		return "UNIQUE"
	case CheckConstraint:
		return "CHECK"
	default:
		panic(fmt.Sprintf("unexpected constraint type: %d", ct))
	}
}

type Constraint struct {
	Type   ConstraintType
	Name   types.Identifier
	ColNum int       // Default, NotNull, and Column Check constraints
	Key    IndexKey  // Primary and Unique constraints
	Check  expr.Expr // Check constraints
}

func (c Constraint) String() string {
	switch c.Type {
	case DefaultConstraint:
	case NotNullConstraint:
	case PrimaryConstraint:
		return fmt.Sprintf(", CONSTRAINT %s PRIMARY KEY %s", c.Name, c.Key)
	case UniqueConstraint:
		return fmt.Sprintf(", CONSTRAINT %s UNIQUE %s", c.Name, c.Key)
	case CheckConstraint:
		return fmt.Sprintf(", CONSTRAINT %s CHECK (%s)", c.Name, c.Check)
	default:
		panic(fmt.Sprintf("unexpected constraint type: %d", c.Type))
	}

	return ""
}

type RefAction int

const (
	NoAction RefAction = iota
	Restrict
	Cascade
	SetNull
	SetDefault
)

type ForeignKey struct {
	Name     types.Identifier
	FKTable  types.TableName
	FKCols   []types.Identifier
	RefTable types.TableName
	RefCols  []types.Identifier
	OnDelete RefAction
	OnUpdate RefAction
}

func (fk ForeignKey) String() string {
	var s string
	if fk.Name == 0 {
		s = "CONSTRAINT FOREIGN KEY ("
	} else {
		s = fmt.Sprintf("CONSTRAINT %s FOREIGN KEY (", fk.Name)
	}
	for i, c := range fk.FKCols {
		if i > 0 {
			s += ", "
		}
		s += c.String()
	}
	s += fmt.Sprintf(") REFERENCES %s", fk.RefTable)
	if len(fk.RefCols) > 0 {
		s += " ("
		for i, c := range fk.RefCols {
			if i > 0 {
				s += ", "
			}
			s += c.String()
		}
		s += ")"
	}
	return s
}

type CreateTable struct {
	Table          types.TableName
	Columns        []types.Identifier
	ColumnTypes    []types.ColumnType
	ColumnDefaults []expr.Expr
	IfNotExists    bool
	Constraints    []Constraint
	ForeignKeys    []*ForeignKey
}

func (stmt *CreateTable) String() string {
	s := "CREATE TABLE"
	if stmt.IfNotExists {
		s += " IF NOT EXISTS"
	}
	s = fmt.Sprintf("%s %s (", s, stmt.Table)

	for i, ct := range stmt.ColumnTypes {
		if i > 0 {
			s += ", "
		}
		s += fmt.Sprintf("%s %s", stmt.Columns[i], ct.Type)
		if ct.NotNull {
			s += " NOT NULL"
		}
		cd := stmt.ColumnDefaults[i]
		if cd != nil {
			s += fmt.Sprintf(" DEFAULT %s", cd)
		}
	}
	for _, c := range stmt.Constraints {
		s += c.String()
	}
	for _, fk := range stmt.ForeignKeys {
		s += ", "
		s += fk.String()
	}
	s += ")"
	return s
}

type CreateIndex struct {
	Index       types.Identifier
	Table       types.TableName
	Key         IndexKey
	IfNotExists bool
}

func (stmt *CreateIndex) String() string {
	s := "CREATE"
	if stmt.Key.Unique {
		s += " UNIQUE "
	}
	s += " INDEX"
	if stmt.IfNotExists {
		s += " IF NOT EXISTS"
	}
	s += fmt.Sprintf(" %s ON %s (%s)", stmt.Index, stmt.Table, stmt.Key)
	return s
}

type CreateDatabase struct {
	Database types.Identifier
	Options  map[types.Identifier]string
}

func (stmt *CreateDatabase) String() string {
	s := fmt.Sprintf("CREATE DATABASE %s", stmt.Database)
	if len(stmt.Options) > 0 {
		s += " WITH"
		for opt, val := range stmt.Options {
			s = fmt.Sprintf("%s %s = %s", s, opt, val)
		}
	}
	return s
}

type CreateSchema struct {
	Schema types.SchemaName
}

func (stmt *CreateSchema) String() string {
	return fmt.Sprintf("CREATE SCHEMA %s", stmt.Schema)
}

type AlterAction interface {
	String() string
}

type AddForeignKey struct {
	ForeignKey
}

type DropConstraint struct {
	Name     types.Identifier
	IfExists bool
	Column   types.Identifier
	Type     ConstraintType
}

type AlterTable struct {
	Table   types.TableName
	Actions []AlterAction
}

func (afk AddForeignKey) String() string {
	return fmt.Sprintf("ADD %s", afk.ForeignKey)
}

func (dc DropConstraint) String() string {
	if dc.Name != 0 {
		s := "DROP CONSTRAINT"
		if dc.IfExists {
			s += " IF EXISTS"
		}
		return fmt.Sprintf("%s %s", s, dc.Name)
	}

	return fmt.Sprintf("ALTER %s DROP %s", dc.Column, dc.Type)
}

func (stmt *AlterTable) String() string {
	s := fmt.Sprintf("ALTER TABLE %s ", stmt.Table)
	for adx, act := range stmt.Actions {
		if adx > 0 {
			s += ", "
		}
		s += act.String()
	}
	return s
}

type DropTable struct {
	IfExists bool
	Cascade  bool
	Tables   []types.TableName
}

func (stmt *DropTable) String() string {
	s := "DROP TABLE "
	if stmt.IfExists {
		s += "IF EXISTS "
	}
	for i, tbl := range stmt.Tables {
		if i > 0 {
			s += ", "
		}
		s += tbl.String()
	}
	if stmt.Cascade {
		s += " CASCADE"
	}
	return s
}

type DropIndex struct {
	Table    types.TableName
	Index    types.Identifier
	IfExists bool
}

func (stmt *DropIndex) String() string {
	s := "DROP INDEX "
	if stmt.IfExists {
		s += "IF EXISTS "
	}
	s += fmt.Sprintf("%s ON %s", stmt.Index, stmt.Table)
	return s
}

type DropDatabase struct {
	IfExists bool
	Database types.Identifier
	Options  map[types.Identifier]string
}

func (stmt *DropDatabase) String() string {
	s := "DROP DATABASE "
	if stmt.IfExists {
		s += "IF EXISTS "
	}
	s += stmt.Database.String()
	if len(stmt.Options) > 0 {
		s += " WITH"
		for opt, val := range stmt.Options {
			s = fmt.Sprintf("%s %s = %s", s, opt, val)
		}
	}
	return s
}

type DropSchema struct {
	IfExists bool
	Schema   types.SchemaName
}

func (stmt *DropSchema) String() string {
	s := "DROP SCHEMA "
	if stmt.IfExists {
		s += "IF EXISTS "
	}
	return s + stmt.Schema.String()
}
