package sql

import (
	"fmt"
	"strings"

	"github.com/leftmike/maho/types"
)

type IndexKey struct {
	Unique  bool
	Columns []types.Identifier
	Reverse []bool // ASC = false, DESC = true
}

func (ik IndexKey) String() string {
	var buf strings.Builder
	buf.WriteRune('(')
	for i := range ik.Columns {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(ik.Columns[i].String())
		if ik.Reverse[i] {
			buf.WriteString(" DESC")
		} else {
			buf.WriteString(" ASC")
		}
	}
	buf.WriteRune(')')
	return buf.String()
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
	ColNum int      // Default, NotNull, and Column Check constraints
	Key    IndexKey // Primary and Unique constraints
	Check  Expr     // Check constraints
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
	var buf strings.Builder
	if fk.Name == 0 {
		buf.WriteString("CONSTRAINT FOREIGN KEY (")
	} else {
		fmt.Fprintf(&buf, "CONSTRAINT %s FOREIGN KEY (", fk.Name)
	}
	for i, c := range fk.FKCols {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(c.String())
	}
	fmt.Fprintf(&buf, ") REFERENCES %s", fk.RefTable)
	if len(fk.RefCols) > 0 {
		buf.WriteString(" (")
		for i, c := range fk.RefCols {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(c.String())
		}
		buf.WriteRune(')')
	}
	return buf.String()
}

type CreateTable struct {
	Table          types.TableName
	Columns        []types.Identifier
	ColumnTypes    []types.ColumnType
	ColumnDefaults []Expr
	IfNotExists    bool
	Constraints    []Constraint
	ForeignKeys    []*ForeignKey
}

func (stmt *CreateTable) String() string {
	var buf strings.Builder
	buf.WriteString("CREATE TABLE")
	if stmt.IfNotExists {
		buf.WriteString(" IF NOT EXISTS")
	}
	fmt.Fprintf(&buf, " %s (", stmt.Table)

	for i, ct := range stmt.ColumnTypes {
		if i > 0 {
			buf.WriteString(", ")
		}
		fmt.Fprintf(&buf, "%s %s", stmt.Columns[i], ct.Type)
		if ct.NotNull {
			buf.WriteString(" NOT NULL")
		}
		cd := stmt.ColumnDefaults[i]
		if cd != nil {
			fmt.Fprintf(&buf, " DEFAULT %s", cd)
		}
	}
	for _, c := range stmt.Constraints {
		buf.WriteString(c.String())
	}
	for _, fk := range stmt.ForeignKeys {
		buf.WriteString(", ")
		buf.WriteString(fk.String())
	}
	buf.WriteRune(')')
	return buf.String()
}

func (stmt *CreateTable) Resolve(r Resolver) {
	stmt.Table = r.ResolveTable(stmt.Table)
}

type CreateIndex struct {
	Index       types.Identifier
	Table       types.TableName
	Key         IndexKey
	IfNotExists bool
}

func (stmt *CreateIndex) String() string {
	var buf strings.Builder
	buf.WriteString("CREATE")
	if stmt.Key.Unique {
		buf.WriteString(" UNIQUE")
	}
	buf.WriteString(" INDEX")
	if stmt.IfNotExists {
		buf.WriteString(" IF NOT EXISTS")
	}
	fmt.Fprintf(&buf, " %s ON %s %s", stmt.Index, stmt.Table, stmt.Key)
	return buf.String()
}

func (stmt *CreateIndex) Resolve(r Resolver) {
	stmt.Table = r.ResolveTable(stmt.Table)
}

type CreateDatabase struct {
	Database types.Identifier
	Options  map[types.Identifier]string
}

func (stmt *CreateDatabase) String() string {
	var buf strings.Builder
	fmt.Fprintf(&buf, "CREATE DATABASE %s", stmt.Database)
	if len(stmt.Options) > 0 {
		buf.WriteString(" WITH")
		for opt, val := range stmt.Options {
			fmt.Fprintf(&buf, " %s = %s", opt, val)
		}
	}
	return buf.String()
}

func (_ *CreateDatabase) Resolve(r Resolver) {}

type CreateSchema struct {
	Schema types.SchemaName
}

func (stmt *CreateSchema) String() string {
	return fmt.Sprintf("CREATE SCHEMA %s", stmt.Schema)
}

func (stmt *CreateSchema) Resolve(r Resolver) {
	stmt.Schema = r.ResolveSchema(stmt.Schema)
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
		if dc.IfExists {
			return fmt.Sprintf("DROP CONSTRAINT IF EXISTS %s", dc.Name)
		}
		return fmt.Sprintf("DROP CONSTRAINT %s", dc.Name)
	}

	return fmt.Sprintf("ALTER COLUMN %s DROP %s", dc.Column, dc.Type)
}

func (stmt *AlterTable) String() string {
	var buf strings.Builder
	fmt.Fprintf(&buf, "ALTER TABLE %s ", stmt.Table)
	for adx, act := range stmt.Actions {
		if adx > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(act.String())
	}
	return buf.String()
}

func (stmt *AlterTable) Resolve(r Resolver) {
	stmt.Table = r.ResolveTable(stmt.Table)
}

type DropTable struct {
	IfExists bool
	Cascade  bool
	Tables   []types.TableName
}

func (stmt *DropTable) String() string {
	var buf strings.Builder
	buf.WriteString("DROP TABLE ")
	if stmt.IfExists {
		buf.WriteString("IF EXISTS ")
	}
	for i, tbl := range stmt.Tables {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(tbl.String())
	}
	if stmt.Cascade {
		buf.WriteString(" CASCADE")
	}
	return buf.String()
}

func (stmt *DropTable) Resolve(r Resolver) {
	for idx := range stmt.Tables {
		stmt.Tables[idx] = r.ResolveTable(stmt.Tables[idx])
	}
}

type DropIndex struct {
	Table    types.TableName
	Index    types.Identifier
	IfExists bool
}

func (stmt *DropIndex) String() string {
	if stmt.IfExists {
		return fmt.Sprintf("DROP INDEX IF EXISTS %s ON %s", stmt.Index, stmt.Table)
	}
	return fmt.Sprintf("DROP INDEX %s ON %s", stmt.Index, stmt.Table)
}

func (stmt *DropIndex) Resolve(r Resolver) {
	stmt.Table = r.ResolveTable(stmt.Table)
}

type DropDatabase struct {
	IfExists bool
	Database types.Identifier
	Options  map[types.Identifier]string
}

func (stmt *DropDatabase) String() string {
	var buf strings.Builder
	buf.WriteString("DROP DATABASE ")
	if stmt.IfExists {
		buf.WriteString("IF EXISTS ")
	}
	buf.WriteString(stmt.Database.String())
	if len(stmt.Options) > 0 {
		buf.WriteString(" WITH")
		for opt, val := range stmt.Options {
			fmt.Fprintf(&buf, " %s = %s", opt, val)
		}
	}
	return buf.String()
}

func (_ *DropDatabase) Resolve(r Resolver) {}

type DropSchema struct {
	IfExists bool
	Schema   types.SchemaName
}

func (stmt *DropSchema) String() string {
	if stmt.IfExists {
		return fmt.Sprintf("DROP SCHEMA IF EXISTS %s", stmt.Schema)
	}
	return fmt.Sprintf("DROP SCHEMA %s", stmt.Schema)
}

func (stmt *DropSchema) Resolve(r Resolver) {
	stmt.Schema = r.ResolveSchema(stmt.Schema)
}
