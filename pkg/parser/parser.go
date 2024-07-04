package parser

import (
	"fmt"
	"io"
	"runtime"
	"strconv"

	"github.com/leftmike/maho/pkg/parser/expr"
	"github.com/leftmike/maho/pkg/parser/scanner"
	"github.com/leftmike/maho/pkg/parser/stmt"
	"github.com/leftmike/maho/pkg/parser/token"
	"github.com/leftmike/maho/pkg/types"
)

const lookBackAmount = 3

type Parser struct {
	scanner   scanner.Scanner
	lookBack  [lookBackAmount]scanner.ScanCtx
	sctx      *scanner.ScanCtx // = &lookBack[current]
	current   uint
	unscanned uint
	scanned   rune
	failed    bool
}

func NewParser(rr io.RuneReader, fn string) *Parser {
	var p Parser
	p.scanner.Init(rr, fn)
	return &p
}

func (p *Parser) Parse() (stmt stmt.Stmt, err error) {
	t, err := p.scanRune()
	if err != nil {
		return nil, err
	} else if t == token.EOF {
		return nil, io.EOF
	}
	p.unscan()

	if p.failed {
		for {
			t, err = p.scanRune()
			if err != nil {
				return nil, err
			} else if t == token.EOF {
				return nil, io.EOF
			} else if t == token.EndOfStatement {
				break
			}
		}
		p.failed = false
	}

	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(runtime.Error); ok {
				panic(r)
			}
			err = r.(error)
			stmt = nil
			p.failed = (p.sctx.Token != token.EndOfStatement)
		}
	}()

	stmt = p.parseStmt()
	p.expectEndOfStatement()
	return
}

func (p *Parser) error(msg string) {
	panic(fmt.Errorf("parser: %s: %s", p.sctx.Position, msg))
}

func (p *Parser) scanRune() (rune, error) {
	p.current = (p.current + 1) % lookBackAmount
	p.sctx = &p.lookBack[p.current]

	if p.unscanned > 0 {
		p.unscanned -= 1
	} else {
		p.scanner.Scan(p.sctx)
		if p.sctx.Token == token.Error {
			return 0, p.sctx.Error
		}
	}
	return p.sctx.Token, nil
}

func (p *Parser) scan() rune {
	r, err := p.scanRune()
	if err != nil {
		p.error(err.Error())
	}
	return r
}

func (p *Parser) unscan() {
	p.unscanned += 1
	if p.unscanned > lookBackAmount {
		panic("parser: too much lookback")
	}
	if p.current == 0 {
		p.current = lookBackAmount - 1
	} else {
		p.current -= 1
	}
	p.sctx = &p.lookBack[p.current]
}

func (p *Parser) got() string {
	switch p.sctx.Token {
	case token.EOF:
		return fmt.Sprintf("end of file")
	case token.EndOfStatement:
		return fmt.Sprintf("end of statement (;)")
	case token.Error:
		return fmt.Sprintf("error %s", p.sctx.Error.Error())
	case token.Identifier:
		return fmt.Sprintf("identifier %s", p.sctx.Identifier)
	case token.Reserved:
		return fmt.Sprintf("reserved identifier %s", p.sctx.Identifier)
	case token.String:
		return fmt.Sprintf("string %q", p.sctx.String)
	case token.Bytes:
		return fmt.Sprintf("bytes %v", p.sctx.String)
	case token.Integer:
		return fmt.Sprintf("integer %d", p.sctx.Integer)
	case token.Float:
		return fmt.Sprintf("float %f", p.sctx.Float)
	}

	return token.Format(p.sctx.Token)
}

func (p *Parser) expectReserved(ids ...types.Identifier) types.Identifier {
	t := p.scan()
	if t == token.Reserved {
		for _, kw := range ids {
			if kw == p.sctx.Identifier {
				return kw
			}
		}
	}

	var msg string
	if len(ids) == 1 {
		msg = ids[0].String()
	} else {
		for i, kw := range ids {
			if i == len(ids)-1 {
				msg += ", or "
			} else if i > 0 {
				msg += ", "
			}
			msg += kw.String()
		}
	}

	p.error(fmt.Sprintf("expected keyword %s; got %s", msg, p.got()))
	return 0
}

func (p *Parser) optionalReserved(ids ...types.Identifier) bool {
	t := p.scan()
	if t == token.Reserved {
		for _, kw := range ids {
			if kw == p.sctx.Identifier {
				return true
			}
		}
	}

	p.unscan()
	return false
}

func (p *Parser) expectIdentifier(msg string) types.Identifier {
	t := p.scan()
	if t != token.Identifier {
		p.error(fmt.Sprintf("%s, got %s", msg, p.got()))
	}
	return p.sctx.Identifier
}

func (p *Parser) maybeIdentifier(id types.Identifier) bool {
	if p.scan() == token.Identifier && p.sctx.Identifier == id {
		return true
	}

	p.unscan()
	return false
}

func (p *Parser) expectTokens(tokens ...rune) rune {
	t := p.scan()
	for _, r := range tokens {
		if t == r {
			return r
		}
	}

	var msg string
	if len(tokens) == 1 {
		msg = fmt.Sprintf("%s", token.Format(tokens[0]))
	} else {
		for i, r := range tokens {
			if i == len(tokens)-1 {
				msg += ", or "
			} else if i > 0 {
				msg += ", "
			}
			msg += fmt.Sprintf("%s", token.Format(r))
		}
	}

	p.error(fmt.Sprintf("expected %s, got %s", msg, p.got()))
	return 0
}

func (p *Parser) maybeToken(mr rune) bool {
	if p.scan() == mr {
		return true
	}
	p.unscan()
	return false
}

func (p *Parser) expectInteger(min, max int64) int64 {
	if p.scan() != token.Integer || p.sctx.Integer < min || p.sctx.Integer > max {
		p.error(fmt.Sprintf("expected a number between %d and %d inclusive, got %s", min, max,
			p.got()))
	}

	return p.sctx.Integer
}

func (p *Parser) expectEndOfStatement() {
	r := p.scan()
	if r != token.EOF && r != token.EndOfStatement {
		p.error(fmt.Sprintf("expected the end of the statement, got %s", p.got()))
	}
}

func (p *Parser) parseStmt() stmt.Stmt {
	if p.maybeToken(token.EndOfStatement) {
		return nil
	}

	switch p.expectReserved(
		types.ALTER,
		types.BEGIN,
		types.COMMIT,
		types.COPY,
		types.CREATE,
		types.DELETE,
		types.DETACH,
		types.DROP,
		types.EXECUTE,
		types.EXPLAIN,
		types.INSERT,
		types.PREPARE,
		types.ROLLBACK,
		types.SELECT,
		types.SET,
		types.SHOW,
		types.START,
		types.UPDATE,
		types.USE,
		types.VALUES,
	) {
	case types.ALTER:
		// ALTER TABLE ...
		p.expectReserved(types.TABLE)
		return p.parseAlterTable()
	case types.BEGIN:
		// BEGIN
		return &stmt.Begin{}
	case types.COMMIT:
		// COMMIT
		return &stmt.Commit{}
	case types.COPY:
		// COPY
		return p.parseCopy()
	case types.CREATE:
		switch p.expectReserved(types.DATABASE, types.INDEX, types.SCHEMA, types.TABLE,
			types.UNIQUE) {
		case types.DATABASE:
			// CREATE DATABASE ...
			return p.parseCreateDatabase()
		case types.INDEX:
			// CREATE INDEX ...
			return p.parseCreateIndex(false)
		case types.SCHEMA:
			// CREATE SCHEMA ...
			return p.parseCreateSchema()
		case types.TABLE:
			// CREATE TABLE ...
			return p.parseCreateTable()
		case types.UNIQUE:
			// CREATE UNIQUE INDEX ...
			p.expectReserved(types.INDEX)
			return p.parseCreateIndex(true)
		}
	case types.DELETE:
		// DELETE FROM ...
		p.expectReserved(types.FROM)
		return p.parseDelete()
	case types.DROP:
		switch p.expectReserved(types.DATABASE, types.INDEX, types.SCHEMA, types.TABLE) {
		case types.DATABASE:
			// DROP DATABASE ...
			return p.parseDropDatabase()
		case types.INDEX:
			// DROP INDEX ...
			return p.parseDropIndex()
		case types.SCHEMA:
			// DROP SCHEMA ...
			return p.parseDropSchema()
		case types.TABLE:
			// DROP TABLE ...
			return p.parseDropTable()
		}
		/*
			case types.EXECUTE:
				return p.parseExecute()
			case types.EXPLAIN:
				return p.parseExplain()
		*/
	case types.INSERT:
		// INSERT INTO ...
		p.expectReserved(types.INTO)
		return p.parseInsert()
		/*
			case types.PREPARE:
				return p.parsePrepare()
		*/
	case types.ROLLBACK:
		// ROLLBACK
		return &stmt.Rollback{}
	case types.SELECT:
		// SELECT ...
		return p.parseSelect()
	case types.SET:
		// SET ...
		return p.parseSet()
	case types.SHOW:
		// SHOW ...
		return p.parseShow()
	case types.START:
		// START TRANSACTION
		p.expectReserved(types.TRANSACTION)
		return &stmt.Begin{}
	case types.UPDATE:
		// UPDATE ...
		return p.parseUpdate()
	case types.USE:
		// USE ...
		return p.parseUse()
	case types.VALUES:
		// VALUES ...
		return p.parseValues()
	}

	return nil
}

func (p *Parser) parseSchemaName() types.SchemaName {
	var sn types.SchemaName
	id := p.expectIdentifier("expected a database or a schema")
	if p.maybeToken(token.Dot) {
		sn.Database = id
		sn.Schema = p.expectIdentifier("expected a schema")
	} else {
		sn.Schema = id
	}
	return sn
}

func (p *Parser) parseTableName() types.TableName {
	var tn types.TableName
	tn.Table = p.expectIdentifier("expected a database, schema, or table")
	if p.maybeToken(token.Dot) {
		tn.Schema = tn.Table
		tn.Table = p.expectIdentifier("expected a schema or table")
		if p.maybeToken(token.Dot) {
			tn.Database = tn.Schema
			tn.Schema = tn.Table
			tn.Table = p.expectIdentifier("expected a table")
		}
	}
	return tn
}

func (p *Parser) parseAlias(required bool) types.Identifier {
	if p.optionalReserved(types.AS) {
		return p.expectIdentifier("expected an alias")
	}
	r := p.scan()
	if r == token.Identifier {
		return p.sctx.Identifier
	} else if required {
		p.error("an alias is required")
	}
	p.unscan()
	return 0
}

func (p *Parser) parseTableAlias() stmt.FromItem {
	tn := p.parseTableName()
	if p.maybeToken(token.AtSign) {
		return &stmt.FromIndexAlias{
			TableName: tn,
			Index:     p.expectIdentifier("expected an index"),
			Alias:     p.parseAlias(false),
		}
	}
	return &stmt.FromTableAlias{TableName: tn, Alias: p.parseAlias(false)}
}

func (p *Parser) parseColumnAliases() []types.Identifier {
	if !p.maybeToken(token.LParen) {
		return nil
	}

	var cols []types.Identifier
	for {
		cols = append(cols, p.expectIdentifier("expected a column alias"))
		if p.maybeToken(token.RParen) {
			break
		}
		p.expectTokens(token.Comma)
	}
	return cols
}

func (p *Parser) parseCreateTable() stmt.Stmt {
	// CREATE TABLE [IF NOT EXISTS] [[database '.'] schema '.'] table ...
	var s stmt.CreateTable

	if p.optionalReserved(types.IF) {
		p.expectReserved(types.NOT)
		p.expectReserved(types.EXISTS)
		s.IfNotExists = true
	}

	s.Table = p.parseTableName()
	p.expectTokens(token.LParen)
	p.parseCreateDetails(&s)
	return &s
}

func (p *Parser) parseKey(unique bool) stmt.IndexKey {
	key := stmt.IndexKey{
		Unique: unique,
	}

	p.expectTokens('(')
	for {
		nam := p.expectIdentifier("expected a column name")
		for _, col := range key.Columns {
			if col == nam {
				p.error(fmt.Sprintf("duplicate column name: %s", nam))
			}
		}
		key.Columns = append(key.Columns, nam)

		if p.optionalReserved(types.ASC) {
			key.Reverse = append(key.Reverse, false)
		} else if p.optionalReserved(types.DESC) {
			key.Reverse = append(key.Reverse, true)
		} else {
			key.Reverse = append(key.Reverse, false)
		}

		r := p.expectTokens(token.Comma, token.RParen)
		if r == token.RParen {
			break
		}
	}

	return key
}

func (p *Parser) parseRefAction() stmt.RefAction {
	switch p.expectReserved(types.NO, types.RESTRICT, types.CASCADE, types.SET) {
	case types.NO:
		p.expectReserved(types.ACTION)
		return stmt.NoAction
	case types.RESTRICT:
		return stmt.Restrict
	case types.CASCADE:
		return stmt.Cascade
	case types.SET:
		switch p.expectReserved(types.NULL, types.DEFAULT) {
		case types.NULL:
			return stmt.SetNull
		case types.DEFAULT:
			return stmt.SetDefault
		}
	}
	panic("never reached")
}

func (p *Parser) parseOnActions(fk *stmt.ForeignKey) *stmt.ForeignKey {
	var onDelete, onUpdate bool
	for p.optionalReserved(types.ON) {
		if p.expectReserved(types.DELETE, types.UPDATE) == types.DELETE {
			if onDelete {
				p.error("ON DELETE may be specified once per foreign key")
			}
			fk.OnDelete = p.parseRefAction()
			onDelete = true
		} else {
			if onUpdate {
				p.error("ON UPDATE may be specified once per foreign key")
			}
			fk.OnUpdate = p.parseRefAction()
			onUpdate = true
		}
	}

	return fk
}

func (p *Parser) parseForeignKey(cn types.Identifier) *stmt.ForeignKey {
	var cols []types.Identifier
	p.expectTokens(token.LParen)
	for {
		cols = append(cols, p.expectIdentifier("expected a column name"))
		if p.maybeToken(token.RParen) {
			break
		}
		p.expectTokens(token.Comma)
	}

	p.expectReserved(types.REFERENCES)

	rtn := p.parseTableName()
	var refCols []types.Identifier
	if p.maybeToken(token.LParen) {
		for {
			refCols = append(refCols, p.expectIdentifier("expected a column name"))
			if p.maybeToken(token.RParen) {
				break
			}
			p.expectTokens(token.Comma)
		}
	}

	return p.parseOnActions(
		&stmt.ForeignKey{
			Name:     cn,
			FKCols:   cols,
			RefTable: rtn,
			RefCols:  refCols,
		})
}

func (p *Parser) parseCreateDetails(s *stmt.CreateTable) {
	/*
		CREATE TABLE [[database '.'] schema '.'] table
			'('	(column data_type [column_constraint] ...
				| [CONSTRAINT constraint] table_constraint) [',' ...] ')'
		table_constraint =
			  PRIMARY KEY key_columns
			| UNIQUE key_columns
			| CHECK '(' expr ')'
			| FOREIGN KEY columns REFERENCES [[database '.'] schema '.'] table [columns]
			  [ON DELETE referential_action] [ON UPDATE referential_action]
		key_columns = '(' column [ASC | DESC] [',' ...] ')'
		columns = '(' column [',' ...] ')'
		referential_action = NO ACTION | RESTRICT | CASCADE | SET NULL | SET DEFAULT
	*/

	for {
		var cn types.Identifier
		if p.optionalReserved(types.CONSTRAINT) {
			cn = p.expectIdentifier("expected a constraint name")
		}

		if p.optionalReserved(types.PRIMARY) {
			p.expectReserved(types.KEY)
			key := p.parseKey(true)
			p.addKeyConstraint(s, stmt.PrimaryConstraint,
				makeKeyConstraintName(cn, key, "primary"), key)
		} else if p.optionalReserved(types.UNIQUE) {
			key := p.parseKey(true)
			p.addKeyConstraint(s, stmt.UniqueConstraint, makeKeyConstraintName(cn, key, "unique"),
				key)
		} else if p.optionalReserved(types.CHECK) {
			p.expectTokens(token.LParen)
			s.Constraints = append(s.Constraints,
				stmt.Constraint{
					Type:   stmt.CheckConstraint,
					Name:   cn,
					ColNum: -1,
					Check:  p.parseExpr(),
				})
			p.expectTokens(token.RParen)
		} else if p.optionalReserved(types.FOREIGN) {
			p.expectReserved(types.KEY)
			s.ForeignKeys = append(s.ForeignKeys, p.parseForeignKey(cn))
		} else if cn != 0 {
			p.error("CONSTRAINT name specified without a constraint")
		} else {
			p.parseColumn(s)
		}

		r := p.expectTokens(token.Comma, token.RParen)
		if r == token.RParen {
			break
		}
	}
}

var columnTypes = map[types.Identifier]types.ColumnType{
	types.BINARY:    {Type: types.BytesType, Fixed: true, Size: 1},
	types.VARBINARY: {Type: types.BytesType, Fixed: false},
	types.BLOB:      {Type: types.BytesType, Fixed: false, Size: types.MaxColumnSize},
	types.BYTEA:     {Type: types.BytesType, Fixed: false, Size: types.MaxColumnSize},
	types.BYTES:     {Type: types.BytesType, Fixed: false, Size: types.MaxColumnSize},
	types.CHAR:      {Type: types.StringType, Fixed: true, Size: 1},
	types.CHARACTER: {Type: types.StringType, Fixed: true, Size: 1},
	types.VARCHAR:   {Type: types.StringType, Fixed: false},
	types.TEXT:      {Type: types.StringType, Fixed: false, Size: types.MaxColumnSize},
	types.BOOL:      {Type: types.BoolType, Size: 1},
	types.BOOLEAN:   {Type: types.BoolType, Size: 1},
	types.DOUBLE:    {Type: types.Float64Type, Size: 8},
	types.REAL:      {Type: types.Float64Type, Size: 4},
	types.SMALLINT:  {Type: types.Int64Type, Size: 2},
	types.INT2:      {Type: types.Int64Type, Size: 2},
	types.INT:       {Type: types.Int64Type, Size: 4},
	types.INTEGER:   {Type: types.Int64Type, Size: 4},
	types.INT4:      {Type: types.Int64Type, Size: 4},
	types.INT8:      {Type: types.Int64Type, Size: 8},
	types.BIGINT:    {Type: types.Int64Type, Size: 8},
}

func (p *Parser) parseColumnType() types.ColumnType {
	/*
		data_type =
			  BINARY ['(' length ')']
			| VARBINARY ['(' length ')']
			| BLOB ['(' length ')']
			| BYTEA ['(' length ')']
			| BYTES ['(' length ')']
			| CHAR ['(' length ')']
			| CHARACTER ['(' length ')']
			| VARCHAR ['(' length ')']
			| TEXT ['(' length ')']
			| BOOL
			| BOOLEAN
			| DOUBLE [PRECISION]
			| REAL
			| SMALLINT
			| INT2
			| INT
			| INTEGER
			| INT4
			| INTEGER
			| BIGINT
			| INT8
	*/

	typ := p.expectIdentifier("expected a data type")
	def, found := columnTypes[typ]
	if !found {
		p.error(fmt.Sprintf("expected a data type, got %s", typ))
	}

	ct := def

	if typ == types.DOUBLE {
		p.maybeIdentifier(types.PRECISION)
	}

	if ct.Type == types.StringType || ct.Type == types.BytesType {
		if p.maybeToken(token.LParen) {
			ct.Size = uint32(p.expectInteger(0, types.MaxColumnSize))
			p.expectTokens(token.RParen)
		}
	}

	return ct
}

func makeKeyConstraintName(cn types.Identifier, key stmt.IndexKey, suffix string) types.Identifier {
	if cn != 0 {
		return cn
	}

	var nam string
	for _, col := range key.Columns {
		nam += col.String() + "_"
	}

	return types.ID(nam+suffix, false)
}

func (p *Parser) addKeyConstraint(s *stmt.CreateTable, ct stmt.ConstraintType,
	cn types.Identifier, nkey stmt.IndexKey) {

	for _, c := range s.Constraints {
		if c.Name == cn {
			p.error(fmt.Sprintf("duplicate constraint name: %s", cn))
		}
		if c.Type == stmt.PrimaryConstraint && ct == stmt.PrimaryConstraint {
			p.error("only one primary key allowed")
		}
	}

	for _, c := range s.Constraints {
		if nkey.Equal(c.Key) {
			p.error("duplicate keys not allowed")
		}
	}

	s.Constraints = append(s.Constraints,
		stmt.Constraint{
			Type:   ct,
			Name:   cn,
			ColNum: -1,
			Key:    nkey,
		})
}

func (p *Parser) addColumnConstraint(s *stmt.CreateTable, ct stmt.ConstraintType,
	cn types.Identifier, colNum int) {

	for _, c := range s.Constraints {
		if c.Name == cn {
			p.error(fmt.Sprintf("duplicate constraint name: %s", cn))
		} else if colNum == c.ColNum && ct == c.Type {
			p.error(fmt.Sprintf("duplicate %s constraint on %s", ct, s.Columns[colNum]))
		}
	}

	s.Constraints = append(s.Constraints,
		stmt.Constraint{
			Type:   ct,
			Name:   cn,
			ColNum: colNum,
		})
}

func (p *Parser) parseColumn(s *stmt.CreateTable) {
	/*
		column data_type [[CONSTRAINT constraint] column_constraint]
		column_constraint =
			  DEFAULT expr
			| NOT NULL
			| PRIMARY KEY
			| UNIQUE
			| CHECK '(' expr ')'
			| REFERENCES [[database '.'] schema '.'] table ['(' column ')']
			  [ON DELETE referential_action] [ON UPDATE referential_action]
		referential_action = NO ACTION | RESTRICT | CASCADE | SET NULL | SET DEFAULT
	*/

	nam := p.expectIdentifier("expected a column name")
	for _, col := range s.Columns {
		if col == nam {
			p.error(fmt.Sprintf("duplicate column name: %s", nam))
		}
	}
	s.Columns = append(s.Columns, nam)

	ct := p.parseColumnType()

	var dflt expr.Expr
	for {
		var cn types.Identifier
		if p.optionalReserved(types.CONSTRAINT) {
			cn = p.expectIdentifier("expected a constraint name")
		}

		if p.optionalReserved(types.DEFAULT) {
			if dflt != nil {
				p.error("DEFAULT specified more than once per column")
			}
			if cn != 0 {
				p.addColumnConstraint(s, stmt.DefaultConstraint, cn, len(s.Columns)-1)
			}
			dflt = p.parseExpr()
		} else if p.optionalReserved(types.NOT) {
			p.expectReserved(types.NULL)

			if ct.NotNull {
				p.error("NOT NULL specified more than once per column")
			}
			if cn != 0 {
				p.addColumnConstraint(s, stmt.NotNullConstraint, cn, len(s.Columns)-1)
			}
			ct.NotNull = true
		} else if p.optionalReserved(types.PRIMARY) {
			p.expectReserved(types.KEY)

			if cn == 0 {
				cn = types.ID(nam.String()+"_primary", false)
			}
			p.addKeyConstraint(s, stmt.PrimaryConstraint, cn,
				stmt.IndexKey{
					Unique:  true,
					Columns: []types.Identifier{nam},
					Reverse: []bool{false},
				})
		} else if p.optionalReserved(types.UNIQUE) {
			if cn == 0 {
				cn = types.ID(nam.String()+"_unique", false)
			}
			p.addKeyConstraint(s, stmt.UniqueConstraint, cn,
				stmt.IndexKey{
					Unique:  true,
					Columns: []types.Identifier{nam},
					Reverse: []bool{false},
				})
		} else if p.optionalReserved(types.CHECK) {
			p.expectTokens(token.LParen)
			s.Constraints = append(s.Constraints,
				stmt.Constraint{
					Type:   stmt.CheckConstraint,
					Name:   cn,
					ColNum: len(s.Columns) - 1,
					Check:  p.parseExpr(),
				})
			p.expectTokens(token.RParen)
		} else if p.optionalReserved(types.REFERENCES) {
			rtn := p.parseTableName()
			var refCols []types.Identifier
			if p.maybeToken(token.LParen) {
				refCols = []types.Identifier{p.expectIdentifier("expected a column name")}
				p.expectTokens(token.RParen)
			}

			s.ForeignKeys = append(s.ForeignKeys,
				p.parseOnActions(
					&stmt.ForeignKey{
						Name:     cn,
						FKCols:   []types.Identifier{nam},
						RefTable: rtn,
						RefCols:  refCols,
					}))
		} else if cn != 0 {
			p.error("CONSTRAINT name specified without a constraint")
		} else {
			break
		}
	}

	s.ColumnTypes = append(s.ColumnTypes, ct)
	s.ColumnDefaults = append(s.ColumnDefaults, dflt)
}

func (p *Parser) parseAlterTable() stmt.Stmt {
	// ALTER TABLE table action [',' ...]
	// action =
	//      ADD [CONSTRAINT constraint] table_constraint
	//    | DROP CONSTRAINT [IF EXISTS] constraint
	//    | ALTER [COLUMN] column DROP DEFAULT
	//    | ALTER [COLUMN] column DROP NOT NULL
	// table_constraint = FOREIGN KEY columns
	//    REFERENCES [[database '.'] schema '.'] table [columns]
	//    [ON DELETE referential_action] [ON UPDATE referential_action]
	// referential_action = NO ACTION | RESTRICT | CASCADE | SET NULL | SET DEFAULT
	// columns = '(' column [',' ...] ')'
	var s stmt.AlterTable

	s.Table = p.parseTableName()

	for {
		switch p.expectReserved(types.ADD, types.DROP, types.ALTER) {
		case types.ADD:
			var cn types.Identifier
			if p.optionalReserved(types.CONSTRAINT) {
				cn = p.expectIdentifier("expected a constraint name")
			}

			p.expectReserved(types.FOREIGN)
			p.expectReserved(types.KEY)

			fk := p.parseForeignKey(cn)
			s.Actions = append(s.Actions, &stmt.AddForeignKey{*fk})
		case types.DROP:
			p.expectReserved(types.CONSTRAINT)

			var ifExists bool
			if p.optionalReserved(types.IF) {
				p.expectReserved(types.EXISTS)
				ifExists = true
			}

			s.Actions = append(s.Actions,
				&stmt.DropConstraint{
					Name:     p.expectIdentifier("expected a constraint name"),
					IfExists: ifExists,
				})
		case types.ALTER:
			p.optionalReserved(types.COLUMN)
			nam := p.expectIdentifier("expected a column name")
			p.expectReserved(types.DROP)

			var ct stmt.ConstraintType
			switch p.expectReserved(types.DEFAULT, types.NOT) {
			case types.DEFAULT:
				ct = stmt.DefaultConstraint
			case types.NOT:
				p.expectReserved(types.NULL)
				ct = stmt.NotNullConstraint
			}

			s.Actions = append(s.Actions,
				&stmt.DropConstraint{
					Column: nam,
					Type:   ct,
				})
		}

		if !p.maybeToken(token.Comma) {
			break
		}
	}

	return &s
}

func (p *Parser) parseCreateIndex(unique bool) stmt.Stmt {
	// CREATE [UNIQUE] INDEX [IF NOT EXISTS] index ON table
	//    [USING btree]
	//    '(' column [ASC | DESC] [, ...] ')'
	var s stmt.CreateIndex

	if p.optionalReserved(types.IF) {
		p.expectReserved(types.NOT)
		p.expectReserved(types.EXISTS)
		s.IfNotExists = true
	}
	s.Index = p.expectIdentifier("expected an index")
	p.expectReserved(types.ON)
	s.Table = p.parseTableName()

	if p.optionalReserved(types.USING) {
		if p.expectIdentifier("expected btree") != types.BTREE {
			p.error(fmt.Sprintf("expected btree, got %s", p.got()))
		}
	}

	s.Key = p.parseKey(unique)
	return &s
}

func (p *Parser) parseDelete() stmt.Stmt {
	// DELETE FROM [database '.'] table [WHERE expr]
	var s stmt.Delete
	s.Table = p.parseTableName()
	if p.optionalReserved(types.WHERE) {
		s.Where = p.parseExpr()
	}

	return &s
}

func (p *Parser) parseDropTable() stmt.Stmt {
	// DROP TABLE [IF EXISTS] [database '.' ] table [',' ...] [CASCADE | RESTRICT]
	var s stmt.DropTable
	if p.optionalReserved(types.IF) {
		p.expectReserved(types.EXISTS)
		s.IfExists = true
	}

	s.Tables = []types.TableName{p.parseTableName()}
	for p.maybeToken(token.Comma) {
		s.Tables = append(s.Tables, p.parseTableName())
	}

	if p.optionalReserved(types.CASCADE) {
		s.Cascade = true
	} else {
		p.optionalReserved(types.RESTRICT)
	}

	return &s
}

func (p *Parser) parseDropIndex() stmt.Stmt {
	// DROP INDEX [IF EXISTS] index ON table
	var s stmt.DropIndex
	if p.optionalReserved(types.IF) {
		p.expectReserved(types.EXISTS)
		s.IfExists = true
	}
	s.Index = p.expectIdentifier("expected an index")
	p.expectReserved(types.ON)
	s.Table = p.parseTableName()
	return &s
}

func (p *Parser) optionalSubquery() (stmt.Stmt, bool) {
	if p.optionalReserved(types.SELECT) {
		// ( select )
		return p.parseSelect(), true
	} else if p.optionalReserved(types.VALUES) {
		// ( values )
		return p.parseValues(), true
	} else if p.optionalReserved(types.SHOW) {
		// ( show )
		return p.parseShow(), true
	} else if p.optionalReserved(types.TABLE) {
		// ( TABLE [[database .] schema .] table )
		return &stmt.Select{
			From: &stmt.FromTableAlias{TableName: p.parseTableName()},
		}, true
	}
	return nil, false
}

func (p *Parser) ParseExpr() (e expr.Expr, err error) {
	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(runtime.Error); ok {
				panic(r)
			}
			err = r.(error)
			e = nil
		}
	}()

	e = p.parseExpr()
	return
}

/*
	func adjustPrecedence(e expr.Expr) expr.Expr {
		switch e := e.(type) {
		case *expr.Unary:
			e.Expr = adjustPrecedence(e.Expr)
			if e.Op == expr.NoOp {
				return e
			}

			// - {2 * 3}  --> {- 2} * 3
			if b, ok := e.Expr.(*expr.Binary); ok && b.Op.Precedence() < e.Op.Precedence() {
				e.Expr = b.Left
				b.Left = e
				return adjustPrecedence(b)
			}
		case *expr.Binary:
			e.Left = adjustPrecedence(e.Left)
			e.Right = adjustPrecedence(e.Right)

			// 1 * {2 + 3} --> {1 * 2} + 3
			if b, ok := e.Right.(*expr.Binary); ok && b.Op.Precedence() <= e.Op.Precedence() {
				e.Right = b.Left
				b.Left = e
				return adjustPrecedence(b)
			}

			// {1 + 2} * 3 --> 1 + {2 * 3}
			if b, ok := e.Left.(*expr.Binary); ok && b.Op.Precedence() < e.Op.Precedence() {
				e.Left = b.Right
				b.Right = e
				return adjustPrecedence(b)
			}
		case *expr.Call:
			for i, a := range e.Args {
				e.Args[i] = adjustPrecedence(a)
			}
		}

		return e
	}
*/
func (p *Parser) parseExpr() expr.Expr {
	return nil
	/* return adjustPrecedence(p.parseSubExpr()) */
}

/*
expr = literal
    | '-' expr
    | NOT expr
    | '(' expr | subquery ')'
    | expr op expr
    | expr IS NULL
    | expr IS NOT NULL
    | ref ['.' ref ...]
    | param
    | func '(' [expr [',' ...]] ')'
    | COUNT '(' '*' ')'
    | EXISTS '(' subquery ')'
    | expr IN '(' subquery ')'
    | expr NOT IN '(' subquery ')'
    | expr op ANY '(' subquery ')'
    | expr op SOME '(' subquery ')'
    | expr op ALL '(' subquery ')'
op = '+' '-' '*' '/' '%'
    | '=' '==' '!=' '<>' '<' '<=' '>' '>='
    | '<<' '>>' '&' '|'
    | AND | OR
subquery = select | values | show
*/

/* XXX
var binaryOps = map[rune]struct {
	op     expr.Op
	isBool bool
}{
	token.Ampersand:      {expr.BinaryAndOp, false},
	token.Bar:            {expr.BinaryOrOp, false},
	token.BarBar:         {expr.ConcatOp, false},
	token.Equal:          {expr.EqualOp, true},
	token.EqualEqual:     {expr.EqualOp, true},
	token.BangEqual:      {expr.NotEqualOp, true},
	token.Greater:        {expr.GreaterThanOp, true},
	token.GreaterEqual:   {expr.GreaterEqualOp, true},
	token.GreaterGreater: {expr.RShiftOp, false},
	token.Less:           {expr.LessThanOp, true},
	token.LessEqual:      {expr.LessEqualOp, true},
	token.LessGreater:    {expr.NotEqualOp, true},
	token.LessLess:       {expr.LShiftOp, false},
	token.Minus:          {expr.SubtractOp, false},
	token.Percent:        {expr.ModuloOp, false},
	token.Plus:           {expr.AddOp, false},
	token.Slash:          {expr.DivideOp, false},
	token.Star:           {expr.MultiplyOp, false},
}

func (p *Parser) optionalBinaryOp() (expr.Op, bool, bool) {
	r := p.scan()
	if bop, ok := binaryOps[r]; ok {
		return bop.op, true, bop.isBool
	} else if r == token.Reserved {
		switch p.sctx.Identifier {
		case types.AND:
			return expr.AndOp, true, true
		case types.OR:
			return expr.OrOp, true, true
		}
	}

	p.unscan()
	return 0, false, false
}

func (p *Parser) parseSubExpr() expr.Expr {
	var e expr.Expr
	r := p.scan()
	if r == token.Reserved {
		if p.sctx.Identifier == types.TRUE {
			e = expr.True()
		} else if p.sctx.Identifier == types.FALSE {
			e = expr.False()
		} else if p.sctx.Identifier == types.NULL {
			e = expr.Nil()
		} else if p.sctx.Identifier == types.NOT {
			e = &expr.Unary{Op: expr.NotOp, Expr: p.parseSubExpr()}
		} else if p.sctx.Identifier == types.EXISTS {
			// EXISTS ( subquery )
			e = expr.Subquery{Op: expr.Exists, Stmt: p.parseSubquery()}
		} else {
			p.error(fmt.Sprintf("unexpected identifier %s", p.sctx.Identifier))
		}
	} else if r == token.String {
		e = expr.StringLiteral(p.sctx.String)
	} else if r == token.Bytes {
		e = expr.BytesLiteral(p.sctx.Bytes)
	} else if r == token.Integer {
		e = expr.Int64Literal(p.sctx.Integer)
	} else if r == token.Float {
		e = expr.Float64Literal(p.sctx.Float)
	} else if r == token.Parameter {
		e = expr.Param{Num: int(p.sctx.Integer)}
	} else if r == token.Identifier {
		id := p.sctx.Identifier
		if p.maybeToken(token.LParen) {
			// func ( expr [,...] )
			c := &expr.Call{Name: id}
			if !p.maybeToken(token.RParen) {
				if id == types.COUNT && p.maybeToken(token.Star) {
					p.expectTokens(token.RParen)
					c.Name = types.COUNT_ALL
				} else {
					for {
						c.Args = append(c.Args, p.parseSubExpr())
						if p.maybeToken(token.RParen) {
							break
						}
						p.expectTokens(token.Comma)
					}
				}
			}
			e = c
		} else {
			// ref [. ref]
			ref := expr.Ref{p.sctx.Identifier}
			for p.maybeToken(token.Dot) {
				ref = append(ref, p.expectIdentifier("expected a reference"))
			}
			e = ref
		}
	} else if r == token.Minus {
		// - expr
		e = &expr.Unary{Op: expr.NegateOp, Expr: p.parseSubExpr()}
	} else if r == token.LParen {
		if s, ok := p.optionalSubquery(); ok {
			// ( subquery )
			e = expr.Subquery{Op: expr.Scalar, Stmt: s}
		} else {
			// ( expr )
			e = &expr.Unary{Op: expr.NoOp, Expr: p.parseSubExpr()}
		}
		if p.scan() != token.RParen {
			p.error(fmt.Sprintf("expected closing parenthesis, got %s", p.got()))
		}
	} else {
		p.error(fmt.Sprintf("expected an expression, got %s", p.got()))
	}

	op, ok, bop := p.optionalBinaryOp()
	if !ok {
		if p.optionalReserved(types.IN, types.NOT, types.IS) {
			switch p.sctx.Identifier {
			case types.IN:
				return expr.Subquery{Op: expr.Any, ExprOp: expr.EqualOp, Expr: e,
					Stmt: p.parseSubquery()}
			case types.NOT:
				if p.optionalReserved(types.IN) {
					return expr.Subquery{Op: expr.All, ExprOp: expr.NotEqualOp, Expr: e,
						Stmt: p.parseSubquery()}
				}
				p.unscan()
			case types.IS:
				var not bool
				if p.optionalReserved(types.NOT) {
					not = true
				}
				p.expectReserved(types.NULL)

				e = &expr.Call{Name: types.ID("is_null"), Args: []expr.Expr{e}}
				if not {
					return &expr.Unary{Op: expr.NotOp, Expr: e}
				}
				return e
			}
		}

		return e
	}

	if p.optionalReserved(types.ANY, types.SOME, types.ALL) {
		if !bop {
			p.error("expected boolean binary operator")
		}
		var subqueryOp expr.SubqueryOp
		if p.sctx.Identifier == types.ALL {
			subqueryOp = expr.All
		} else {
			subqueryOp = expr.Any
		}
		return expr.Subquery{Op: subqueryOp, ExprOp: op, Expr: e, Stmt: p.parseSubquery()}
	}

	return &expr.Binary{Op: op, Left: e, Right: p.parseSubExpr()}
}

func (p *Parser) parseSubquery() stmt.Stmt {
	p.expectTokens(token.LParen)
	s, ok := p.optionalSubquery()
	if !ok {
		p.error("expected a subquery")
	}
	p.expectTokens(token.RParen)
	return s
}
*/

func (p *Parser) parseInsert() stmt.Stmt {
	/*
		INSERT INTO [database '.'] table ['(' column [',' ...] ')']
			VALUES '(' (expr | DEFAULT) [',' ...] ')' [',' ...]
	*/

	var s stmt.InsertValues
	s.Table = p.parseTableName()

	if p.maybeToken(token.LParen) {
		for {
			nam := p.expectIdentifier("expected a column name")
			for _, c := range s.Columns {
				if c == nam {
					p.error(fmt.Sprintf("duplicate column name %s", nam))
				}
			}
			s.Columns = append(s.Columns, nam)
			r := p.expectTokens(token.Comma, token.RParen)
			if r == token.RParen {
				break
			}
		}
	}

	p.expectReserved(types.VALUES)

	for {
		var row []expr.Expr

		p.expectTokens(token.LParen)
		for {
			r := p.scan()
			if r == token.Reserved && p.sctx.Identifier == types.DEFAULT {
				row = append(row, nil)
			} else {
				p.unscan()
				row = append(row, p.parseExpr())
			}
			r = p.expectTokens(token.Comma, token.RParen)
			if r == token.RParen {
				break
			}
		}

		s.Rows = append(s.Rows, row)

		if !p.maybeToken(token.Comma) {
			break
		}
	}

	return &s
}

func (p *Parser) parseCopy() stmt.Stmt {
	/*
		COPY [[database '.'] schema '.'] table '(' column [',' ...] ')' FROM STDIN
			[DELIMITER delimiter]
	*/

	var s stmt.Copy
	s.Table = p.parseTableName()

	if p.maybeToken(token.LParen) {
		for {
			nam := p.expectIdentifier("expected a column name")
			for _, c := range s.Columns {
				if c == nam {
					p.error(fmt.Sprintf("duplicate column name %s", nam))
				}
			}
			s.Columns = append(s.Columns, nam)
			r := p.expectTokens(token.Comma, token.RParen)
			if r == token.RParen {
				break
			}
		}
	}

	p.expectReserved(types.FROM)
	if p.expectIdentifier("expected STDIN") != types.STDIN {
		p.error("expected STDIN")
	}

	s.Delimiter = '\t'
	if p.optionalReserved(types.DELIMITER) {
		if p.scan() != token.String || len(p.sctx.String) != 1 {
			p.error("expected a one character string")
		}
		s.Delimiter = rune(p.sctx.String[0])
	}

	// Must be last because the scanner will skip to the end of the line before returning
	// the reader.
	s.From, s.FromLine = p.scanner.RuneReader()

	return &s
}

func (p *Parser) parseValues() *stmt.Values {
	/*
	   values = VALUES '(' expr [',' ...] ')' [',' ...]
	*/

	var s stmt.Values
	for {
		var row []expr.Expr

		p.expectTokens(token.LParen)
		for {
			row = append(row, p.parseExpr())
			r := p.expectTokens(token.Comma, token.RParen)
			if r == token.RParen {
				break
			}
		}

		if s.Expressions != nil && len(s.Expressions[0]) != len(row) {
			p.error("values: all rows must have same number of columns")
		}
		s.Expressions = append(s.Expressions, row)

		if !p.maybeToken(token.Comma) {
			break
		}
	}

	return &s
}

/*
select =
    SELECT select-list
    [FROM from-item [',' ...]]
    [WHERE expr]
    [GROUP BY expr [',' ...]]
    [HAVING expr]
    [ORDER BY column [ASC | DESC] [',' ...]]
select-list = '*'
    | select-item [',' ...]
select-item = table '.' '*'
    | [table '.' ] column [[AS] column-alias]
    | expr [[AS] column-alias]
*/

func (p *Parser) parseSelect() *stmt.Select {
	var s stmt.Select
	if !p.maybeToken(token.Star) {
		for {
			t := p.scan()
			if t == token.Identifier {
				tbl := p.sctx.Identifier
				if p.maybeToken(token.Dot) {
					if p.maybeToken(token.Star) {
						// table '.' *
						s.Results = append(s.Results, stmt.TableResult{Table: tbl})

						if !p.maybeToken(token.Comma) {
							break
						}
						continue
					}
					p.unscan()
				}
			}
			p.unscan()

			// expr [[ AS ] column-alias]
			s.Results = append(s.Results, stmt.ExprResult{
				Expr:  p.parseExpr(),
				Alias: p.parseAlias(false),
			})

			if !p.maybeToken(token.Comma) {
				break
			}
		}
	}

	if p.optionalReserved(types.FROM) {
		s.From = p.parseFromList()
	}

	if p.optionalReserved(types.WHERE) {
		s.Where = p.parseExpr()
	}

	if p.optionalReserved(types.GROUP) {
		p.expectReserved(types.BY)

		for {
			s.GroupBy = append(s.GroupBy, p.parseExpr())
			if !p.maybeToken(token.Comma) {
				break
			}
		}
	}

	if p.optionalReserved(types.HAVING) {
		s.Having = p.parseExpr()
	}

	if p.optionalReserved(types.ORDER) {
		p.expectReserved(types.BY)

		/* XXX
		for {
			var by stmt.OrderBy
			by.Expr = expr.Ref{p.expectIdentifier("expected a column")}
			if p.optionalReserved(types.DESC) {
				by.Reverse = true
			} else {
				p.optionalReserved(types.ASC)
			}
			s.OrderBy = append(s.OrderBy, by)
			if !p.maybeToken(token.Comma) {
				break
			}
		}
		*/
	}

	return &s
}

/*
from-item = [[database '.'] schema '.'] table ['@' index] [[AS] alias]
    | '(' select | values | show ')' [AS] alias ['(' column-alias [',' ...] ')']
    | '(' from-item [',' ...] ')'
    | from-item join-type from-item [ON expr | USING '(' join-column [',' ...] ')']
join-type = [INNER] JOIN
    | LEFT [OUTER] JOIN
    | RIGHT [OUTER] JOIN
    | FULL [OUTER] JOIN
    | CROSS JOIN
*/

func (p *Parser) parseFromItem() stmt.FromItem {
	var fi stmt.FromItem
	if p.maybeToken(token.LParen) {
		if s, ok := p.optionalSubquery(); ok {
			// ( subquery )
			fi = p.parseFromStmt(s)
		} else {
			fi = p.parseFromList()
			p.expectTokens(token.RParen)
		}
	} else {
		fi = p.parseTableAlias()
	}

	jt := stmt.NoJoin
	if p.optionalReserved(types.JOIN) {
		jt = stmt.Join
	} else if p.optionalReserved(types.INNER) {
		p.expectReserved(types.JOIN)
		jt = stmt.Join
	} else if p.optionalReserved(types.LEFT) {
		p.optionalReserved(types.OUTER)
		jt = stmt.LeftJoin
		p.expectReserved(types.JOIN)
	} else if p.optionalReserved(types.RIGHT) {
		p.optionalReserved(types.OUTER)
		jt = stmt.RightJoin
		p.expectReserved(types.JOIN)
	} else if p.optionalReserved(types.FULL) {
		p.optionalReserved(types.OUTER)
		jt = stmt.FullJoin
		p.expectReserved(types.JOIN)
	} else if p.optionalReserved(types.CROSS) {
		p.expectReserved(types.JOIN)
		jt = stmt.CrossJoin
	}

	if jt == stmt.NoJoin {
		return fi
	}

	fj := stmt.FromJoin{Left: fi, Right: p.parseFromItem(), Type: jt}
	if p.optionalReserved(types.ON) {
		fj.On = p.parseExpr()
	} else if p.optionalReserved(types.USING) {
		p.expectTokens(token.LParen)
		for {
			nam := p.expectIdentifier("expected a column name")
			for _, c := range fj.Using {
				if c == nam {
					p.error(fmt.Sprintf("duplicate column %s", nam))
				}
			}
			fj.Using = append(fj.Using, nam)
			r := p.expectTokens(token.Comma, token.RParen)
			if r == token.RParen {
				break
			}
		}
	}

	if jt == stmt.Join || jt == stmt.LeftJoin || jt == stmt.RightJoin || jt == stmt.FullJoin {
		if (fj.On != nil && fj.Using != nil) || (fj.On == nil && fj.Using == nil) {
			p.error(fmt.Sprintf("%s must have one of ON or USING", jt))
		}
	}
	if jt == stmt.CrossJoin {
		if fj.On != nil || fj.Using != nil {
			p.error("CROSS JOIN may not have ON or USING")
		}
	}

	return fj
}

func (p *Parser) parseFromList() stmt.FromItem {
	fi := p.parseFromItem()
	for p.maybeToken(token.Comma) {
		fi = stmt.FromJoin{Left: fi, Right: p.parseFromItem(), Type: stmt.CrossJoin}
	}
	return fi
}

func (p *Parser) parseFromStmt(s stmt.Stmt) stmt.FromItem {
	p.expectTokens(token.RParen)
	a := p.parseAlias(true)
	return stmt.FromStmt{Stmt: s, Alias: a, ColumnAliases: p.parseColumnAliases()}
}

func (p *Parser) parseUpdate() stmt.Stmt {
	// UPDATE [database '.'] table SET column '=' (expr | DEFAULT) [',' ...] [WHERE expr]
	var s stmt.Update
	s.Table = p.parseTableName()
	p.expectReserved(types.SET)

	for {
		var cu stmt.ColumnUpdate
		cu.Column = p.expectIdentifier("expected a column name")
		p.expectTokens(token.Equal)
		r := p.scan()
		if r == token.Reserved && p.sctx.Identifier == types.DEFAULT {
			cu.Expr = nil
		} else {
			p.unscan()
			cu.Expr = p.parseExpr()
		}
		s.ColumnUpdates = append(s.ColumnUpdates, cu)
		if !p.maybeToken(token.Comma) {
			break
		}
	}

	if p.optionalReserved(types.WHERE) {
		s.Where = p.parseExpr()
	}

	return &s
}

func (p *Parser) parseSet() stmt.Stmt {
	return nil
	/* XXX
	// SET variable ( TO | '=' ) literal
	var s stmt.Set

	if p.optionalReserved(types.DATABASE) {
		s.Variable = types.DATABASE
	} else if p.optionalReserved(types.SCHEMA) {
		s.Variable = types.SCHEMA
	} else {
		s.Variable = p.expectIdentifier("expected a config variable")
	}
	if !p.maybeToken(token.Equal) {
		p.expectReserved(types.TO)
	}
	e := p.parseExpr()
	l, ok := e.(*expr.Literal)
	if !ok {
		p.error(fmt.Sprintf("expected a literal value, got %s", e.String()))
	}
	if sv, ok := l.Value.(types.StringValue); ok {
		s.Value = string(sv)
	} else {
		s.Value = l.Value.String()
	}

	return &s
	*/
}

func (p *Parser) parseShowFromTable() (types.TableName, expr.Expr) {
	return types.TableName{}, nil
}

/*
func (p *Parser) parseShowFromTable() (types.TableName, *expr.Binary) {
	tn := p.parseTableName()

	var schemaTest *expr.Binary
	if tn.Schema == 0 {
		schemaTest = &expr.Binary{
			Op:    expr.EqualOp,
			Left:  expr.Ref{types.ID("schema_name")},
			Right: expr.Subquery{Op: expr.Scalar, Stmt: &stmt.Show{Variable: types.SCHEMA}},
		}
	} else {
		schemaTest = &expr.Binary{
			Op:    expr.EqualOp,
			Left:  expr.Ref{types.ID("schema_name")},
			Right: expr.StringLiteral(tn.Schema.String()),
		}
	}

	return tn, schemaTest
}
*/

func (p *Parser) parseShow() stmt.Stmt {
	return nil
	/* XXX
	// SHOW COLUMNS FROM [[database '.'] schema '.'] table
	// SHOW CONFIG
	// SHOW CONSTRAINTS FROM [[database '.'] schema '.'] table
	// SHOW DATABASE
	// SHOW DATABASES
	// SHOW SCHEMA
	// SHOW SCHEMAS [FROM database]
	// SHOW TABLES [FROM [database '.'] schema]
	// SHOW flag

	t := p.scan()
	if t != token.Reserved && t != token.Identifier {
		p.error("expected COLUMNS, CONSTRAINTS, DATABASE, DATABASES, SCHEMA, SCHEMAS, TABLES, " +
			"or a config variable")
	}

	switch p.sctx.Identifier {
	case types.COLUMNS:
		p.expectReserved(types.FROM)
		tn, schemaTest := p.parseShowFromTable()

		return &stmt.Select{
			From: &stmt.FromTableAlias{
				TableName: types.TableName{
					Database: tn.Database,
					Schema:   types.METADATA,
					Table:    types.COLUMNS,
				},
			},
			Where: &expr.Binary{
				Op: expr.AndOp,
				Left: &expr.Binary{
					Op:    expr.EqualOp,
					Left:  expr.Ref{types.ID("table_name")},
					Right: expr.StringLiteral(tn.Table.String()),
				},
				Right: schemaTest,
			},
		}
	case types.CONFIG:
		return &stmt.Select{
			Results: []stmt.SelectResult{
				stmt.ExprResult{Expr: expr.Ref{types.ID("name")}},
				stmt.ExprResult{Expr: expr.Ref{types.ID("value")}},
				stmt.ExprResult{Expr: expr.Ref{types.ID("by")}},
			},
			From: &stmt.FromTableAlias{
				TableName: types.TableName{
					Database: types.SYSTEM,
					Schema:   types.INFO,
					Table:    types.CONFIG,
				},
			},
			Where: &expr.Binary{
				Op:    expr.EqualOp,
				Left:  expr.Ref{types.ID("hidden")},
				Right: expr.False(),
			},
		}
	case types.CONSTRAINTS:
		p.expectReserved(types.FROM)
		tn, schemaTest := p.parseShowFromTable()

		return &stmt.Select{
			From: &stmt.FromTableAlias{
				TableName: types.TableName{
					Database: tn.Database,
					Schema:   types.METADATA,
					Table:    types.CONSTRAINTS,
				},
			},
			Where: &expr.Binary{
				Op: expr.AndOp,
				Left: &expr.Binary{
					Op:    expr.EqualOp,
					Left:  expr.Ref{types.ID("table_name")},
					Right: expr.StringLiteral(tn.Table.String()),
				},
				Right: schemaTest,
			},
		}
	case types.DATABASES:
		return &stmt.Select{
			From: &stmt.FromTableAlias{
				TableName: types.TableName{
					Database: types.SYSTEM,
					Schema:   types.INFO,
					Table:    types.DATABASES,
				},
			},
		}
	case types.SCHEMAS:
		var db types.Identifier
		if p.optionalReserved(types.FROM) {
			db = p.expectIdentifier("expected a database")
		}
		return &stmt.Select{
			From: &stmt.FromTableAlias{
				TableName: types.TableName{
					Database: db,
					Schema:   types.METADATA,
					Table:    types.SCHEMAS,
				},
			},
		}
	case types.TABLES:
		var sn types.SchemaName
		var where *expr.Binary

		if p.optionalReserved(types.FROM) {
			sn = p.parseSchemaName()
			where = &expr.Binary{
				Op:    expr.EqualOp,
				Left:  expr.Ref{types.ID("schema_name")},
				Right: expr.StringLiteral(sn.Schema.String()),
			}
		} else {
			where = &expr.Binary{
				Op:    expr.EqualOp,
				Left:  expr.Ref{types.ID("schema_name")},
				Right: expr.Subquery{Op: expr.Scalar, Stmt: &stmt.Show{Variable: types.SCHEMA}},
			}
		}
		return &stmt.Select{
			From: &stmt.FromTableAlias{
				TableName: types.TableName{
					Database: sn.Database,
					Schema:   types.METADATA,
					Table:    types.TABLES,
				},
			},
			Where: where,
		}
	default:
		return &stmt.Show{Variable: p.sctx.Identifier}
	}
	*/
}

func (p *Parser) parseUse() stmt.Stmt {
	return nil
	/* XXX
	// USE database
	s := stmt.Set{Variable: types.DATABASE}

	e := p.parseExpr()
	l, ok := e.(*expr.Literal)
	if !ok {
		p.error(fmt.Sprintf("expected a literal value, got %s", e.String()))
	}
	if sv, ok := l.Value.(types.StringValue); ok {
		s.Value = string(sv)
	} else {
		s.Value = l.Value.String()
	}

	return &s
	*/
}

func (p *Parser) parseOptions() map[types.Identifier]string {
	options := map[types.Identifier]string{}
	for {
		if p.scan() != token.Identifier {
			p.unscan()
			break
		}

		opt := p.sctx.Identifier

		p.maybeToken(token.Equal)

		var val string
		switch p.scan() {
		case token.Identifier:
			val = p.sctx.Identifier.String()
		case token.String:
			val = p.sctx.String
		case token.Bytes:
			val = string(p.sctx.Bytes)
		case token.Integer:
			val = strconv.FormatInt(p.sctx.Integer, 10)
		case token.Float:
			val = strconv.FormatFloat(p.sctx.Float, 'g', -1, 64)
		default:
			p.error("expected a value")
		}

		options[opt] = val
	}
	if len(options) == 0 {
		p.error("expected options")
	}
	return options
}

func (p *Parser) parseCreateDatabase() stmt.Stmt {
	// CREATE DATABASE database
	//     [ WITH [ PATH [ '=' ] path ] ]
	var s stmt.CreateDatabase

	s.Database = p.expectIdentifier("expected a database")
	if p.optionalReserved(types.WITH) {
		s.Options = p.parseOptions()
	}
	return &s
}

func (p *Parser) parseDropDatabase() stmt.Stmt {
	// DROP DATABASE [IF EXISTS] database
	var s stmt.DropDatabase

	if p.optionalReserved(types.IF) {
		p.expectReserved(types.EXISTS)
		s.IfExists = true
	}

	s.Database = p.expectIdentifier("expected a database")
	return &s
}

func (p *Parser) parseCreateSchema() stmt.Stmt {
	// CREATE SCHEMA [database '.'] schema
	var s stmt.CreateSchema

	s.Schema = p.parseSchemaName()
	return &s
}

func (p *Parser) parseDropSchema() stmt.Stmt {
	// DROP SCHEMA [IF EXISTS] [database '.'] schema
	var s stmt.DropSchema

	if p.optionalReserved(types.IF) {
		p.expectReserved(types.EXISTS)
		s.IfExists = true
	}

	s.Schema = p.parseSchemaName()
	return &s
}

/* XXX
func (p *Parser) parseExplain() stmt.Stmt {
	// EXPLAIN [VERBOSE] select

	var s stmt.Explain
	s.Verbose = p.optionalReserved(types.VERBOSE)
	switch p.expectReserved(types.SELECT) {
	case types.SELECT:
		// SELECT ...
		s.Stmt = p.parseSelect()
	}

	return s
}

func (p *Parser) parsePrepare() stmt.Stmt {
	// PREPARE name AS (delete | insert | select | update | values)

	var s stmt.Prepare
	s.Name = p.expectIdentifier("expected a prepared statement")
	p.expectReserved(types.AS)
	switch p.expectReserved(types.DELETE, types.INSERT, types.SELECT, types.UPDATE, types.VALUES) {
	case types.DELETE:
		// DELETE FROM ...
		p.expectReserved(types.FROM)
		s.Stmt = p.parseDelete()
	case types.INSERT:
		// INSERT INTO ...
		p.expectReserved(types.INTO)
		s.Stmt = p.parseInsert()
	case types.SELECT:
		// SELECT ...
		s.Stmt = p.parseSelect()
	case types.UPDATE:
		// UPDATE ...
		s.Stmt = p.parseUpdate()
	case types.VALUES:
		// VALUES ...
		s.Stmt = p.parseValues()
	}

	return &s
}

func (p *Parser) parseExecute() stmt.Stmt {
	// EXECUTE name ['(' expr [',' ...] ')']

	var s stmt.Execute
	s.Name = p.expectIdentifier("expected a prepared statement")
	if p.maybeToken(token.LParen) {
		for {
			s.Params = append(s.Params, p.parseExpr())
			if p.maybeToken(token.RParen) {
				break
			}
			p.expectTokens(token.Comma)
		}
	}

	return &s
}
*/
