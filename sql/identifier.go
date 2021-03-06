package sql

import (
	"errors"
	"strings"
)

type Identifier int

const MaxIdentifier = 128

const (
	BIGINT Identifier = iota + 1
	BINARY
	BLOB
	BOOL
	BOOLEAN
	BTREE
	BYTEA
	BYTES
	CHAR
	CHARACTER
	COLUMNS
	CONFIG
	CONSTRAINTS
	COUNT
	COUNT_ALL
	DATABASES
	DESCRIPTION
	DOUBLE
	FLAGS
	FIELD
	INDEXES
	INFO
	INT
	INT2
	INT4
	INT8
	INTEGER
	METADATA
	PATH
	PRIMARY_QUOTED
	PRIVATE
	PUBLIC
	PRECISION
	REAL
	SCHEMAS
	SEQUENCES
	SMALLINT
	STDIN
	SYSTEM
	TABLES
	TEXT
	TREE
	VARBINARY
	VARCHAR
)

const (
	ACTION Identifier = -(iota + 1)
	ADD
	ALL
	ALTER
	AND
	ANY
	AS
	ASC
	BEGIN
	BY
	CASCADE
	CHECK
	COLUMN
	COMMIT
	CONSTRAINT
	COPY
	CREATE
	CROSS
	DATABASE
	DEFAULT
	DELETE
	DELIMITER
	DESC
	DETACH
	DROP
	EXECUTE
	EXISTS
	EXPLAIN
	FALSE
	FOREIGN
	FROM
	FULL
	GROUP
	HAVING
	IF
	IN
	INDEX
	INNER
	INSERT
	INTO
	IS
	JOIN
	KEY
	LEFT
	NO
	NOT
	NULL
	ON
	OR
	ORDER
	OUTER
	PREPARE
	PRIMARY
	REFERENCES
	RESTRICT
	RIGHT
	ROLLBACK
	SCHEMA
	SELECT
	SET
	SHOW
	SOME
	START
	TABLE
	TO
	TRANSACTION
	TRUE
	UNIQUE
	UPDATE
	USE
	USING
	VALUES
	VERBOSE
	WHERE
	WITH
)

var knownIdentifiers = map[string]Identifier{
	"btree":       BTREE,
	"columns":     COLUMNS,
	"config":      CONFIG,
	"constraints": CONSTRAINTS,
	"count":       COUNT,
	"count_all":   COUNT_ALL,
	"databases":   DATABASES,
	"description": DESCRIPTION,
	"field":       FIELD,
	"flags":       FLAGS,
	"indexes":     INDEXES,
	"info":        INFO,
	"metadata":    METADATA,
	"primary":     PRIMARY_QUOTED,
	"private":     PRIVATE,
	"public":      PUBLIC,
	"schemas":     SCHEMAS,
	"sequences":   SEQUENCES,
	"system":      SYSTEM,
	"tables":      TABLES,
	"tree":        TREE,
}

var knownKeywords = map[string]struct {
	id       Identifier
	reserved bool
}{
	"ACTION":      {ACTION, true},
	"ADD":         {ADD, true},
	"ALL":         {ALL, true},
	"ALTER":       {ALTER, true},
	"AND":         {AND, true},
	"ANY":         {ANY, true},
	"AS":          {AS, true},
	"ASC":         {ASC, true},
	"BEGIN":       {BEGIN, true},
	"BY":          {BY, true},
	"BIGINT":      {BIGINT, false},
	"BINARY":      {BINARY, false},
	"BLOB":        {BLOB, false},
	"BOOL":        {BOOL, false},
	"BOOLEAN":     {BOOLEAN, false},
	"BYTEA":       {BYTEA, false},
	"BYTES":       {BYTES, false},
	"CASCADE":     {CASCADE, true},
	"CHAR":        {CHAR, false},
	"CHARACTER":   {CHARACTER, false},
	"CHECK":       {CHECK, true},
	"COLUMN":      {COLUMN, true},
	"COMMIT":      {COMMIT, true},
	"CONSTRAINT":  {CONSTRAINT, true},
	"COPY":        {COPY, true},
	"CREATE":      {CREATE, true},
	"CROSS":       {CROSS, true},
	"DATABASE":    {DATABASE, true},
	"DEFAULT":     {DEFAULT, true},
	"DELETE":      {DELETE, true},
	"DELIMITER":   {DELIMITER, true},
	"DESC":        {DESC, true},
	"DETACH":      {DETACH, true},
	"DOUBLE":      {DOUBLE, false},
	"DROP":        {DROP, true},
	"EXECUTE":     {EXECUTE, true},
	"EXISTS":      {EXISTS, true},
	"EXPLAIN":     {EXPLAIN, true},
	"FALSE":       {FALSE, true},
	"FOREIGN":     {FOREIGN, true},
	"FROM":        {FROM, true},
	"FULL":        {FULL, true},
	"GROUP":       {GROUP, true},
	"HAVING":      {HAVING, true},
	"IF":          {IF, true},
	"IN":          {IN, true},
	"INDEX":       {INDEX, true},
	"INNER":       {INNER, true},
	"INSERT":      {INSERT, true},
	"INT":         {INT, false},
	"INT2":        {INT2, false},
	"INT4":        {INT4, false},
	"INT8":        {INT8, false},
	"INTEGER":     {INTEGER, false},
	"INTO":        {INTO, true},
	"IS":          {IS, true},
	"JOIN":        {JOIN, true},
	"KEY":         {KEY, true},
	"LEFT":        {LEFT, true},
	"NO":          {NO, true},
	"NOT":         {NOT, true},
	"NULL":        {NULL, true},
	"ON":          {ON, true},
	"OR":          {OR, true},
	"ORDER":       {ORDER, true},
	"OUTER":       {OUTER, true},
	"PATH":        {PATH, false},
	"PRECISION":   {PRECISION, false},
	"PREPARE":     {PREPARE, true},
	"PRIMARY":     {PRIMARY, true},
	"REAL":        {REAL, false},
	"RESTRICT":    {RESTRICT, true},
	"REFERENCES":  {REFERENCES, true},
	"RIGHT":       {RIGHT, true},
	"ROLLBACK":    {ROLLBACK, true},
	"SCHEMA":      {SCHEMA, true},
	"SELECT":      {SELECT, true},
	"SET":         {SET, true},
	"SHOW":        {SHOW, true},
	"SMALLINT":    {SMALLINT, false},
	"SOME":        {SOME, true},
	"STDIN":       {STDIN, false},
	"START":       {START, true},
	"TABLE":       {TABLE, true},
	"TEXT":        {TEXT, false},
	"TO":          {TO, true},
	"TRANSACTION": {TRANSACTION, true},
	"TRUE":        {TRUE, true},
	"UNIQUE":      {UNIQUE, true},
	"UPDATE":      {UPDATE, true},
	"USE":         {USE, true},
	"USING":       {USING, true},
	"VALUES":      {VALUES, true},
	"VARBINARY":   {VARBINARY, false},
	"VARCHAR":     {VARCHAR, false},
	"VERBOSE":     {VERBOSE, true},
	"WHERE":       {WHERE, true},
	"WITH":        {WITH, true},
}

var (
	lastIdentifier = Identifier(0)
	identifiers    = map[string]Identifier{
		"": Identifier(0),
	}

	keywords = map[string]Identifier{}
	Names    = map[Identifier]string{
		Identifier(0): "",
	}
)

func ID(s string) Identifier {
	if len(s) > MaxIdentifier {
		s = s[:MaxIdentifier]
	}

	s = strings.ToLower(s)
	if id, found := identifiers[s]; found {
		return id
	}
	lastIdentifier += 1
	identifiers[s] = lastIdentifier
	Names[lastIdentifier] = s
	return lastIdentifier
}

func UnquotedID(s string) Identifier {
	if len(s) > MaxIdentifier {
		s = s[:MaxIdentifier]
	}

	if id, found := keywords[strings.ToUpper(s)]; found {
		return id
	}

	s = strings.ToLower(s)
	if id, found := identifiers[s]; found {
		return id
	}
	lastIdentifier += 1
	identifiers[s] = lastIdentifier
	Names[lastIdentifier] = s
	return lastIdentifier
}

func QuotedID(s string) Identifier {
	if len(s) > MaxIdentifier {
		s = s[:MaxIdentifier]
	}

	if id, found := identifiers[s]; found {
		return id
	}
	lastIdentifier += 1
	identifiers[s] = lastIdentifier
	Names[lastIdentifier] = s
	return lastIdentifier
}

func (id Identifier) String() string {
	return Names[id]
}

func (id Identifier) IsReserved() bool {
	if id < 0 {
		return true
	}
	return false
}

func (id *Identifier) GobDecode(val []byte) error {
	if len(val) == 0 || val[0] > 1 {
		return errors.New("unable to decode identifier")
	}
	if val[0] == 0 {
		*id = QuotedID(string(val[1:]))
	} else {
		*id = UnquotedID(string(val[1:]))
	}
	return nil
}

func (id Identifier) GobEncode() ([]byte, error) {
	if id.IsReserved() {
		return append([]byte{1}, []byte(id.String())...), nil
	}
	return append([]byte{0}, []byte(id.String())...), nil
}

func init() {
	for s, id := range knownIdentifiers {
		identifiers[strings.ToLower(s)] = id
		Names[id] = s
		if id > lastIdentifier {
			lastIdentifier = id
		}
	}
	for s, n := range knownKeywords {
		keywords[s] = n.id
		Names[n.id] = s
		if n.id > lastIdentifier {
			lastIdentifier = n.id
		}
	}
}
