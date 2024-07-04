package types

import (
	"errors"
	"strings"
	"sync"
)

type Identifier int

const (
	MaxIdentifier = 128
)

// Known identifiers and keywords
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

// Reserved keywords
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

var (
	identifiers = map[string]Identifier{
		"":            Identifier(0),
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

	keywords = map[string]Identifier{
		"ACTION":      ACTION,
		"ADD":         ADD,
		"ALL":         ALL,
		"ALTER":       ALTER,
		"AND":         AND,
		"ANY":         ANY,
		"AS":          AS,
		"ASC":         ASC,
		"BEGIN":       BEGIN,
		"BY":          BY,
		"BIGINT":      BIGINT,
		"BINARY":      BINARY,
		"BLOB":        BLOB,
		"BOOL":        BOOL,
		"BOOLEAN":     BOOLEAN,
		"BYTEA":       BYTEA,
		"BYTES":       BYTES,
		"CASCADE":     CASCADE,
		"CHAR":        CHAR,
		"CHARACTER":   CHARACTER,
		"CHECK":       CHECK,
		"COLUMN":      COLUMN,
		"COMMIT":      COMMIT,
		"CONSTRAINT":  CONSTRAINT,
		"COPY":        COPY,
		"CREATE":      CREATE,
		"CROSS":       CROSS,
		"DATABASE":    DATABASE,
		"DEFAULT":     DEFAULT,
		"DELETE":      DELETE,
		"DELIMITER":   DELIMITER,
		"DESC":        DESC,
		"DETACH":      DETACH,
		"DOUBLE":      DOUBLE,
		"DROP":        DROP,
		"EXECUTE":     EXECUTE,
		"EXISTS":      EXISTS,
		"EXPLAIN":     EXPLAIN,
		"FALSE":       FALSE,
		"FOREIGN":     FOREIGN,
		"FROM":        FROM,
		"FULL":        FULL,
		"GROUP":       GROUP,
		"HAVING":      HAVING,
		"IF":          IF,
		"IN":          IN,
		"INDEX":       INDEX,
		"INNER":       INNER,
		"INSERT":      INSERT,
		"INT":         INT,
		"INT2":        INT2,
		"INT4":        INT4,
		"INT8":        INT8,
		"INTEGER":     INTEGER,
		"INTO":        INTO,
		"IS":          IS,
		"JOIN":        JOIN,
		"KEY":         KEY,
		"LEFT":        LEFT,
		"NO":          NO,
		"NOT":         NOT,
		"NULL":        NULL,
		"ON":          ON,
		"OR":          OR,
		"ORDER":       ORDER,
		"OUTER":       OUTER,
		"PATH":        PATH,
		"PRECISION":   PRECISION,
		"PREPARE":     PREPARE,
		"PRIMARY":     PRIMARY,
		"REAL":        REAL,
		"RESTRICT":    RESTRICT,
		"REFERENCES":  REFERENCES,
		"RIGHT":       RIGHT,
		"ROLLBACK":    ROLLBACK,
		"SCHEMA":      SCHEMA,
		"SELECT":      SELECT,
		"SET":         SET,
		"SHOW":        SHOW,
		"SMALLINT":    SMALLINT,
		"SOME":        SOME,
		"STDIN":       STDIN,
		"START":       START,
		"TABLE":       TABLE,
		"TEXT":        TEXT,
		"TO":          TO,
		"TRANSACTION": TRANSACTION,
		"TRUE":        TRUE,
		"UNIQUE":      UNIQUE,
		"UPDATE":      UPDATE,
		"USE":         USE,
		"USING":       USING,
		"VALUES":      VALUES,
		"VARBINARY":   VARBINARY,
		"VARCHAR":     VARCHAR,
		"VERBOSE":     VERBOSE,
		"WHERE":       WHERE,
		"WITH":        WITH,
	}

	names          = map[Identifier]string{}
	lastIdentifier Identifier
	mutex          sync.RWMutex
)

func lookupID(s string, quoted bool, create bool) (Identifier, bool) {
	if !quoted {
		if id, ok := keywords[strings.ToUpper(s)]; ok {
			return id, true
		}

		s = strings.ToLower(s)
	}

	if id, ok := identifiers[s]; ok {
		return id, true
	}

	if create {
		lastIdentifier += 1
		identifiers[s] = lastIdentifier
		names[lastIdentifier] = s
		return lastIdentifier, true
	}

	return 0, false
}

func ID(s string, quoted bool) Identifier {
	if len(s) > MaxIdentifier {
		s = s[:MaxIdentifier]
	}

	mutex.RLock()
	id, ok := lookupID(s, quoted, false)
	mutex.RUnlock()
	if ok {
		return id
	}

	// Check again -- another thread might have created it between when the RUnlock and the Lock.

	mutex.Lock()
	defer mutex.Unlock()

	id, _ = lookupID(s, quoted, true)
	return id
}

func (id Identifier) String() string {
	return names[id]
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
		*id = ID(string(val[1:]), true)
	} else {
		*id = ID(string(val[1:]), false)
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
	for s, id := range identifiers {
		names[id] = s
		if id > lastIdentifier {
			lastIdentifier = id
		}
	}

	for s, id := range keywords {
		names[id] = s
		if id > lastIdentifier {
			lastIdentifier = id
		}
	}
}
