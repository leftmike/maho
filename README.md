# Maho
Maho is a partial implementation of a basic SQL server.

## Goals (Motivations)
* Learn [Go](https://golang.org/) and SQL.
* Mostly [PostgreSQL](https://www.postgresql.org/) compatible; see [sqltest](https://github.com/leftmike/sqltest) for SQL compatibility tests.

## Features
* Parse and execute SQL statements.
* Somewhat PostgreSQL protocol compatible; use psql to interact with maho.

## Remote Access

To run an ssh server, maho needs a ssh host key; by default it uses `id_rsa` in the current
directory. Generate it if necessary.

```
ssh-keygen -t rsa -f id_rsa
```

Authorization of remote clients is done using an `authorized_keys` file and / or a list of
usernames and passwords. The list of usernames and passwords are specified in the config file;
the default is `maho.hcl` in the current directory.

```
// maho config
database = maho
engine = basic
accounts = [
    {user: "michael", password: "password"}
    {user: "test", password: "secret"}
    {
        user: setup
        password: default
    }
]
```

Run maho: `maho -ssh=1`. And then in another terminal, connect using ssh:
`ssh -p 8241 test@localhost`; using the config above, the password will be `secret`.

## Supported SQL
```
ALTER TABLE table action [',' ...]
action =
      ADD [CONSTRAINT constraint] table_constraint
    | DROP CONSTRAINT [IF EXISTS] constraint
    | ALTER [COLUMN] column DROP DEFAULT
    | ALTER [COLUMN] column DROP NOT NULL
table_constraint = FOREIGN KEY columns REFERENCES [[database '.'] schema '.'] table [columns]
    [ON DELETE referential_action] [ON UPDATE referential_action]
referential_action = NO ACTION | RESTRICT | CASCADE | SET NULL | SET DEFAULT
columns = '(' column [',' ...] ')'
```

```
BEGIN
```

```
COMMIT
```

```
COPY [[database '.'] schema '.'] table '(' column [',' ...] ')' FROM STDIN
    [DELIMITER delimiter]
```

```
CREATE DATABASE database
```

```
CREATE [UNIQUE] INDEX [IF NOT EXISTS] index ON table
    [USING btree]
    '(' column [ASC | DESC] [, ...] ')'
```

```
CREATE SCHEMA [database '.'] schema
```

```
CREATE TABLE [IF NOT EXISTS] [[database '.'] schema '.'] table
    '('
        ( column data_type [[CONSTRAINT constraint] column_constraint] ...
        | [CONSTRAINT constraint] table_constraint ) [',' ...]
    ')'
table_constraint =
      PRIMARY KEY key_columns
    | UNIQUE key_columns
    | CHECK '(' expr ')'
    | FOREIGN KEY columns REFERENCES [[database '.'] schema '.'] table [columns]
      [ON DELETE referential_action] [ON UPDATE referential_action]
key_columns = '(' column [ASC | DESC] [',' ...] ')'
columns = '(' column [',' ...] ')'
column_constraint =
      DEFAULT expr
    | NOT NULL
    | PRIMARY KEY
    | UNIQUE
    | CHECK '(' expr ')'
    | REFERENCES [[database '.'] schema '.'] table ['(' column ')']
      [ON DELETE referential_action] [ON UPDATE referential_action]
referential_action = NO ACTION | RESTRICT | CASCADE | SET NULL | SET DEFAULT
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
```

```
DELETE FROM [[database '.'] schema '.'] table [WHERE expr]
```

```
DROP DATABASE [IF EXISTS] database
```

```
DROP INDEX [IF EXISTS] index ON table
```

```
DROP SCHEMA [IF EXISTS] [database '.'] schema
```

```
DROP TABLE [IF EXISTS] [[database '.'] schema '.'] table [',' ...] [CASCADE | RESTRICT]
```

```
EXECUTE name ['(' expr [',' ...] ')']
```

```
EXPLAIN [VERBOSE] select
```

```
INSERT INTO [[database '.'] schema '.'] table ['(' column [',' ...] ')']
	VALUES '(' (expr | DEFAULT) [',' ...] ')' [',' ...]
```

```
PREPARE name AS (delete | insert | select | update | values)
```

```
ROLLBACK
```

```
SELECT select-list
    [FROM from-item [',' ...]]
    [WHERE expr]
    [GROUP BY expr [',' ...]]
    [HAVING expr]
    [ORDER BY column [ASC | DESC] [',' ...]]
select-list =
      '*'
    | select-item [',' ...]
select-item =
      table '.' '*'
    | [table '.' ] column [[AS] column-alias]
    | expr [[AS] column-alias]
from-item =
      [[database '.'] schema '.'] table ['@' index] [[AS] alias]
    | '(' select | values | show ')' [AS] alias ['(' column-alias [',' ...] ')']
    | '(' from-item [',' ...] ')'
    | from-item join-type from-item [ON expr | USING '(' join-column [',' ...] ')']
join-type =
      [INNER] JOIN
    | LEFT [OUTER] JOIN
    | RIGHT [OUTER] JOIN
    | FULL [OUTER] JOIN
    | CROSS JOIN
```

```
SET DATABASE (TO | '=') database
SET SCHEMA (TO | '=') schema
SET flag (TO | '=') value
```

```
SHOW COLUMNS FROM [[database '.'] schema '.'] table
SHOW CONFIG
SHOW CONSTRAINTS FROM [[database '.'] schema '.'] table
SHOW DATABASE
SHOW DATABASES
SHOW FLAGS
SHOW SCHEMA
SHOW SCHEMAS [FROM database]
SHOW TABLES [FROM [database '.'] schema]
SHOW flag
```

```
START TRANSACTION
```

```
UPDATE [[database '.'] schema '.'] table SET column '=' (expr | DEFAULT) [',' ...] [WHERE expr]
```

```
USE database
```

```
VALUES '(' expr [',' ...] ')' [',' ...]
```

```
expr =
      literal
    | '-' expr
    | NOT expr
    | '(' expr | subquery ')'
    | expr op expr
    | expr IS NULL
    | expr IS NOT NULL
    | ref ['.' ref ...]
    | param
    | func '(' [expr [',' ...]] ')'
    | EXISTS '(' subquery ')'
    | expr IN '(' subquery ')'
    | expr NOT IN '(' subquery ')'
    | expr op ANY '(' subquery ')'
    | expr op SOME '(' subquery ')'
    | expr op ALL '(' subquery ')'
op =
      '+' '-' '*' '/' '%'
    | '=' '==' '!=' '<>' '<' '<=' '>' '>='
    | '<<' '>>' '&' '|'
    | AND | OR
subquery = select | values | show
```

Scalar Functions:
* `abs(<number>)`
* `concat(<arg1>, <arg2>, ...)`

Aggregate Functions:
* `avg(<number>)`
* `count(<arg>)` or `count(*)`
* `max(<number>)`
* `min(<number>)`
* `sum(<number>)`

String Literals:

Maho accepts the same string contants (`' ... '`) and escaped string constants
(`e' ... '` or `E' ... '`) as
[PostgreSQL](https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-CONSTANTS).

Bytes Literals:

Maho accepts `x'<hex-digit> ...'` and `X'<hex-digit> ...'` for bytes constants. In addition,
`b' ... '` works like `e' ... '` escaped string contants, but is an escaped bytes constant.
