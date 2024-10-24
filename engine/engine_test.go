package engine_test

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"testing"

	"github.com/leftmike/maho/engine"
	"github.com/leftmike/maho/storage"
	"github.com/leftmike/maho/storage/basic"
	"github.com/leftmike/maho/types"
)

func TestTableType(t *testing.T) {
	tableTypes := []engine.TableType{
		{},
		{
			Version: 123,
			ColumnNames: []types.Identifier{
				types.ID("col1", false),
				types.ID("col2", false),
				types.ID("col3", false),
				types.ID("col4", false),
				types.ID("col5", false),
				types.ID("col6", false),
			},
			ColumnTypes: []types.ColumnType{
				types.IdColType,
				types.Int32ColType,
				types.NullInt64ColType,
				types.BoolColType,
				types.StringColType,
				types.NullStringColType,
			},
			Key: []types.ColumnKey{
				types.MakeColumnKey(0, false),
				types.MakeColumnKey(2, true),
				types.MakeColumnKey(5, false),
			},
		},
	}

	for _, tt := range tableTypes {
		buf, err := tt.Encode()
		if err != nil {
			t.Errorf("Encode(%#v) failed with %s", &tt, err)
		}

		rtt, err := engine.DecodeTableType(buf)
		if err != nil {
			t.Errorf("DecodeTableType(%#v) failed with %s", &tt, err)
		} else if !reflect.DeepEqual(&tt, rtt) {
			t.Errorf("DecodeTableType(Encode(%#v)) got %#v", &tt, rtt)
		}
	}
}

func newEngine(t *testing.T) engine.Engine {
	t.Helper()

	s := t.TempDir()
	store, err := basic.NewStore(s)
	if err != nil {
		t.Fatalf("NewStore(%s) failed with %s", s, err)
	}
	err = engine.Init(store)
	if err != nil {
		t.Fatalf("Init() failed with %s", err)
	}

	return engine.NewEngine(store)
}

type createDatabase struct {
	dn   types.Identifier
	opts storage.OptionsMap
	fail bool
}

type dropDatabase struct {
	dn       types.Identifier
	ifExists bool
	fail     bool
}

type listDatabases struct {
	databases []types.Identifier
}

func TestDatabase(t *testing.T) {
	cases := []interface{}{
		createDatabase{
			dn: types.ID("db", false),
		},
		createDatabase{
			dn:   types.ID("db", false),
			fail: true,
		},
		listDatabases{
			databases: []types.Identifier{
				types.SYSTEM,
				types.MAHO,
				types.ID("db", false),
			},
		},
		createDatabase{
			dn: types.ID("db2", false),
		},
		listDatabases{
			databases: []types.Identifier{
				types.SYSTEM,
				types.MAHO,
				types.ID("db", false),
				types.ID("db2", false),
			},
		},
		createDatabase{
			dn: types.ID("db3", false),
		},
		listDatabases{
			databases: []types.Identifier{
				types.SYSTEM,
				types.MAHO,
				types.ID("db", false),
				types.ID("db2", false),
				types.ID("db3", false),
			},
		},
		dropDatabase{
			dn: types.ID("db2", false),
		},
		listDatabases{
			databases: []types.Identifier{
				types.SYSTEM,
				types.MAHO,
				types.ID("db", false),
				types.ID("db3", false),
			},
		},
		dropDatabase{
			dn:   types.ID("db2", false),
			fail: true,
		},
		listDatabases{
			databases: []types.Identifier{
				types.SYSTEM,
				types.MAHO,
				types.ID("db", false),
				types.ID("db3", false),
			},
		},
		dropDatabase{
			dn:       types.ID("db2", false),
			ifExists: true,
		},
		listDatabases{
			databases: []types.Identifier{
				types.SYSTEM,
				types.MAHO,
				types.ID("db", false),
				types.ID("db3", false),
			},
		},
	}

	eng := newEngine(t)
	for _, c := range cases {
		switch c := c.(type) {
		case createDatabase:
			err := eng.CreateDatabase(c.dn, c.opts)
			if c.fail {
				if err == nil {
					t.Errorf("CreateDatabase(%s) did not fail", c.dn)
				}
			} else if err != nil {
				t.Errorf("CreateDatabase(%s) failed with %s", c.dn, err)
			}
		case dropDatabase:
			err := eng.DropDatabase(c.dn, c.ifExists)
			if c.fail {
				if err == nil {
					t.Errorf("DropDatabase(%s, %v) did not fail", c.dn, c.ifExists)
				}
			} else if err != nil {
				t.Errorf("DropDatabase(%s, %v) failed with %s", c.dn, c.ifExists, err)
			}
		case listDatabases:
			databases, err := eng.ListDatabases()
			if err != nil {
				t.Errorf("ListDatabases() failed with %s", err)
			} else {
				slices.Sort(databases)
				slices.Sort(c.databases)
				if !reflect.DeepEqual(databases, c.databases) {
					t.Errorf("ListDatabases() got %v want %v", databases, c.databases)
				}
			}
		default:
			panic(fmt.Sprintf("unexpected case: %T %#v", c, c))
		}
	}
}

func TestSchema(t *testing.T) {
	eng := newEngine(t)

	testEngine(t, eng.Begin(), []interface{}{
		createSchema{
			sn: types.SchemaName{
				Database: types.MAHO,
				Schema:   types.ID("test", false),
			},
		},
		commit{},
	})

	testEngine(t, eng.Begin(), []interface{}{
		createSchema{
			sn: types.SchemaName{
				Database: types.MAHO,
				Schema:   types.ID("test", false),
			},
			fail: true,
		},
		rollback{},
	})

	testEngine(t, eng.Begin(), []interface{}{
		createSchema{
			sn: types.SchemaName{
				Database: types.ID("not_a_db", false),
				Schema:   types.ID("test", false),
			},
			fail: true,
		},
		rollback{},
	})

	testEngine(t, eng.Begin(), []interface{}{
		createSchema{
			sn: types.SchemaName{
				Database: types.MAHO,
				Schema:   types.ID("test2", false),
			},
		},
		createSchema{
			sn: types.SchemaName{
				Database: types.MAHO,
				Schema:   types.ID("test3", false),
			},
		},
		listSchemas{
			dn: types.MAHO,
			schemas: []types.Identifier{
				types.ID("public", false),
				types.ID("test", false),
				types.ID("test2", false),
				types.ID("test3", false),
			},
		},
		commit{},
	})

	testEngine(t, eng.Begin(), []interface{}{
		createSchema{
			sn: types.SchemaName{
				Database: types.MAHO,
				Schema:   types.ID("test4", false),
			},
		},
		listSchemas{
			dn: types.MAHO,
			schemas: []types.Identifier{
				types.ID("public", false),
				types.ID("test", false),
				types.ID("test2", false),
				types.ID("test3", false),
				types.ID("test4", false),
			},
		},
		rollback{},
	})

	testEngine(t, eng.Begin(), []interface{}{
		listSchemas{
			dn: types.MAHO,
			schemas: []types.Identifier{
				types.ID("public", false),
				types.ID("test", false),
				types.ID("test2", false),
				types.ID("test3", false),
			},
		},
		rollback{},
	})

	testEngine(t, eng.Begin(), []interface{}{
		listSchemas{
			dn:   types.ID("not_a_db", false),
			fail: true,
		},
		rollback{},
	})

	testEngine(t, eng.Begin(), []interface{}{
		listSchemas{
			dn: types.MAHO,
			schemas: []types.Identifier{
				types.ID("public", false),
				types.ID("test", false),
				types.ID("test2", false),
				types.ID("test3", false),
			},
		},
		dropSchema{
			sn: types.SchemaName{
				Database: types.MAHO,
				Schema:   types.ID("test2", false),
			},
		},
		listSchemas{
			dn: types.MAHO,
			schemas: []types.Identifier{
				types.ID("public", false),
				types.ID("test", false),
				types.ID("test3", false),
			},
		},
		commit{},
	})

	testEngine(t, eng.Begin(), []interface{}{
		listSchemas{
			dn: types.MAHO,
			schemas: []types.Identifier{
				types.ID("public", false),
				types.ID("test", false),
				types.ID("test3", false),
			},
		},
		dropSchema{
			sn: types.SchemaName{
				Database: types.MAHO,
				Schema:   types.ID("test", false),
			},
		},
		listSchemas{
			dn: types.MAHO,
			schemas: []types.Identifier{
				types.ID("public", false),
				types.ID("test3", false),
			},
		},
		rollback{},
	})

	testEngine(t, eng.Begin(), []interface{}{
		listSchemas{
			dn: types.MAHO,
			schemas: []types.Identifier{
				types.ID("public", false),
				types.ID("test", false),
				types.ID("test3", false),
			},
		},
		dropSchema{
			sn: types.SchemaName{
				Database: types.MAHO,
				Schema:   types.ID("test2", false),
			},
			fail: true,
		},
		rollback{},
	})

	testEngine(t, eng.Begin(), []interface{}{
		listSchemas{
			dn: types.MAHO,
			schemas: []types.Identifier{
				types.ID("public", false),
				types.ID("test", false),
				types.ID("test3", false),
			},
		},
		dropSchema{
			sn: types.SchemaName{
				Database: types.MAHO,
				Schema:   types.ID("test2", false),
			},
			ifExists: true,
		},
		listSchemas{
			dn: types.MAHO,
			schemas: []types.Identifier{
				types.ID("public", false),
				types.ID("test", false),
				types.ID("test3", false),
			},
		},
		rollback{},
	})
}

func TestTable(t *testing.T) {
	eng := newEngine(t)

	colNames1 := []types.Identifier{}
	colTypes1 := []types.ColumnType{}
	primary1 := []types.ColumnKey{}

	testEngine(t, eng.Begin(), []interface{}{
		createTable{
			tn: types.TableName{
				Database: types.MAHO,
				Schema:   types.PUBLIC,
				Table:    types.ID("test", false),
			},
			colNames: colNames1,
			colTypes: colTypes1,
			primary:  primary1,
		},
		commit{},
	})

	// XXX: test CreateTable and OpenTable
}

type createSchema struct {
	sn   types.SchemaName
	fail bool
}

type dropSchema struct {
	sn       types.SchemaName
	ifExists bool
	fail     bool
}

type listSchemas struct {
	dn      types.Identifier
	schemas []types.Identifier
	fail    bool
}

type openTable struct {
	tn       types.TableName
	colNames []types.Identifier
	colTypes []types.ColumnType
	primary  []types.ColumnKey
	tid      storage.TableId
	fail     bool
}

type createTable struct {
	tn       types.TableName
	colNames []types.Identifier
	colTypes []types.ColumnType
	primary  []types.ColumnKey
	fail     bool
}

func testEngine(t *testing.T, tx engine.Transaction, cases []interface{}) {
	t.Helper()

	ctx := context.Background()
	for _, c := range cases {
		switch c := c.(type) {
		case createSchema:
			err := tx.CreateSchema(ctx, c.sn)
			if c.fail {
				if err == nil {
					t.Errorf("CreateSchema(%s) did not fail", c.sn)
				}
			} else if err != nil {
				t.Errorf("CreateSchema(%s) failed with %s", c.sn, err)
			}
		case dropSchema:
			err := tx.DropSchema(ctx, c.sn, c.ifExists)
			if c.fail {
				if err == nil {
					t.Errorf("DropSchema(%s, %v) did not fail", c.sn, c.ifExists)
				}
			} else if err != nil {
				t.Errorf("DropSchema(%s, %v) failed with %s", c.sn, c.ifExists, err)
			}
		case listSchemas:
			schemas, err := tx.ListSchemas(ctx, c.dn)
			if c.fail {
				if err == nil {
					t.Errorf("ListSchemas(%s) did not fail", c.dn)
				}
			} else if err != nil {
				t.Errorf("ListSchemas(%s) failed with %s", c.dn, err)
			} else {
				slices.Sort(schemas)
				slices.Sort(c.schemas)
				if !reflect.DeepEqual(schemas, c.schemas) {
					t.Errorf("ListSchemas(%s) got %v want %v", c.dn, schemas, c.schemas)
				}
			}
		case openTable:
			tbl, err := tx.OpenTable(ctx, c.tn)
			if c.fail {
				if err == nil {
					t.Errorf("OpenTable(%s) did not fail", c.tn)
				}
			} else if err != nil {
				t.Errorf("OpenTable(%s) failed with %s", c.tn, err)
			} else {
				tn := tbl.Name()
				if tn != c.tn {
					t.Errorf("Name(%s) got %s want %s", c.tn, tn, c.tn)
				}
				tt := tbl.Type()
				if !reflect.DeepEqual(tt.ColumnNames, c.colNames) {
					t.Errorf("ColumnNames(%s) got %v want %v", c.tn, tt.ColumnNames, c.colNames)
				}
				if !reflect.DeepEqual(tt.ColumnTypes, c.colTypes) {
					t.Errorf("ColumnTypes(%s) got %v want %v", c.tn, tt.ColumnTypes, c.colTypes)
				}
				if !reflect.DeepEqual(tt.Key, c.primary) {
					t.Errorf("Key(%s) got %v want %v", c.tn, tt.Key, c.primary)
				}
				/*
					XXX:	tid  storage.TableId
				*/
			}
		case createTable:
			err := tx.CreateTable(ctx, c.tn, c.colNames, c.colTypes, c.primary)
			if c.fail {
				if err == nil {
					t.Errorf("CreateTable(%s) did not fail", c.tn)
				}
			} else if err != nil {
				t.Errorf("CreateTable(%s) failed with %s", c.tn, err)
			}
		case commit:
			err := tx.Commit(ctx)
			if err != nil {
				t.Fatalf("Commit() failed with %s", err)
			}
		case rollback:
			err := tx.Rollback()
			if err != nil {
				t.Fatalf("Rollback() failed with %s", err)
			}
		default:
			panic(fmt.Sprintf("unexpected case: %T %#v", c, c))
		}
	}
}
