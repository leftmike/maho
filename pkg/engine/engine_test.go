package engine_test

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"testing"

	"github.com/leftmike/maho/pkg/engine"
	"github.com/leftmike/maho/pkg/storage"
	"github.com/leftmike/maho/pkg/storage/basic"
	"github.com/leftmike/maho/pkg/types"
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

func TestDatabase(t *testing.T) {
	cases := []interface{}{
		createDatabase{
			dn: types.ID("db", false),
		},
		createDatabase{
			dn:   types.ID("db", false),
			fail: true,
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
			// XXX
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
}

type createSchema struct {
	sn   types.SchemaName
	fail bool
}

// XXX: dropSchema

type listSchemas struct {
	dn      types.Identifier
	schemas []types.Identifier
	fail    bool
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
		// XXX: dropSchema
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
