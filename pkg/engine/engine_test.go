package engine_test

import (
	"reflect"
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

	s := t.TempDir()
	store, err := basic.NewStore(s)
	if err != nil {
		t.Fatalf("NewStore(%s) failed with %s", s, err)
	}
	err = engine.Init(store)
	if err != nil {
		t.Fatalf("Init() failed with %s", err)
	}

	eng := engine.NewEngine(store)
	for _, c := range cases {
		switch c := c.(type) {
		case createDatabase:
			err := eng.CreateDatabase(c.dn, c.opts)
			if c.fail {
				if err == nil {
					t.Error("CreateDatabase() did not fail")
				}
			} else if err != nil {
				t.Errorf("CreateDatabase() failed with %s", err)
			}
		case dropDatabase:

		}
	}
}
