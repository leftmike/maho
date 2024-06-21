package test

import (
	"context"
	"reflect"
	"testing"

	"github.com/leftmike/maho/pkg/storage"
	"github.com/leftmike/maho/pkg/types"
)

type NewStore func(dataDir string) (storage.Store, error)

func errorPanicked(fn func() error) (err error, panicked bool) {
	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(string); ok {
				panicked = true
			} else {
				panic(r)
			}
		}
	}()

	return fn(), false
}

func TestCreateTable(t *testing.T, store string, newStore NewStore) {
	t.Helper()

	db := types.ID("db", false)
	scm := types.ID("scm", false)

	col1 := types.ID("col1", false)
	col2 := types.ID("col2", false)
	col3 := types.ID("col3", false)
	//col4 := types.ID("col4", false)
	//col5 := types.ID("col5", false)

	cases := []struct {
		tid      storage.TableId
		name     types.TableName
		colNames []types.Identifier
		colTypes []types.ColumnType
		primary  []types.ColumnKey
		panicked bool
	}{
		{
			tid:      storage.EngineTableId,
			name:     types.TableName{db, scm, types.ID("table1", false)},
			colNames: []types.Identifier{col1, col2},
			colTypes: []types.ColumnType{types.IdColType, types.Int32ColType},
			primary:  []types.ColumnKey{types.MakeColumnKey(0, false)},
		},
		{
			tid:      1,
			panicked: true,
		},
		{
			tid:      storage.EngineTableId + 1,
			name:     types.TableName{db, scm, types.ID("table1", false)},
			colNames: []types.Identifier{col1, col2, col3},
			colTypes: []types.ColumnType{types.IdColType, types.Int32ColType, types.StringColType},
			primary:  []types.ColumnKey{types.MakeColumnKey(0, false)},
		},
		{
			tid:      storage.EngineTableId + 2,
			name:     types.TableName{db, scm, types.ID("table1", false)},
			colNames: []types.Identifier{col1, col2, col3},
			colTypes: []types.ColumnType{types.IdColType, types.Int32ColType},
			panicked: true,
		},
		{
			tid:      storage.EngineTableId + 1,
			name:     types.TableName{db, scm, types.ID("table1", false)},
			colNames: []types.Identifier{col1, col2, col3},
			colTypes: []types.ColumnType{types.IdColType, types.Int32ColType, types.StringColType},
			primary:  []types.ColumnKey{types.MakeColumnKey(3, false)},
			panicked: true,
		},
	}

	st, err := newStore(t.TempDir())
	if err != nil {
		t.Fatalf("%s.NewStore() failed with %s", store, err)
	}

	ctx := context.Background()
	tx := st.Begin()

	for _, c := range cases {
		err, panicked := errorPanicked(func() error {
			return tx.CreateTable(ctx, c.tid, c.name, c.colNames, c.colTypes, c.primary)
		})
		if panicked {
			if !c.panicked {
				t.Errorf("CreateTable(%d) panicked", c.tid)
			}
			continue
		} else if c.panicked {
			t.Errorf("CreateTable(%d) did not panic", c.tid)
		} else if err != nil {
			t.Errorf("CreateTable(%d) failed with %s", c.tid, err)
		}

		tbl, err := tx.OpenTable(ctx, c.tid)
		if err != nil {
			t.Errorf("OpenTable(%d) failed with %s", c.tid, err)
			continue
		}

		ver := tbl.Version()
		if ver != 1 {
			t.Errorf("%d.Version() got %d want 1", c.tid, ver)
		}
		name := tbl.Name()
		if name != c.name {
			t.Errorf("%d.Name() got %s want %s", c.tid, name, c.name)
		}

		// XXX: if c.primary == nil, adjust cn and ct to skip 0 columns
		cn := tbl.ColumnNames()
		if !reflect.DeepEqual(cn, c.colNames) {
			t.Errorf("%d.ColumnNames() got %#v want %#v", c.tid, cn, c.colNames)
		}
		ct := tbl.ColumnTypes()
		if !reflect.DeepEqual(ct, c.colTypes) {
			t.Errorf("%d.ColumnTypes() got %#v want %#v", c.tid, ct, c.colTypes)
		}
		p := tbl.Primary()
		if !reflect.DeepEqual(p, c.primary) {
			t.Errorf("%d.Primary() got %#v want %#v", c.tid, p, c.primary)
		}
	}
}

func TestStore(t *testing.T, store string, newStore NewStore) {
	t.Helper()

	_, err := newStore(t.TempDir())
	if err != nil {
		t.Errorf("%s.NewStore() failed with %s", store, err)
	}
}
