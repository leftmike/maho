package test

import (
	"testing"

	"github.com/leftmike/maho/pkg/storage"
	"github.com/leftmike/maho/pkg/testutil"
	"github.com/leftmike/maho/pkg/types"
)

var (
	col1 = types.ID("col1", false)
	col2 = types.ID("col2", false)
	col3 = types.ID("col3", false)
	col4 = types.ID("col4", false)
	col5 = types.ID("col5", false)
)

func TestCreateTable(t *testing.T, store string, newStore NewStore) {
	t.Helper()

	st, err := newStore(t.TempDir())
	if err != nil {
		t.Fatalf("%s.NewStore() failed with %s", store, err)
	}

	colNames1 := []types.Identifier{col1, col2}
	colTypes1 := []types.ColumnType{types.IdColType, types.Int32ColType}
	primary1 := []types.ColumnKey{types.MakeColumnKey(0, false)}
	testStorage(t, st.Begin(), nil, []interface{}{
		OpenTable{
			tid:      storage.EngineTableId + 1,
			panicked: true,
		},
		CreateTable{
			tid:      storage.EngineTableId + 1,
			colNames: colNames1,
			colTypes: colTypes1,
			primary:  primary1,
		},
		OpenTable{
			tid: storage.EngineTableId + 1,
		},
		TableType{
			tid:      storage.EngineTableId + 1,
			ver:      1,
			colNames: colNames1,
			colTypes: colTypes1,
			primary:  primary1,
		},
		Commit{},
	})

	testStorage(t, st.Begin(), nil, []interface{}{
		OpenTable{
			tid: storage.EngineTableId + 1,
		},
		TableType{
			tid:      storage.EngineTableId + 1,
			ver:      1,
			colNames: colNames1,
			colTypes: colTypes1,
			primary:  primary1,
		},
		Rollback{},
	})

	colNames2 := []types.Identifier{col1, col2, col3}
	colTypes2 := []types.ColumnType{types.IdColType, types.Int32ColType, types.StringColType}
	primary2 := []types.ColumnKey{types.MakeColumnKey(2, false), types.MakeColumnKey(1, true)}
	testStorage(t, st.Begin(), nil, []interface{}{
		CreateTable{
			tid:      storage.EngineTableId + 2,
			colNames: colNames2,
			colTypes: colTypes2,
			primary:  primary2,
		},
		Rollback{},
	})

	testStorage(t, st.Begin(), nil, []interface{}{
		OpenTable{
			tid:      storage.EngineTableId + 2,
			panicked: true,
		},
		OpenTable{
			tid: storage.EngineTableId + 1,
		},
		TableType{
			tid:      storage.EngineTableId + 1,
			ver:      1,
			colNames: colNames1,
			colTypes: colTypes1,
			primary:  primary1,
		},
		Rollback{},
	})

	testStorage(t, st.Begin(), nil, []interface{}{
		CreateTable{
			tid:      storage.EngineTableId + 2,
			colNames: colNames2,
			colTypes: colTypes2,
			primary:  primary2,
		},
		Commit{},
	})

	testStorage(t, st.Begin(), nil, []interface{}{
		OpenTable{
			tid: storage.EngineTableId + 1,
		},
		TableType{
			tid:      storage.EngineTableId + 1,
			ver:      1,
			colNames: colNames1,
			colTypes: colTypes1,
			primary:  primary1,
		},
		OpenTable{
			tid: storage.EngineTableId + 2,
		},
		TableType{
			tid:      storage.EngineTableId + 2,
			ver:      1,
			colNames: colNames2,
			colTypes: colTypes2,
			primary:  primary2,
		},
		Rollback{},
	})

	testStorage(t, st.Begin(), nil, []interface{}{
		CreateTable{
			tid:      1,
			panicked: true,
		},
		CreateTable{
			tid:      storage.EngineTableId + 3,
			colNames: []types.Identifier{col1, col2, col3},
			colTypes: []types.ColumnType{types.IdColType, types.Int32ColType},
			panicked: true,
		},
		CreateTable{
			tid:      storage.EngineTableId + 4,
			colNames: []types.Identifier{col1, col2, col3},
			colTypes: []types.ColumnType{types.IdColType, types.Int32ColType, types.StringColType},
			primary:  []types.ColumnKey{types.MakeColumnKey(3, false)},
			panicked: true,
		},
		Commit{},
	})

	colNames5 := []types.Identifier{col1, col2, col3}
	colTypes5 := []types.ColumnType{types.IdColType, types.Int32ColType, types.StringColType}
	testStorage(t, st.Begin(), nil, []interface{}{
		CreateTable{
			tid:      storage.EngineTableId + 5,
			colNames: colNames5,
			colTypes: colTypes5,
		},
		OpenTable{
			tid: storage.EngineTableId + 5,
		},
		TableType{
			tid:      storage.EngineTableId + 5,
			ver:      1,
			colNames: colNames5,
			colTypes: colTypes5,
		},
		Commit{},
	})
}

func TestDropTable(t *testing.T, store string, newStore NewStore) {
	t.Helper()

	st, err := newStore(t.TempDir())
	if err != nil {
		t.Fatalf("%s.NewStore() failed with %s", store, err)
	}

	colNames := []types.Identifier{col1, col2}
	colTypes := []types.ColumnType{types.IdColType, types.Int32ColType}
	primary := []types.ColumnKey{types.MakeColumnKey(0, false)}
	testStorage(t, st.Begin(), nil, []interface{}{
		DropTable{
			tid:      storage.EngineTableId + 1,
			panicked: true,
		},
		CreateTable{
			tid:      storage.EngineTableId + 1,
			colNames: colNames,
			colTypes: colTypes,
			primary:  primary,
		},
		OpenTable{
			tid: storage.EngineTableId + 1,
		},
		CreateTable{
			tid:      storage.EngineTableId + 2,
			colNames: colNames,
			colTypes: colTypes,
			primary:  primary,
		},
		OpenTable{
			tid: storage.EngineTableId + 2,
		},
		Commit{},
	})

	testStorage(t, st.Begin(), nil, []interface{}{
		OpenTable{
			tid: storage.EngineTableId + 1,
		},
		DropTable{
			tid: storage.EngineTableId + 1,
		},
		OpenTable{
			tid:      storage.EngineTableId + 1,
			panicked: true,
		},
		Rollback{},
	})

	testStorage(t, st.Begin(), nil, []interface{}{
		OpenTable{
			tid: storage.EngineTableId + 1,
		},
		DropTable{
			tid: storage.EngineTableId + 1,
		},
		OpenTable{
			tid:      storage.EngineTableId + 1,
			panicked: true,
		},
		Commit{},
	})

	testStorage(t, st.Begin(), nil, []interface{}{
		OpenTable{
			tid:      storage.EngineTableId + 1,
			panicked: true,
		},
		OpenTable{
			tid: storage.EngineTableId + 2,
		},
		Rollback{},
	})
}

func TestInsert(t *testing.T, store string, newStore NewStore) {
	t.Helper()

	st, err := newStore(t.TempDir())
	if err != nil {
		t.Fatalf("%s.NewStore() failed with %s", store, err)
	}

	colNames := []types.Identifier{col1, col2, col3, col4, col5}
	colTypes := []types.ColumnType{
		types.BoolColType,
		types.StringColType,
		types.ColumnType{Type: types.BytesType, Size: 2048},
		types.ColumnType{Type: types.Float64Type, NotNull: true},
		types.ColumnType{Type: types.Int64Type, Size: 4},
	}
	primary := []types.ColumnKey{types.MakeColumnKey(1, false)}
	testStorage(t, st.Begin(), nil, []interface{}{
		CreateTable{
			tid:      storage.EngineTableId + 1,
			colNames: colNames,
			colTypes: colTypes,
			primary:  primary,
		},
		Commit{},
	})

	testStorage(t, st.Begin(), nil, []interface{}{
		OpenTable{
			tid: storage.EngineTableId + 1,
		},
		Insert{
			rows: testutil.MustParseRows(`
(true, 'abcdef', null, 123.456, 789),
(false, 'ABC', '\x010203', 1.23, 45),
(false, 'xyz', null, 23.45)`),
		},
		Commit{},
	})

	testStorage(t, st.Begin(), nil, []interface{}{
		OpenTable{
			tid: storage.EngineTableId + 1,
		},
		Insert{
			rows: testutil.MustParseRows(`
(true, 'abcdef', null, 123.456, 789)`),
			fail: true,
		},
		Rollback{},
	})

	testStorage(t, st.Begin(), nil, []interface{}{
		OpenTable{
			tid: storage.EngineTableId + 1,
		},
		Insert{
			rows: testutil.MustParseRows(`
(true, 'qrst', null, 123.456, 789, false)`),
			fail: true,
		},
		Rollback{},
	})
}

func TestRows(t *testing.T, store string, newStore NewStore) {
	t.Helper()

	st, err := newStore(t.TempDir())
	if err != nil {
		t.Fatalf("%s.NewStore() failed with %s", store, err)
	}

	colNames := []types.Identifier{col1, col2, col3, col4}
	colTypes := []types.ColumnType{
		types.ColumnType{Type: types.Int64Type, Size: 4, NotNull: true},
		types.ColumnType{Type: types.Int64Type, Size: 4, NotNull: true},
		types.ColumnType{Type: types.Float64Type, NotNull: true},
		types.StringColType,
	}
	primary := []types.ColumnKey{types.MakeColumnKey(0, false)}

	testStorage(t, st.Begin(), nil, []interface{}{
		CreateTable{
			tid:      storage.EngineTableId + 1,
			colNames: colNames,
			colTypes: colTypes,
			primary:  primary,
		},
		Commit{},
	})

	testStorage(t, st.Begin(), nil, []interface{}{
		OpenTable{
			tid: storage.EngineTableId + 1,
		},
		Insert{
			rows: testutil.MustParseRows(`
(0, 0, 0, 'zero'),
(2, 20, 2.2, 'two'),
(4, 40, 4.4, 'four'),
(6, 60, 6.6, 'six'),
(8, 80, 8.8, 'eight'),
(10, 100, 10.10, 'ten')`),
		},
		Commit{},
	})

	testStorage(t, st.Begin(), nil, []interface{}{
		OpenTable{
			tid: storage.EngineTableId + 1,
		},
		Rows{},
		Next{
			row: testutil.MustParseRow("(0, 0, 0, 'zero')"),
		},
		Current{},
		Next{
			row: testutil.MustParseRow("(2, 20, 2.2, 'two')"),
		},
		Current{},
		Next{
			row: testutil.MustParseRow("(4, 40, 4.4, 'four')"),
		},
		Next{
			row: testutil.MustParseRow("(6, 60, 6.6, 'six')"),
		},
		Next{
			row: testutil.MustParseRow("(8, 80, 8.8, 'eight')"),
		},
		Next{
			row: testutil.MustParseRow("(10, 100, 10.10, 'ten')`)"),
		},
		Next{
			eof: true,
		},
		Close{},
		Commit{},
	})

	testStorage(t, st.Begin(), nil, []interface{}{
		OpenTable{
			tid: storage.EngineTableId + 1,
		},
		Rows{},
		Current{
			panicked: true,
		},
		Close{},
		Rollback{},
	})

	testStorage(t, st.Begin(), nil, []interface{}{
		OpenTable{
			tid: storage.EngineTableId + 1,
		},
		Rows{},
		Next{
			row: testutil.MustParseRow("(0, 0, 0, 'zero')"),
		},
		Close{},
		Next{
			panicked: true,
		},
		Rollback{},
	})

	testStorage(t, st.Begin(), nil, []interface{}{
		OpenTable{
			tid: storage.EngineTableId + 1,
		},
		Rows{},
		Next{
			row: testutil.MustParseRow("(0, 0, 0, 'zero')"),
		},
		Current{},
		Close{},
		Current{
			panicked: true,
		},
		Rollback{},
	})

	testStorage(t, st.Begin(), nil, []interface{}{
		OpenTable{
			tid: storage.EngineTableId + 1,
		},
		Rows{},
		Next{
			row: testutil.MustParseRow("(0, 0, 0, 'zero')"),
		},
		Close{},
		Close{
			panicked: true,
		},
		Rollback{},
	})

	testStorage(t, st.Begin(), nil, []interface{}{
		OpenTable{
			tid: storage.EngineTableId + 1,
		},
		Rows{},
		Next{
			row: testutil.MustParseRow("(0, 0, 0, 'zero')"),
		},
		Rollback{
			panicked: true,
		},
		Close{},
		Rollback{},
	})

	testStorage(t, st.Begin(), nil, []interface{}{
		OpenTable{
			tid: storage.EngineTableId + 1,
		},
		Rows{},
		Next{
			row: testutil.MustParseRow("(0, 0, 0, 'zero')"),
		},
		Commit{
			panicked: true,
		},
		Close{},
		Commit{},
	})

	testStorage(t, st.Begin(), nil, []interface{}{
		OpenTable{
			tid: storage.EngineTableId + 1,
		},
		Rows{
			minRow: testutil.MustParseRow("(6, 0, 0, '')"),
		},
		Next{
			row: testutil.MustParseRow("(6, 60, 6.6, 'six')"),
		},
		Next{
			row: testutil.MustParseRow("(8, 80, 8.8, 'eight')"),
		},
		Next{
			row: testutil.MustParseRow("(10, 100, 10.10, 'ten')`)"),
		},
		Next{
			eof: true,
		},
		Close{},
		Commit{},
	})

	testStorage(t, st.Begin(), nil, []interface{}{
		OpenTable{
			tid: storage.EngineTableId + 1,
		},
		Rows{
			maxRow: testutil.MustParseRow("(4, 0, 0, '')"),
		},
		Next{
			row: testutil.MustParseRow("(0, 0, 0, 'zero')"),
		},
		Next{
			row: testutil.MustParseRow("(2, 20, 2.2, 'two')"),
		},
		Next{
			row: testutil.MustParseRow("(4, 40, 4.4, 'four')"),
		},
		Next{
			eof: true,
		},
		Close{},
		Commit{},
	})

	testStorage(t, st.Begin(), nil, []interface{}{
		OpenTable{
			tid: storage.EngineTableId + 1,
		},
		Rows{
			minRow: testutil.MustParseRow("(4, 0, 0, '')"),
			maxRow: testutil.MustParseRow("(8, 0, 0, '')"),
		},
		Next{
			row: testutil.MustParseRow("(4, 40, 4.4, 'four')"),
		},
		Next{
			row: testutil.MustParseRow("(6, 60, 6.6, 'six')"),
		},
		Next{
			row: testutil.MustParseRow("(8, 80, 8.8, 'eight')"),
		},
		Next{
			eof: true,
		},
		Close{},
		Commit{},
	})

	testStorage(t, st.Begin(), nil, []interface{}{
		OpenTable{
			tid: storage.EngineTableId + 1,
		},
		Rows{
			minRow: testutil.MustParseRow("(3, 0, 0, '')"),
			maxRow: testutil.MustParseRow("(9, 0, 0, '')"),
		},
		Next{
			row: testutil.MustParseRow("(4, 40, 4.4, 'four')"),
		},
		Next{
			row: testutil.MustParseRow("(6, 60, 6.6, 'six')"),
		},
		Next{
			row: testutil.MustParseRow("(8, 80, 8.8, 'eight')"),
		},
		Next{
			eof: true,
		},
		Close{},
		Commit{},
	})
}
