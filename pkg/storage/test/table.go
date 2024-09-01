package test

import (
	"bytes"
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
	col6 = types.ID("col6", false)
)

func TestCreateTable(t *testing.T, store string, newStore NewStore) {
	st, err := newStore(t.TempDir())
	if err != nil {
		t.Fatalf("%s.NewStore() failed with %s", store, err)
	}

	colNames1 := []types.Identifier{col1, col2}
	colTypes1 := []types.ColumnType{types.IdColType, types.Int32ColType}
	primary1 := []types.ColumnKey{types.MakeColumnKey(0, false)}
	testStorage(t, st.Begin(), []interface{}{
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
			tid:      storage.EngineTableId + 1,
			colNames: colNames1,
			colTypes: colTypes1,
			primary:  primary1,
		},
		TableType{
			tid:      storage.EngineTableId + 1,
			ver:      1,
			colNames: colNames1,
			colTypes: colTypes1,
			key:      primary1,
		},
		Commit{},
	})

	testStorage(t, st.Begin(), []interface{}{
		OpenTable{
			tid:      storage.EngineTableId + 1,
			colNames: colNames1,
			colTypes: colTypes1,
			primary:  primary1,
		},
		TableType{
			tid:      storage.EngineTableId + 1,
			ver:      1,
			colNames: colNames1,
			colTypes: colTypes1,
			key:      primary1,
		},
		Rollback{},
	})

	colNames2 := []types.Identifier{col1, col2, col3}
	colTypes2 := []types.ColumnType{types.IdColType, types.Int32ColType, types.StringColType}
	primary2 := []types.ColumnKey{types.MakeColumnKey(2, false), types.MakeColumnKey(1, true)}
	testStorage(t, st.Begin(), []interface{}{
		CreateTable{
			tid:      storage.EngineTableId + 2,
			colNames: colNames2,
			colTypes: colTypes2,
			primary:  primary2,
		},
		Rollback{},
	})

	testStorage(t, st.Begin(), []interface{}{
		OpenTable{
			tid:      storage.EngineTableId + 2,
			panicked: true,
		},
		OpenTable{
			tid:      storage.EngineTableId + 1,
			colNames: colNames1,
			colTypes: colTypes1,
			primary:  primary1,
		},
		TableType{
			tid:      storage.EngineTableId + 1,
			ver:      1,
			colNames: colNames1,
			colTypes: colTypes1,
			key:      primary1,
		},
		Rollback{},
	})

	testStorage(t, st.Begin(), []interface{}{
		CreateTable{
			tid:      storage.EngineTableId + 2,
			colNames: colNames2,
			colTypes: colTypes2,
			primary:  primary2,
		},
		Commit{},
	})

	testStorage(t, st.Begin(), []interface{}{
		OpenTable{
			tid:      storage.EngineTableId + 1,
			colNames: colNames1,
			colTypes: colTypes1,
			primary:  primary1,
		},
		TableType{
			tid:      storage.EngineTableId + 1,
			ver:      1,
			colNames: colNames1,
			colTypes: colTypes1,
			key:      primary1,
		},
		OpenTable{
			tid:      storage.EngineTableId + 2,
			colNames: colNames2,
			colTypes: colTypes2,
			primary:  primary2,
		},
		TableType{
			tid:      storage.EngineTableId + 2,
			ver:      1,
			colNames: colNames2,
			colTypes: colTypes2,
			key:      primary2,
		},
		Rollback{},
	})

	testStorage(t, st.Begin(), []interface{}{
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
	testStorage(t, st.Begin(), []interface{}{
		CreateTable{
			tid:      storage.EngineTableId + 5,
			colNames: colNames5,
			colTypes: colTypes5,
		},
		OpenTable{
			tid:      storage.EngineTableId + 5,
			colNames: colNames5,
			colTypes: colTypes5,
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
	st, err := newStore(t.TempDir())
	if err != nil {
		t.Fatalf("%s.NewStore() failed with %s", store, err)
	}

	colNames := []types.Identifier{col1, col2}
	colTypes := []types.ColumnType{types.IdColType, types.Int32ColType}
	primary := []types.ColumnKey{types.MakeColumnKey(0, false)}
	testStorage(t, st.Begin(), []interface{}{
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
			tid:      storage.EngineTableId + 1,
			colNames: colNames,
			colTypes: colTypes,
			primary:  primary,
		},
		CreateTable{
			tid:      storage.EngineTableId + 2,
			colNames: colNames,
			colTypes: colTypes,
			primary:  primary,
		},
		OpenTable{
			tid:      storage.EngineTableId + 2,
			colNames: colNames,
			colTypes: colTypes,
			primary:  primary,
		},
		Commit{},
	})

	testStorage(t, st.Begin(), []interface{}{
		OpenTable{
			tid:      storage.EngineTableId + 1,
			colNames: colNames,
			colTypes: colTypes,
			primary:  primary,
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

	testStorage(t, st.Begin(), []interface{}{
		OpenTable{
			tid:      storage.EngineTableId + 1,
			colNames: colNames,
			colTypes: colTypes,
			primary:  primary,
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

	testStorage(t, st.Begin(), []interface{}{
		OpenTable{
			tid:      storage.EngineTableId + 1,
			panicked: true,
		},
		OpenTable{
			tid:      storage.EngineTableId + 2,
			colNames: colNames,
			colTypes: colTypes,
			primary:  primary,
		},
		Rollback{},
	})
}

func TestInsert(t *testing.T, store string, newStore NewStore) {
	st, err := newStore(t.TempDir())
	if err != nil {
		t.Fatalf("%s.NewStore() failed with %s", store, err)
	}

	colNames := []types.Identifier{col1, col2, col3, col4, col5}
	colTypes := []types.ColumnType{
		types.StringColType,
		types.BoolColType,
		types.ColumnType{Type: types.BytesType, Size: 2048},
		types.ColumnType{Type: types.Float64Type, NotNull: true},
		types.ColumnType{Type: types.Int64Type, Size: 4},
	}
	primary := []types.ColumnKey{types.MakeColumnKey(0, false)}
	testStorage(t, st.Begin(), []interface{}{
		CreateTable{
			tid:      storage.EngineTableId + 1,
			colNames: colNames,
			colTypes: colTypes,
			primary:  primary,
		},
		Commit{},
	})

	testStorage(t, st.Begin(), []interface{}{
		OpenTable{
			tid:      storage.EngineTableId + 1,
			colNames: colNames,
			colTypes: colTypes,
			primary:  primary,
		},
		Insert{
			rows: testutil.MustParseRows(`
('abcdef', true, null, 123.456, 789),
('ABC', false, '\x010203', 1.23, 45),
('xyz', false, null, 23.45)`),
		},
		Commit{},
	})

	testStorage(t, st.Begin(), []interface{}{
		OpenTable{
			tid:      storage.EngineTableId + 1,
			colNames: colNames,
			colTypes: colTypes,
			primary:  primary,
		},
		Select{
			rows: testutil.MustParseRows(`
('abcdef', true, null, 123.456, 789),
('ABC', false, '\x010203', 1.23, 45),
('xyz', false, null, 23.45, null)`),
			unordered: true,
		},
		Commit{},
	})

	testStorage(t, st.Begin(), []interface{}{
		OpenTable{
			tid:      storage.EngineTableId + 1,
			colNames: colNames,
			colTypes: colTypes,
			primary:  primary,
		},
		Insert{
			rows: testutil.MustParseRows(`
('abcdef',true, null, 123.456, 789)`),
			fail: true,
		},
		Rollback{},
	})

	testStorage(t, st.Begin(), []interface{}{
		OpenTable{
			tid:      storage.EngineTableId + 1,
			colNames: colNames,
			colTypes: colTypes,
			primary:  primary,
		},
		Insert{
			rows: testutil.MustParseRows(`
('qrst', true, null, 123.456, 789, false)`),
			fail: true,
		},
		Rollback{},
	})

	testStorage(t, st.Begin(), []interface{}{
		OpenTable{
			tid:      storage.EngineTableId + 1,
			colNames: colNames,
			colTypes: colTypes,
			primary:  primary,
		},
		Select{
			rows: testutil.MustParseRows(`
('abcdef', true, null, 123.456, 789),
('ABC', false, '\x010203', 1.23, 45),
('xyz', false, null, 23.45, null)`),
			unordered: true,
		},
		Rollback{},
	})
}

type predicateFunc struct {
	col         types.ColumnNum
	boolPred    func(b types.BoolValue) bool
	stringPred  func(s types.StringValue) bool
	bytesPred   func(b types.BytesValue) bool
	float64Pred func(f types.Float64Value) bool
	int64Pred   func(i types.Int64Value) bool
}

func (pf predicateFunc) Column() types.ColumnNum {
	return pf.col
}

func (pf predicateFunc) BoolPred(b types.BoolValue) bool {
	return pf.boolPred(b)
}

func (pf predicateFunc) StringPred(s types.StringValue) bool {
	return pf.stringPred(s)
}

func (pf predicateFunc) BytesPred(b types.BytesValue) bool {
	return pf.bytesPred(b)
}

func (pf predicateFunc) Float64Pred(f types.Float64Value) bool {
	return pf.float64Pred(f)
}

func (pf predicateFunc) Int64Pred(i types.Int64Value) bool {
	return pf.int64Pred(i)
}

func TestRows(t *testing.T, store string, newStore NewStore) {
	st, err := newStore(t.TempDir())
	if err != nil {
		t.Fatalf("%s.NewStore() failed with %s", store, err)
	}

	colNames1 := []types.Identifier{col1, col2, col3, col4}
	colTypes1 := []types.ColumnType{
		types.ColumnType{Type: types.Int64Type, Size: 4, NotNull: true},
		types.ColumnType{Type: types.Int64Type, Size: 4, NotNull: true},
		types.ColumnType{Type: types.Float64Type, NotNull: true},
		types.StringColType,
	}
	primary1 := []types.ColumnKey{types.MakeColumnKey(0, false)}

	testStorage(t, st.Begin(), []interface{}{
		CreateTable{
			tid:      storage.EngineTableId + 1,
			colNames: colNames1,
			colTypes: colTypes1,
			primary:  primary1,
		},
		Commit{},
	})

	testStorage(t, st.Begin(), []interface{}{
		OpenTable{
			tid:      storage.EngineTableId + 1,
			colNames: colNames1,
			colTypes: colTypes1,
			primary:  primary1,
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

	testStorage(t, st.Begin(), []interface{}{
		OpenTable{
			tid:      storage.EngineTableId + 1,
			colNames: colNames1,
			colTypes: colTypes1,
			primary:  primary1,
		},
		Rows{},
		Next{row: testutil.MustParseRow("(0, 0, 0, 'zero')")},
		Current{},
		Next{row: testutil.MustParseRow("(2, 20, 2.2, 'two')")},
		Current{},
		Next{row: testutil.MustParseRow("(4, 40, 4.4, 'four')")},
		Next{row: testutil.MustParseRow("(6, 60, 6.6, 'six')")},
		Next{row: testutil.MustParseRow("(8, 80, 8.8, 'eight')")},
		Next{row: testutil.MustParseRow("(10, 100, 10.10, 'ten')`)")},
		Next{
			eof: true,
		},
		Close{},
		Commit{},
	})

	testStorage(t, st.Begin(), []interface{}{
		OpenTable{
			tid:      storage.EngineTableId + 1,
			colNames: colNames1,
			colTypes: colTypes1,
			primary:  primary1,
		},
		Rows{},
		Current{
			panicked: true,
		},
		Close{},
		Rollback{},
	})

	testStorage(t, st.Begin(), []interface{}{
		OpenTable{
			tid:      storage.EngineTableId + 1,
			colNames: colNames1,
			colTypes: colTypes1,
			primary:  primary1,
		},
		Rows{},
		Next{row: testutil.MustParseRow("(0, 0, 0, 'zero')")},
		Close{},
		Next{
			panicked: true,
		},
		Rollback{},
	})

	testStorage(t, st.Begin(), []interface{}{
		OpenTable{
			tid:      storage.EngineTableId + 1,
			colNames: colNames1,
			colTypes: colTypes1,
			primary:  primary1,
		},
		Rows{},
		Next{row: testutil.MustParseRow("(0, 0, 0, 'zero')")},
		Current{},
		Close{},
		Current{
			panicked: true,
		},
		Rollback{},
	})

	testStorage(t, st.Begin(), []interface{}{
		OpenTable{
			tid:      storage.EngineTableId + 1,
			colNames: colNames1,
			colTypes: colTypes1,
			primary:  primary1,
		},
		Rows{},
		Next{row: testutil.MustParseRow("(0, 0, 0, 'zero')")},
		Close{},
		Close{
			panicked: true,
		},
		Rollback{},
	})

	testStorage(t, st.Begin(), []interface{}{
		OpenTable{
			tid:      storage.EngineTableId + 1,
			colNames: colNames1,
			colTypes: colTypes1,
			primary:  primary1,
		},
		Rows{},
		Next{row: testutil.MustParseRow("(0, 0, 0, 'zero')")},
		Rollback{
			panicked: true,
		},
		Close{},
		Rollback{},
	})

	testStorage(t, st.Begin(), []interface{}{
		OpenTable{
			tid:      storage.EngineTableId + 1,
			colNames: colNames1,
			colTypes: colTypes1,
			primary:  primary1,
		},
		Rows{},
		Next{row: testutil.MustParseRow("(0, 0, 0, 'zero')")},
		Commit{
			panicked: true,
		},
		Close{},
		Commit{},
	})

	testStorage(t, st.Begin(), []interface{}{
		OpenTable{
			tid:      storage.EngineTableId + 1,
			colNames: colNames1,
			colTypes: colTypes1,
			primary:  primary1,
		},
		Select{
			minRow: testutil.MustParseRow("(6, 0, 0, '')"),
			rows: testutil.MustParseRows(`
(6, 60, 6.6, 'six'),
(8, 80, 8.8, 'eight'),
(10, 100, 10.10, 'ten')`),
		},
		Commit{},
	})

	testStorage(t, st.Begin(), []interface{}{
		OpenTable{
			tid:      storage.EngineTableId + 1,
			colNames: colNames1,
			colTypes: colTypes1,
			primary:  primary1,
		},
		Select{
			maxRow: testutil.MustParseRow("(4, 0, 0, '')"),
			rows: testutil.MustParseRows(`
(0, 0, 0, 'zero'),
(2, 20, 2.2, 'two'),
(4, 40, 4.4, 'four')`),
		},
		Commit{},
	})

	testStorage(t, st.Begin(), []interface{}{
		OpenTable{
			tid:      storage.EngineTableId + 1,
			colNames: colNames1,
			colTypes: colTypes1,
			primary:  primary1,
		},
		Select{
			minRow: testutil.MustParseRow("(4, 0, 0, '')"),
			maxRow: testutil.MustParseRow("(8, 0, 0, '')"),
			rows: testutil.MustParseRows(`
(4, 40, 4.4, 'four'),
(6, 60, 6.6, 'six'),
(8, 80, 8.8, 'eight')`),
		},
		Commit{},
	})

	testStorage(t, st.Begin(), []interface{}{
		OpenTable{
			tid:      storage.EngineTableId + 1,
			colNames: colNames1,
			colTypes: colTypes1,
			primary:  primary1,
		},
		Select{
			minRow: testutil.MustParseRow("(3, 0, 0, '')"),
			maxRow: testutil.MustParseRow("(9, 0, 0, '')"),
			rows: testutil.MustParseRows(`
(4, 40, 4.4, 'four'),
(6, 60, 6.6, 'six'),
(8, 80, 8.8, 'eight')`),
		},
		Commit{},
	})

	testStorage(t, st.Begin(), []interface{}{
		OpenTable{
			tid:      storage.EngineTableId + 1,
			colNames: colNames1,
			colTypes: colTypes1,
			primary:  primary1,
		},
		Select{
			cols: []types.ColumnNum{3, 1},
			rows: testutil.MustParseRows(`
('zero', 0),
('two', 20),
('four', 40),
('six', 60),
('eight', 80),
('ten', 100)`),
		},
		Commit{},
	})

	testStorage(t, st.Begin(), []interface{}{
		OpenTable{
			tid:      storage.EngineTableId + 1,
			colNames: colNames1,
			colTypes: colTypes1,
			primary:  primary1,
		},
		Select{
			cols:   []types.ColumnNum{3, 1},
			minRow: testutil.MustParseRow("(3, 0, 0, '')"),
			maxRow: testutil.MustParseRow("(9, 0, 0, '')"),
			rows:   testutil.MustParseRows("('four', 40), ('six', 60), ('eight', 80)"),
		},
		Commit{},
	})

	colNames2 := []types.Identifier{col1, col2, col3, col4, col5, col6}
	colTypes2 := []types.ColumnType{
		types.ColumnType{Type: types.Int64Type, Size: 4},
		types.ColumnType{Type: types.BoolType},
		types.ColumnType{Type: types.StringType, Size: 1024},
		types.ColumnType{Type: types.BytesType, Size: 1024},
		types.ColumnType{Type: types.Float64Type},
		types.ColumnType{Type: types.Int64Type, Size: 4},
	}
	primary2 := []types.ColumnKey{types.MakeColumnKey(0, false)}

	testStorage(t, st.Begin(), []interface{}{
		CreateTable{
			tid:      storage.EngineTableId + 2,
			colNames: colNames2,
			colTypes: colTypes2,
			primary:  primary2,
		},
		Commit{},
	})

	testStorage(t, st.Begin(), []interface{}{
		OpenTable{
			tid:      storage.EngineTableId + 2,
			colNames: colNames2,
			colTypes: colTypes2,
			primary:  primary2,
		},
		Insert{
			rows: testutil.MustParseRows(`
(1, true, 'true', '\x0102', 1.1, 1),
(2, true, 'abc', '\x01', 2.2, 2),
(3, false, '', '\x010203', 3.3, 3),
(4, false, 'abc', '\x0102', 1.1, 2),
(5, false, 'false', '\x01', 3.3, 1),
(6, true, 'true', '\x01', 2.2, 2)
`),
		},
		Commit{},
	})

	testStorage(t, st.Begin(), []interface{}{
		OpenTable{
			tid:      storage.EngineTableId + 2,
			colNames: colNames2,
			colTypes: colTypes2,
			primary:  primary2,
		},
		Select{
			rows: testutil.MustParseRows(`
(1, true, 'true', '\x0102', 1.1, 1),
(2, true, 'abc', '\x01', 2.2, 2),
(3, false, '', '\x010203', 3.3, 3),
(4, false, 'abc', '\x0102', 1.1, 2),
(5, false, 'false', '\x01', 3.3, 1),
(6, true, 'true', '\x01', 2.2, 2)`),
		},
		Commit{},
	})

	testStorage(t, st.Begin(), []interface{}{
		OpenTable{
			tid:      storage.EngineTableId + 2,
			colNames: colNames2,
			colTypes: colTypes2,
			primary:  primary2,
		},
		Select{
			pred: predicateFunc{
				col:      1,
				boolPred: func(b types.BoolValue) bool { return b == true },
			},
			rows: testutil.MustParseRows(`
(1, true, 'true', '\x0102', 1.1, 1),
(2, true, 'abc', '\x01', 2.2, 2),
(6, true, 'true', '\x01', 2.2, 2)`),
		},
		Commit{},
	})

	testStorage(t, st.Begin(), []interface{}{
		OpenTable{
			tid:      storage.EngineTableId + 2,
			colNames: colNames2,
			colTypes: colTypes2,
			primary:  primary2,
		},
		Select{
			pred: predicateFunc{
				col:        2,
				stringPred: func(s types.StringValue) bool { return s == "abc" },
			},
			rows: testutil.MustParseRows(`
(2, true, 'abc', '\x01', 2.2, 2),
(4, false, 'abc', '\x0102', 1.1, 2)`),
		},
		Commit{},
	})

	testStorage(t, st.Begin(), []interface{}{
		OpenTable{
			tid:      storage.EngineTableId + 2,
			colNames: colNames2,
			colTypes: colTypes2,
			primary:  primary2,
		},
		Select{
			pred: predicateFunc{
				col: 3,
				bytesPred: func(b types.BytesValue) bool {
					return bytes.Compare(b, []byte{01}) == 0
				},
			},
			rows: testutil.MustParseRows(`
(2, true, 'abc', '\x01', 2.2, 2),
(5, false, 'false', '\x01', 3.3, 1),
(6, true, 'true', '\x01', 2.2, 2)`),
		},
		Commit{},
	})

	testStorage(t, st.Begin(), []interface{}{
		OpenTable{
			tid:      storage.EngineTableId + 2,
			colNames: colNames2,
			colTypes: colTypes2,
			primary:  primary2,
		},
		Select{
			pred: predicateFunc{
				col:         4,
				float64Pred: func(f types.Float64Value) bool { return f == 3.3 },
			},
			rows: testutil.MustParseRows(`
(3, false, '', '\x010203', 3.3, 3),
(5, false, 'false', '\x01', 3.3, 1)`),
		},
		Commit{},
	})

	testStorage(t, st.Begin(), []interface{}{
		OpenTable{
			tid:      storage.EngineTableId + 2,
			colNames: colNames2,
			colTypes: colTypes2,
			primary:  primary2,
		},
		Select{
			pred: predicateFunc{
				col:       5,
				int64Pred: func(i types.Int64Value) bool { return i == 2 },
			},
			rows: testutil.MustParseRows(`
(2, true, 'abc', '\x01', 2.2, 2),
(4, false, 'abc', '\x0102', 1.1, 2),
(6, true, 'true', '\x01', 2.2, 2)`),
		},
		Commit{},
	})

	testStorage(t, st.Begin(), []interface{}{
		OpenTable{
			tid:      storage.EngineTableId + 2,
			colNames: colNames2,
			colTypes: colTypes2,
			primary:  primary2,
		},
		Select{
			cols:   []types.ColumnNum{4, 1, 0},
			minRow: testutil.MustParseRow(`(2, true, 'abc', '\x01', 2.2, 2)`),
			maxRow: testutil.MustParseRow(`(5, false, 'false', '\x01', 3.3, 1)`),
			pred: predicateFunc{
				col:       5,
				int64Pred: func(i types.Int64Value) bool { return i == 2 },
			},
			rows: testutil.MustParseRows("(2.2, true, 2), (1.1, false, 4)"),
		},
		Commit{},
	})
}

func TestDelete(t *testing.T, store string, newStore NewStore) {
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

	testStorage(t, st.Begin(), []interface{}{
		CreateTable{
			tid:      storage.EngineTableId + 1,
			colNames: colNames,
			colTypes: colTypes,
			primary:  primary,
		},
		Commit{},
	})

	testStorage(t, st.Begin(), []interface{}{
		OpenTable{
			tid:      storage.EngineTableId + 1,
			colNames: colNames,
			colTypes: colTypes,
			primary:  primary,
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

	testStorage(t, st.Begin(), []interface{}{
		OpenTable{
			tid:      storage.EngineTableId + 1,
			colNames: colNames,
			colTypes: colTypes,
			primary:  primary,
		},
		Rows{},
		Next{row: testutil.MustParseRow("(0, 0, 0, 'zero')")},
		Next{row: testutil.MustParseRow("(2, 20, 2.2, 'two')")},
		Current{},
		Delete{},
		Close{},
		Commit{},
	})

	testStorage(t, st.Begin(), []interface{}{
		OpenTable{
			tid:      storage.EngineTableId + 1,
			colNames: colNames,
			colTypes: colTypes,
			primary:  primary,
		},
		Select{
			rows: testutil.MustParseRows(`
(0, 0, 0, 'zero'),
(4, 40, 4.4, 'four'),
(6, 60, 6.6, 'six'),
(8, 80, 8.8, 'eight'),
(10, 100, 10.10, 'ten')`),
		},
		Rows{},
		Next{row: testutil.MustParseRow("(0, 0, 0, 'zero')")},
		Next{row: testutil.MustParseRow("(4, 40, 4.4, 'four')")},
		Next{row: testutil.MustParseRow("(6, 60, 6.6, 'six')")},
		Current{},
		Close{},
		Delete{},
		Select{
			rows: testutil.MustParseRows(`
(0, 0, 0, 'zero'),
(4, 40, 4.4, 'four'),
(8, 80, 8.8, 'eight'),
(10, 100, 10.10, 'ten')`),
		},
		Commit{},
	})

	testStorage(t, st.Begin(), []interface{}{
		OpenTable{
			tid:      storage.EngineTableId + 1,
			colNames: colNames,
			colTypes: colTypes,
			primary:  primary,
		},
		Select{
			rows: testutil.MustParseRows(`
(0, 0, 0, 'zero'),
(4, 40, 4.4, 'four'),
(8, 80, 8.8, 'eight'),
(10, 100, 10.10, 'ten')`),
		},
		Rows{},
		Next{row: testutil.MustParseRow("(0, 0, 0, 'zero')")},
		Next{row: testutil.MustParseRow("(4, 40, 4.4, 'four')")},
		Current{},
		Delete{},
		Close{},
		Select{
			rows: testutil.MustParseRows(`
(0, 0, 0, 'zero'),
(8, 80, 8.8, 'eight'),
(10, 100, 10.10, 'ten')`),
		},
		Rollback{},
	})

	testStorage(t, st.Begin(), []interface{}{
		OpenTable{
			tid:      storage.EngineTableId + 1,
			colNames: colNames,
			colTypes: colTypes,
			primary:  primary,
		},
		Select{
			rows: testutil.MustParseRows(`
(0, 0, 0, 'zero'),
(4, 40, 4.4, 'four'),
(8, 80, 8.8, 'eight'),
(10, 100, 10.10, 'ten')`),
		},
		Rollback{},
	})

	testStorage(t, st.Begin(), []interface{}{
		OpenTable{
			tid:      storage.EngineTableId + 1,
			colNames: colNames,
			colTypes: colTypes,
			primary:  primary,
		},
		Rows{},
		Next{row: testutil.MustParseRow("(0, 0, 0, 'zero')")},
		Current{},
		Delete{},
		Delete{
			panicked: true,
		},
		Close{},
		Rollback{},
	})
}

func TestUpdate(t *testing.T, store string, newStore NewStore) {
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

	testStorage(t, st.Begin(), []interface{}{
		CreateTable{
			tid:      storage.EngineTableId + 1,
			colNames: colNames,
			colTypes: colTypes,
			primary:  primary,
		},
		Commit{},
	})

	testStorage(t, st.Begin(), []interface{}{
		OpenTable{
			tid:      storage.EngineTableId + 1,
			colNames: colNames,
			colTypes: colTypes,
			primary:  primary,
		},
		Insert{
			rows: testutil.MustParseRows(`
(0, 0, 0, 'zero'),
(2, 20, 2.2, 'two'),
(4, 40, 4.4, 'four'),
(6, 60, 6.6, 'six')`),
		},
		Commit{},
	})

	testStorage(t, st.Begin(), []interface{}{
		OpenTable{
			tid:      storage.EngineTableId + 1,
			colNames: colNames,
			colTypes: colTypes,
			primary:  primary,
		},
		Rows{},
		Next{row: testutil.MustParseRow("(0, 0, 0, 'zero')")},
		Next{row: testutil.MustParseRow("(2, 20, 2.2, 'two')")},
		Current{},
		Update{
			cols: []types.ColumnNum{3, 1},
			vals: []types.Value{types.StringValue("two two"), types.Int64Value(200)},
		},
		Close{},
		Commit{},
	})

	testStorage(t, st.Begin(), []interface{}{
		OpenTable{
			tid:      storage.EngineTableId + 1,
			colNames: colNames,
			colTypes: colTypes,
			primary:  primary,
		},
		Select{
			rows: testutil.MustParseRows(`
(0, 0, 0, 'zero'),
(2, 200, 2.2, 'two two'),
(4, 40, 4.4, 'four'),
(6, 60, 6.6, 'six')`),
		},
		Rows{},
		Next{row: testutil.MustParseRow("(0, 0, 0, 'zero')")},
		Next{row: testutil.MustParseRow("(2, 200, 2.2, 'two two')")},
		Next{row: testutil.MustParseRow("(4, 40, 4.4, 'four')")},
		Current{},
		Close{},
		Update{
			cols: []types.ColumnNum{0, 2},
			vals: []types.Value{types.Int64Value(1), types.Float64Value(8.8)},
		},
		Select{
			rows: testutil.MustParseRows(`
(0, 0, 0, 'zero'),
(1, 40, 8.8, 'four'),
(2, 200, 2.2, 'two two'),
(6, 60, 6.6, 'six')`),
		},
		Commit{},
	})

	testStorage(t, st.Begin(), []interface{}{
		OpenTable{
			tid:      storage.EngineTableId + 1,
			colNames: colNames,
			colTypes: colTypes,
			primary:  primary,
		},
		Select{
			rows: testutil.MustParseRows(`
(0, 0, 0, 'zero'),
(1, 40, 8.8, 'four'),
(2, 200, 2.2, 'two two'),
(6, 60, 6.6, 'six')`),
		},
		Rows{},
		Next{row: testutil.MustParseRow("(0, 0, 0, 'zero')")},
		Next{row: testutil.MustParseRow("(1, 40, 8.8, 'four')")},
		Current{},
		Update{
			cols: []types.ColumnNum{1},
			vals: []types.Value{types.Int64Value(400)},
		},
		Close{},
		Select{
			rows: testutil.MustParseRows(`
(0, 0, 0, 'zero'),
(1, 400, 8.8, 'four'),
(2, 200, 2.2, 'two two'),
(6, 60, 6.6, 'six')`),
		},
		Rollback{},
	})

	testStorage(t, st.Begin(), []interface{}{
		OpenTable{
			tid:      storage.EngineTableId + 1,
			colNames: colNames,
			colTypes: colTypes,
			primary:  primary,
		},
		Select{
			rows: testutil.MustParseRows(`
(0, 0, 0, 'zero'),
(1, 40, 8.8, 'four'),
(2, 200, 2.2, 'two two'),
(6, 60, 6.6, 'six')`),
		},
		Rollback{},
	})

	testStorage(t, st.Begin(), []interface{}{
		OpenTable{
			tid:      storage.EngineTableId + 1,
			colNames: colNames,
			colTypes: colTypes,
			primary:  primary,
		},
		Rows{},
		Next{row: testutil.MustParseRow("(0, 0, 0, 'zero')")},
		Current{},
		Update{
			cols:     []types.ColumnNum{1, 2},
			vals:     []types.Value{types.Int64Value(100)},
			panicked: true,
		},
		Close{},
		Rollback{},
	})

	testStorage(t, st.Begin(), []interface{}{
		OpenTable{
			tid:      storage.EngineTableId + 1,
			colNames: colNames,
			colTypes: colTypes,
			primary:  primary,
		},
		Rows{},
		Next{row: testutil.MustParseRow("(0, 0, 0, 'zero')")},
		Current{},
		Update{
			cols: []types.ColumnNum{0},
			vals: []types.Value{types.Int64Value(1)},
			fail: true,
		},
		Close{},
		Rollback{},
	})
}

func TestTable(t *testing.T, store string, newStore NewStore) {
	st, err := newStore(t.TempDir())
	if err != nil {
		t.Fatalf("%s.NewStore() failed with %s", store, err)
	}

	colNames := []types.Identifier{col1, col2, col3, col4}
	colTypes := []types.ColumnType{
		types.ColumnType{Type: types.Int64Type, Size: 4},
		types.ColumnType{Type: types.BoolType},
		types.ColumnType{Type: types.StringType, Size: 1024},
		types.ColumnType{Type: types.Int64Type, Size: 4},
	}
	primary := []types.ColumnKey{types.MakeColumnKey(0, false)}

	testStorage(t, st.Begin(), []interface{}{
		CreateTable{
			tid:      storage.EngineTableId + 1,
			colNames: colNames,
			colTypes: colTypes,
			primary:  primary,
		},
		Commit{},
	})

	var rows []types.Row
	for n := 1; n < 20; n++ {
		rows = append(rows,
			[]types.Value{
				types.Int64Value(n),
				types.BoolValue(n%2 == 0),
				types.StringValue(""),
				types.Int64Value(n),
			})
	}

	testStorage(t, st.Begin(), []interface{}{
		OpenTable{
			tid:      storage.EngineTableId + 1,
			colNames: colNames,
			colTypes: colTypes,
			primary:  primary,
		},
		Insert{
			rows: rows,
		},
		Commit{},
	})

	testStorage(t, st.Begin(), []interface{}{
		OpenTable{
			tid:      storage.EngineTableId + 1,
			colNames: colNames,
			colTypes: colTypes,
			primary:  primary,
		},
		Select{
			rows: rows,
		},
		Commit{},
	})

	testStorage(t, st.Begin(), []interface{}{
		OpenTable{
			tid:      storage.EngineTableId + 1,
			colNames: colNames,
			colTypes: colTypes,
			primary:  primary,
		},
		UpdateSet{
			pred: predicateFunc{
				col:      1,
				boolPred: func(b types.BoolValue) bool { return b == true },
			},
			update: func(row types.Row) ([]types.ColumnNum, []types.Value) {
				return []types.ColumnNum{3}, []types.Value{-(row[3].(types.Int64Value))}
			},
		},
		Commit{},
	})

	for _, row := range rows {
		if row[1].(types.BoolValue) == true {
			row[3] = -(row[3].(types.Int64Value))
		}
	}

	testStorage(t, st.Begin(), []interface{}{
		OpenTable{
			tid:      storage.EngineTableId + 1,
			colNames: colNames,
			colTypes: colTypes,
			primary:  primary,
		},
		Select{
			rows: rows,
		},
		Commit{},
	})

	testStorage(t, st.Begin(), []interface{}{
		OpenTable{
			tid:      storage.EngineTableId + 1,
			colNames: colNames,
			colTypes: colTypes,
			primary:  primary,
		},
		DeleteFrom{
			pred: predicateFunc{
				col:      1,
				boolPred: func(b types.BoolValue) bool { return b == false },
			},
		},
		Commit{},
	})

	var nrows []types.Row
	for _, row := range rows {
		if row[1].(types.BoolValue) == true {
			nrows = append(nrows, row)
		}
	}
	rows = nrows

	testStorage(t, st.Begin(), []interface{}{
		OpenTable{
			tid:      storage.EngineTableId + 1,
			colNames: colNames,
			colTypes: colTypes,
			primary:  primary,
		},
		Select{
			rows: rows,
		},
		Commit{},
	})
}
