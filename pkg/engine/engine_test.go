package engine_test

import (
	"reflect"
	"testing"

	"github.com/leftmike/maho/pkg/engine"
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
