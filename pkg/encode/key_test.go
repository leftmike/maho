package encode_test

import (
	"bytes"
	"math"
	"testing"

	"github.com/leftmike/maho/pkg/encode"
	"github.com/leftmike/maho/pkg/types"
)

func testMakeKey(t *testing.T, key []types.ColumnKey, values types.Row,
	makeRow func(val types.Value) types.Row) {

	var prev []byte
	for _, val := range values {
		row := makeRow(val)
		buf := encode.MakeKey(key, row)
		if bytes.Compare(prev, buf) >= 0 {
			t.Errorf("MakeKey() not greater: %s", row)
		}
		prev = buf
	}
}

func TestMakeKey(t *testing.T) {
	values := types.Row{
		nil,
		types.BoolValue(false),
		types.BoolValue(true),
		types.Int64Value(-999),
		types.Int64Value(-9),
		types.Int64Value(0),
		types.Int64Value(9),
		types.Int64Value(999),
		types.Float64Value(math.NaN()),
		types.Float64Value(-999.9),
		types.Float64Value(-9.9),
		types.Float64Value(0.0),
		types.Float64Value(9.9),
		types.Float64Value(999.9),
		types.StringValue("A"),
		types.StringValue("AA"),
		types.StringValue("AAA"),
		types.StringValue("AB"),
		types.StringValue("BBB"),
		types.StringValue("aaa"),
		types.BytesValue([]byte{0}),
		types.BytesValue([]byte{0, 0}),
		types.BytesValue([]byte{0, 0, 0}),
		types.BytesValue([]byte{0, 1}),
		types.BytesValue([]byte{1, 1}),
		types.BytesValue([]byte{2, 0, 0, 0, 1}),
		types.BytesValue([]byte{2, 0, 0, 1}),
		types.BytesValue([]byte{2, 0, 0, 2}),
		types.BytesValue([]byte{2, 2, 0, 0}),
		types.BytesValue([]byte{254, 0}),
		types.BytesValue([]byte{254, 0, 0}),
		types.BytesValue([]byte{254, 255}),
		types.BytesValue([]byte{255}),
	}

	reverseValues := types.Row{
		nil,
		types.BoolValue(true),
		types.BoolValue(false),
		types.Int64Value(999),
		types.Int64Value(9),
		types.Int64Value(0),
		types.Int64Value(-9),
		types.Int64Value(-999),
		types.Float64Value(999.9),
		types.Float64Value(9.9),
		types.Float64Value(0.0),
		types.Float64Value(-9.9),
		types.Float64Value(-999.9),
		types.Float64Value(math.NaN()),
		types.StringValue("aaa"),
		types.StringValue("BBB"),
		types.StringValue("AB"),
		types.StringValue("AAA"),
		types.StringValue("AA"),
		types.StringValue("A"),
		types.BytesValue([]byte{255}),
		types.BytesValue([]byte{254, 255}),
		types.BytesValue([]byte{254, 0, 0}),
		types.BytesValue([]byte{254, 0}),
		types.BytesValue([]byte{2, 2, 0, 0}),
		types.BytesValue([]byte{2, 0, 0, 2}),
		types.BytesValue([]byte{2, 0, 0, 1}),
		types.BytesValue([]byte{2, 0, 0, 0, 1}),
		types.BytesValue([]byte{1, 1}),
		types.BytesValue([]byte{0, 1}),
		types.BytesValue([]byte{0, 0, 0}),
		types.BytesValue([]byte{0, 0}),
		types.BytesValue([]byte{0}),
	}

	testMakeKey(t, []types.ColumnKey{types.MakeColumnKey(0, false)}, values,
		func(val types.Value) types.Row {
			return types.Row{val}
		})

	for _, val0 := range values {
		testMakeKey(t, []types.ColumnKey{
			types.MakeColumnKey(1, false),
			types.MakeColumnKey(0, false),
		}, values,
			func(val1 types.Value) types.Row {
				return types.Row{val0, val1}
			})
	}

	testMakeKey(t, []types.ColumnKey{types.MakeColumnKey(0, true)}, reverseValues,
		func(val types.Value) types.Row {
			return types.Row{val}
		})

	for _, val0 := range reverseValues {
		testMakeKey(t, []types.ColumnKey{
			types.MakeColumnKey(1, false),
			types.MakeColumnKey(0, true),
		}, values,
			func(val1 types.Value) types.Row {
				return types.Row{val0, val1}
			})
	}

	for _, val0 := range values {
		testMakeKey(t, []types.ColumnKey{
			types.MakeColumnKey(0, false),
			types.MakeColumnKey(1, true),
		}, reverseValues,
			func(val1 types.Value) types.Row {
				return types.Row{val0, val1}
			})
	}
}
