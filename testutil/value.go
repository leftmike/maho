package testutil

import (
	"github.com/leftmike/maho/types"
)

func B(b bool) types.BoolValue         { return types.BoolValue(b) }
func S(s string) types.StringValue     { return types.StringValue(s) }
func Bytes(b ...byte) types.BytesValue { return types.BytesValue(b) }
func F(f float64) types.Float64Value   { return types.Float64Value(f) }
func I(i int) types.Int64Value         { return types.Int64Value(i) }
