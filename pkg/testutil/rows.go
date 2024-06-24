package testutil

import (
	"strings"

	"github.com/leftmike/maho/pkg/types"
)

func FormatRows(rows []types.Row, sep string) string {
	var buf strings.Builder
	for rdx, r := range rows {
		if rdx > 0 && sep != "" {
			buf.WriteString(sep)
		}
		buf.WriteString(r.String())
	}

	return buf.String()
}

func RowsEqual(rows1, rows2 []types.Row, unordered bool) bool {
	// XXX
	return false
}
