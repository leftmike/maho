package testutil

import (
	"sort"
	"strings"

	"github.com/leftmike/maho/types"
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

func CompareRows(row1, row2 types.Row) int {
	rdx := 0
	for rdx < len(row1) && rdx < len(row2) {
		cmp := types.Compare(row1[rdx], row2[rdx])
		if cmp != 0 {
			return cmp
		}

		rdx += 1
	}

	if rdx < len(row1) {
		return 1
	} else if rdx < len(row2) {
		return -1
	}

	return 0
}

func RowsEqual(rows1, rows2 []types.Row, unordered bool) bool {
	if len(rows1) != len(rows2) {
		return false
	}

	if unordered {
		sort.Slice(rows1,
			func(i, j int) bool {
				return CompareRows(rows1[i], rows1[j]) < 0
			})
		sort.Slice(rows2,
			func(i, j int) bool {
				return CompareRows(rows2[i], rows2[j]) < 0
			})
	}

	for idx := range rows1 {
		if CompareRows(rows1[idx], rows2[idx]) != 0 {
			return false
		}
	}

	return true
}
