package types

import (
	"fmt"
	"math"
)

type ColumnNum uint32
type ColumnKey int32

const (
	MaxColumnSize = math.MaxUint32 - 1
)

type ColumnType struct {
	Type ValueType

	// Size of the column in bytes for integers and in characters for character columns
	Size  uint32
	Fixed bool // fixed sized character column

	NotNull bool // not allowed to be NULL
}

var (
	IdColType         = ColumnType{Type: StringType, Size: MaxIdentifier, NotNull: true}
	Int32ColType      = ColumnType{Type: Int64Type, Size: 4, NotNull: true}
	Int64ColType      = ColumnType{Type: Int64Type, Size: 8, NotNull: true}
	NullInt64ColType  = ColumnType{Type: Int64Type, Size: 8}
	BoolColType       = ColumnType{Type: BoolType, NotNull: true}
	StringColType     = ColumnType{Type: StringType, Size: 4096, NotNull: true}
	NullStringColType = ColumnType{Type: StringType, Size: 4096}
)

func (ct ColumnType) String() string {
	switch ct.Type {
	case UnknownType:
		return "UNKNOWN"
	case BoolType:
		return "BOOL"
	case StringType:
		if ct.Size == 0 {
			panic("integer column type must have non-zero size")
		}

		if ct.Fixed {
			return fmt.Sprintf("CHAR(%d)", ct.Size)
		} else if ct.Size == MaxColumnSize {
			return "TEXT"
		} else {
			return fmt.Sprintf("VARCHAR(%d)", ct.Size)
		}
	case BytesType:
		if ct.Size == 0 {
			panic("bytes column type must have non-zero size")
		}

		if ct.Fixed {
			return fmt.Sprintf("BINARY(%d)", ct.Size)
		} else if ct.Size == MaxColumnSize {
			return "BYTES"
		} else {
			return fmt.Sprintf("VARBINARY(%d)", ct.Size)
		}
	case Float64Type:
		return "DOUBLE"
	case Int64Type:
		switch ct.Size {
		case 2:
			return "SMALLINT"
		case 4:
			return "INT"
		case 8:
			return "BIGINT"
		default:
			panic(fmt.Sprintf("integer size must be 2, 4, or 8: %d", ct.Size))
		}
	default:
		panic(fmt.Sprintf("unexpected column type: %#v %d", ct, ct.Type))
	}
}

func MakeColumnKey(num ColumnNum, reverse bool) ColumnKey {
	num += 1
	if reverse {
		return -ColumnKey(num)
	}
	return ColumnKey(num)
}

func (ck ColumnKey) Reverse() bool {
	return ck < 0
}

func (ck ColumnKey) Column() ColumnNum {
	if ck < 0 {
		ck = -ck
	}
	return ColumnNum(ck - 1)
}

func ColumnKeyUpdated(colKey []ColumnKey, cols []ColumnNum) bool {
	for _, ck := range colKey {
		for _, col := range cols {
			if col == ck.Column() {
				return true
			}
		}
	}

	return false
}
