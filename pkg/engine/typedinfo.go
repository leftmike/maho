package engine

import (
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"
	"unicode"

	"github.com/leftmike/maho/pkg/storage"
	"github.com/leftmike/maho/pkg/types"
)

type TypedInfo struct {
	typ      reflect.Type
	tid      storage.TableId
	tn       types.TableName
	colNames []types.Identifier
	colTypes []types.ColumnType
	primary  []types.ColumnKey
	fldNames []string
}

func fieldNameToColumnName(n string) types.Identifier {
	s := strings.NewReader(n)
	r, _, err := s.ReadRune()
	if err != nil {
		panic(fmt.Sprintf("typed table: bad field name: %s: %s", n, err))
	}
	if !unicode.IsUpper(r) {
		panic(fmt.Sprintf("typed table: bad field name: %s", n))
	}

	var buf strings.Builder
	buf.WriteRune(unicode.ToLower(r))

	upper := true
	for {
		r, _, err = s.ReadRune()
		if err == io.EOF {
			break
		} else if err != nil {
			panic(fmt.Sprintf("typed table: bad field name: %s: %s", n, err))
		}

		if unicode.IsUpper(r) {
			if !upper {
				buf.WriteRune('_')
			}
			buf.WriteRune(unicode.ToLower(r))
			upper = true
		} else {
			buf.WriteRune(r)
			upper = false
		}
	}

	return types.ID(buf.String(), true)
}

var (
	validTags = map[string]bool{
		"name":    true,
		"notnull": false,
		"primary": false,
		"size":    true,
	}
)

func fieldTags(s string) map[string]string {
	if s == "" {
		return nil
	}

	tags := map[string]string{}
	flds := strings.Split(s, ",")
	for _, fld := range flds {
		kv := strings.Split(fld, "=")
		hasKV, ok := validTags[kv[0]]
		if !ok || (hasKV && len(kv) != 2) || (!hasKV && len(kv) != 1) {
			panic(fmt.Sprintf("typed table: bad struct field tag: %s", fld))
		}

		if hasKV {
			tags[kv[0]] = kv[1]
		} else {
			tags[kv[0]] = ""
		}
	}

	return tags
}

func MakeTypedInfo(tid storage.TableId, tn types.TableName, st interface{}) *TypedInfo {
	typ := reflect.TypeOf(st)
	if typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}

	if typ.Kind() != reflect.Struct {
		panic(fmt.Sprintf("typed table: not a struct or a pointer to a struct: %T", st))
	}

	var colNames []types.Identifier
	var colTypes []types.ColumnType
	var primary []types.ColumnKey
	var fldNames []string
	for idx := 0; idx < typ.NumField(); idx += 1 {
		fld := typ.Field(idx)
		tags := fieldTags(fld.Tag.Get("maho"))
		if name, ok := tags["name"]; ok {
			colNames = append(colNames, types.ID(name, true))
		} else {
			colNames = append(colNames, fieldNameToColumnName(fld.Name))
		}
		fldNames = append(fldNames, fld.Name)

		size := uint32(1)
		if val, ok := tags["size"]; ok {
			n, err := strconv.Atoi(val)
			if err != nil || n <= 0 {
				panic(fmt.Sprintf("typed table: size not a positive integer: %s: %s", val, err))
			}
			size = uint32(n)
		}

		notNull := true
		ftyp := fld.Type
		if ftyp.Kind() == reflect.Pointer {
			ftyp = ftyp.Elem()
			if ftyp.Kind() == reflect.Slice {
				panic("typed table: must not be pointer to a slice")
			}

			notNull = false
		}

		var vt types.ValueType
		switch ftyp.Kind() {
		case reflect.Bool:
			vt = types.BoolType
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			vt = types.Int64Type
			size = uint32(ftyp.Size())
		case reflect.Float32, reflect.Float64:
			vt = types.Float64Type
			size = 8
		case reflect.Slice:
			elem := ftyp.Elem()
			if elem.Kind() != reflect.Uint8 || elem.Size() != 1 {
				panic(fmt.Sprintf("typed table: must be a slice of bytes: %s", elem))
			}
			vt = types.BytesType
		case reflect.String:
			vt = types.StringType
		default:
			panic(fmt.Sprintf("typed table: bad field type: %s: %s", fld.Name, ftyp.Kind()))
		}

		_, ok := tags["notnull"]
		if ftyp.Kind() == reflect.Slice {
			notNull = ok
		} else if ok {
			panic(fmt.Sprintf("typed table: not null tag must be on a slice: %s", ftyp.Kind()))
		}

		ct := types.ColumnType{
			Type:    vt,
			Size:    size,
			NotNull: notNull,
		}
		colTypes = append(colTypes, ct)

		if _, ok := tags["primary"]; ok {
			primary = append(primary, types.MakeColumnKey(types.ColumnNum(idx), false))
		}
	}

	return &TypedInfo{
		typ:      typ,
		tid:      tid,
		tn:       tn,
		colNames: colNames,
		colTypes: colTypes,
		primary:  primary,
		fldNames: fldNames,
	}
}

func (ti *TypedInfo) TableType() *TableType {
	return &TableType{
		Version:     1,
		ColumnNames: ti.colNames,
		ColumnTypes: ti.colTypes,
		Key:         ti.primary,
	}
}

func (ti *TypedInfo) TableId() storage.TableId {
	return ti.tid
}

func (ti *TypedInfo) TableName() types.TableName {
	return ti.tn
}
