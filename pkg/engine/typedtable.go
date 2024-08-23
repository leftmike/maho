package engine

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"
	"unicode"

	"github.com/leftmike/maho/pkg/storage"
	"github.com/leftmike/maho/pkg/types"
)

type typedTableInfo struct {
	colNames []types.Identifier
	colTypes []types.ColumnType
	primary  []types.ColumnKey
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
		"primary": false,
		"fixed":   false,
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

func makeTypedTableInfo(row interface{}) *typedTableInfo {
	typ := reflect.TypeOf(row)
	//val := reflect.ValueOf(row)
	if typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
		//val = val.Elem()
	}

	if typ.Kind() != reflect.Struct {
		panic(fmt.Sprintf("typed table: not a struct or a pointer to a struct: %T", row))
	}

	var colNames []types.Identifier
	var colTypes []types.ColumnType
	var primary []types.ColumnKey
	for idx := 0; idx < typ.NumField(); idx += 1 {
		fld := typ.Field(idx)
		tags := fieldTags(fld.Tag.Get("maho"))
		if name, ok := tags["name"]; ok {
			colNames = append(colNames, types.ID(name, true))
		} else {
			colNames = append(colNames, fieldNameToColumnName(fld.Name))
		}

		size := uint32(1)
		if val, ok := tags["size"]; ok {
			n, err := strconv.Atoi(val)
			if err != nil || n <= 0 {
				panic(fmt.Sprintf("typed table: size not a positive integer: %s: %s", val, err))
			}
			size = uint32(n)
		}

		var fixed bool
		if _, ok := tags["fixed"]; ok {
			fixed = true
		}

		notNull := true
		ftyp := fld.Type
		if ftyp.Kind() == reflect.Pointer {
			ftyp = ftyp.Elem()
			notNull = false
		}

		var vt types.ValueType
		switch ftyp.Kind() {
		case reflect.Bool:
			vt = types.BoolType
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			vt = types.Int64Type
			size = uint32(ftyp.Size())
		case reflect.Float32, reflect.Float64:
			vt = types.Float64Type
			size = 8
		case reflect.Array, reflect.Slice:
			elem := ftyp.Elem()
			if elem.Kind() != reflect.Uint8 || elem.Size() != 1 {
				panic(fmt.Sprintf("typed table: must slice or array of bytes: %s", elem))
			}
			vt = types.BytesType
			if ftyp.Kind() == reflect.Array {
				size = uint32(ftyp.Size())
				fixed = true
			}
		case reflect.String:
			vt = types.StringType
		default:
			panic(fmt.Sprintf("typed table: bad field type: %s: %s", fld.Name, ftyp.Kind()))
		}

		ct := types.ColumnType{
			Type:    vt,
			Size:    size,
			Fixed:   fixed,
			NotNull: notNull,
		}
		colTypes = append(colTypes, ct)

		if _, ok := tags["primary"]; ok {
			primary = append(primary, types.MakeColumnKey(types.ColumnNum(idx), false))
		}
	}

	return &typedTableInfo{
		colNames: colNames,
		colTypes: colTypes,
		primary:  primary,
	}
}

type typedTable struct {
	tbl storage.Table
}

type typedRows struct {
	rows storage.Rows
}

func openTypedTable(ctx context.Context, tx storage.Transaction, tid storage.TableId,
	row interface{}) (*typedTable, error) {

	// XXX
	return nil, nil
}

func createTypedTable(ctx context.Context, tx storage.Transaction, tid storage.TableId,
	tn types.TableName, row interface{}) error {

	// XXX
	return nil
}

func (tt *typedTable) rows(ctx context.Context, minRow, maxRow interface{},
	pred storage.Predicate) (*typedRows, error) {

	// XXX
	return nil, nil
}

func (tt *typedTable) update(ctx context.Context, rid storage.RowId, update interface{}) error {
	// XXX
	return nil
}

func (tt *typedTable) delete(ctx context.Context, rid storage.RowId) error {
	// XXX
	return nil
}

func (tt *typedTable) insert(ctx context.Context, row interface{}) error {
	// XXX
	return nil
}

func (tr *typedRows) next(ctx context.Context, row interface{}) error {
	// XXX
	return nil
}

func (tr *typedRows) current() (storage.RowId, error) {
	return tr.rows.Current()
}

func (tr *typedRows) close(ctx context.Context) error {
	err := tr.rows.Close(ctx)
	tr.rows = nil
	return err
}
