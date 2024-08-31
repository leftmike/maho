package engine

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"unicode"

	"github.com/leftmike/maho/pkg/storage"
	"github.com/leftmike/maho/pkg/types"
)

type typedInfo struct {
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

func makeTypedInfo(tid storage.TableId, tn types.TableName, st interface{}) *typedInfo {
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

	return &typedInfo{
		typ:      typ,
		tid:      tid,
		tn:       tn,
		colNames: colNames,
		colTypes: colTypes,
		primary:  primary,
		fldNames: fldNames,
	}
}

func (ti *typedInfo) toTableType() *TableType {
	return &TableType{
		Version:     1,
		ColumnNames: ti.colNames,
		ColumnTypes: ti.colTypes,
		Key:         ti.primary,
	}
}

func fieldToValue(ct types.ColumnType, fval reflect.Value) types.Value {
	if !ct.NotNull {
		if fval.IsNil() {
			return nil
		}
		if ct.Type != types.BytesType {
			fval = fval.Elem()
		}
	}

	switch ct.Type {
	case types.BoolType:
		return types.BoolValue(fval.Bool())
	case types.StringType:
		s := fval.String()
		if len(s) > int(ct.Size) {
			panic(fmt.Sprintf("typed table: bad string value: %d: %d", ct.Size, len(s)))
		}
		return types.StringValue(s)
	case types.BytesType:
		b := fval.Bytes()
		if len(b) > int(ct.Size) {
			panic(fmt.Sprintf("typed table: bad bytes value: %d: %d", ct.Size, len(b)))
		}
		return types.BytesValue(slices.Clone(b))
	case types.Float64Type:
		return types.Float64Value(fval.Float())
	case types.Int64Type:
		return types.Int64Value(fval.Int())
	default:
		panic(fmt.Sprintf("unexpected column type: %#v %d", ct, ct.Type))
	}
}

func (ti *typedInfo) structToRow(st interface{}) types.Row {
	if st == nil {
		return nil
	}

	typ := reflect.TypeOf(st)
	if typ.Kind() != reflect.Pointer {
		panic(fmt.Sprintf("typed table: must be pointer to a struct; got %#v", st))
	}

	typ = typ.Elem()
	val := reflect.ValueOf(st).Elem()
	if typ != ti.typ {
		panic(fmt.Sprintf("typed table: bad struct type: %s %s", typ, ti.typ))
	}

	row := make(types.Row, len(ti.colTypes))
	for cdx, ct := range ti.colTypes {
		row[cdx] = fieldToValue(ct, val.Field(cdx))
	}

	return row
}

func (ti *typedInfo) rowToStruct(row types.Row, st interface{}) {
	typ := reflect.TypeOf(st)
	if typ.Kind() != reflect.Pointer {
		panic(fmt.Sprintf("typed table: must be pointer to a struct; got %#v", st))
	}

	typ = typ.Elem()
	val := reflect.ValueOf(st).Elem()
	if typ != ti.typ {
		panic(fmt.Sprintf("typed table: bad struct type: %s %s", typ, ti.typ))
	}

	for cdx, ct := range ti.colTypes {
		fval := val.Field(cdx)
		if !ct.NotNull {
			if row[cdx] == nil {
				fval.SetZero()
				continue
			}
		}

		switch ct.Type {
		case types.BoolType:
			b, ok := row[cdx].(types.BoolValue)
			if !ok {
				panic(fmt.Sprintf("typed tabled: expected boolean value: %#v", row[cdx]))
			}
			if ct.NotNull {
				fval.SetBool(bool(b))
			} else {
				b := bool(b)
				fval.Set(reflect.ValueOf(&b))
			}
		case types.StringType:
			s, ok := row[cdx].(types.StringValue)
			if !ok {
				panic(fmt.Sprintf("typed tabled: expected string value: %#v", row[cdx]))
			}
			if ct.NotNull {
				fval.SetString(string(s))
			} else {
				s := string(s)
				fval.Set(reflect.ValueOf(&s))
			}
		case types.BytesType:
			b, ok := row[cdx].(types.BytesValue)
			if !ok {
				panic(fmt.Sprintf("typed tabled: expected bytes value: %#v", row[cdx]))
			}
			fval.SetBytes(slices.Clone(b))
		case types.Float64Type:
			f, ok := row[cdx].(types.Float64Value)
			if !ok {
				panic(fmt.Sprintf("typed tabled: expected float64 value: %#v", row[cdx]))
			}
			if ct.NotNull {
				fval.SetFloat(float64(f))
			} else {
				f := float64(f)
				fval.Set(reflect.ValueOf(&f))
			}
		case types.Int64Type:
			i, ok := row[cdx].(types.Int64Value)
			if !ok {
				panic(fmt.Sprintf("typed tabled: expected int64 value: %#v", row[cdx]))
			}
			if ct.NotNull {
				fval.SetInt(int64(i))
			} else {
				i := int64(i)
				fval.Set(reflect.ValueOf(&i))
			}
		default:
			panic(fmt.Sprintf("typed table: unexpected column type: %#v %d", ct, ct.Type))
		}
	}
}

func fieldNameToColumn(fldNames []string, name string) int {
	for idx, fn := range fldNames {
		if fn == name {
			return idx
		}
	}

	panic(fmt.Sprintf("typed table: field name not found: %s: %v", name, fldNames))
}

func (ti *typedInfo) structToColsVals(update interface{}) ([]types.ColumnNum, []types.Value) {
	var cols []types.ColumnNum
	var vals []types.Value

	typ := reflect.TypeOf(update)
	if typ.Kind() != reflect.Pointer {
		panic(fmt.Sprintf("typed table: must be pointer to a struct; got %#v", update))
	}

	typ = typ.Elem()
	val := reflect.ValueOf(update).Elem()
	for idx := 0; idx < typ.NumField(); idx += 1 {
		cdx := fieldNameToColumn(ti.fldNames, typ.Field(idx).Name)
		cols = append(cols, types.ColumnNum(cdx))
		vals = append(vals, fieldToValue(ti.colTypes[cdx], val.Field(idx)))
	}

	return cols, vals
}

type typedTable struct {
	tbl storage.Table
	ti  *typedInfo
}

type typedRows struct {
	rows storage.Rows
	ti   *typedInfo
}

type typedRowRef struct {
	rr storage.RowRef
	ti *typedInfo
}

func openTypedTable(ctx context.Context, tx storage.Transaction, ti *typedInfo) (*typedTable,
	error) {

	tbl, err := tx.OpenTable(ctx, ti.tid, ti.tn, ti.colNames, ti.colTypes, ti.primary)
	if err != nil {
		return nil, err
	}

	return &typedTable{
		tbl: tbl,
		ti:  ti,
	}, nil
}

func createTypedTable(ctx context.Context, tx storage.Transaction, ti *typedInfo) error {
	return tx.CreateTable(ctx, ti.tid, ti.tn, ti.colNames, ti.colTypes, ti.primary)
}

func (tt *typedTable) rows(ctx context.Context, minSt, maxSt interface{}) (*typedRows, error) {
	rows, err := tt.tbl.Rows(ctx, nil, tt.ti.structToRow(minSt), tt.ti.structToRow(maxSt), nil)
	if err != nil {
		return nil, err
	}

	return &typedRows{
		rows: rows,
		ti:   tt.ti,
	}, nil
}

func (tt *typedTable) insert(ctx context.Context, structs ...interface{}) error {
	var rows []types.Row
	for _, st := range structs {
		rows = append(rows, tt.ti.structToRow(st))
	}

	return tt.tbl.Insert(ctx, rows)
}

func (tt *typedTable) lookup(ctx context.Context, st interface{}) error {
	tr, err := tt.rows(ctx, st, st)
	if err != nil {
		return err
	}
	defer func() {
		tr.close(ctx)
	}()

	return tr.next(ctx, st)
}

func (tr *typedRows) next(ctx context.Context, st interface{}) error {
	row, err := tr.rows.Next(ctx)
	if err != nil {
		return err
	}

	tr.ti.rowToStruct(row, st)
	return nil
}

func (tr *typedRows) current() (*typedRowRef, error) {
	rr, err := tr.rows.Current()
	if err != nil {
		return nil, err
	}

	return &typedRowRef{
		rr: rr,
		ti: tr.ti,
	}, nil
}

func (tr *typedRows) close(ctx context.Context) error {
	err := tr.rows.Close(ctx)
	tr.rows = nil
	return err
}

func (trr *typedRowRef) update(ctx context.Context, update interface{}) error {
	cols, vals := trr.ti.structToColsVals(update)
	return trr.rr.Update(ctx, cols, vals)
}

func (trr *typedRowRef) delete(ctx context.Context) error {
	return trr.rr.Delete(ctx)
}
