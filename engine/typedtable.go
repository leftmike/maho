package engine

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"slices"

	"github.com/leftmike/maho/storage"
	"github.com/leftmike/maho/types"
)

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

func (ti *TypedInfo) structToRow(st interface{}) types.Row {
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

func (ti *TypedInfo) RowToStruct(row types.Row, st interface{}) {
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

func (ti *TypedInfo) structToColsVals(update interface{}) ([]types.ColumnNum, []types.Value) {
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

func CreateTypedTable(ctx context.Context, tx storage.Transaction, ti *TypedInfo) error {
	return tx.CreateTable(ctx, ti.tid, ti.tn, ti.colNames, ti.colTypes, ti.primary)
}

func TypedTableInsert(ctx context.Context, tx storage.Transaction, ti *TypedInfo,
	structs ...interface{}) error {

	tbl, err := tx.OpenTable(ctx, ti.tid, ti.tn, ti.colNames, ti.colTypes, ti.primary)
	if err != nil {
		return err
	}

	var rows []types.Row
	for _, st := range structs {
		rows = append(rows, ti.structToRow(st))
	}

	return tbl.Insert(ctx, rows)
}

func typedTableSelect(ctx context.Context, tx storage.Transaction, ti *TypedInfo,
	minSt, maxSt interface{}, fn func(rows storage.Rows, row types.Row) error) error {

	tbl, err := tx.OpenTable(ctx, ti.tid, ti.tn, ti.colNames, ti.colTypes, ti.primary)
	if err != nil {
		return err
	}
	rows, err := tbl.Rows(ctx, nil, ti.structToRow(minSt), ti.structToRow(maxSt), nil)
	if err != nil {
		return err
	}
	defer func() {
		rows.Close(ctx)
	}()

	for {
		row, err := rows.Next(ctx)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		err = fn(rows, row)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
	}

	return nil
}

func TypedTableLookup(ctx context.Context, tx storage.Transaction, ti *TypedInfo,
	st interface{}) error {

	var found bool
	err := typedTableSelect(ctx, tx, ti, st, st,
		func(rows storage.Rows, row types.Row) error {
			if found {
				panic("typed table: lookup returned more than one row")
			}
			found = true

			ti.RowToStruct(row, st)
			return nil
		})
	if err != nil {
		return err
	} else if !found {
		return io.EOF
	}
	return nil
}

func TypedTableSelect(ctx context.Context, tx storage.Transaction, ti *TypedInfo,
	minSt, maxSt interface{}, fn func(row types.Row) error) error {

	return typedTableSelect(ctx, tx, ti, minSt, maxSt,
		func(rows storage.Rows, row types.Row) error {
			return fn(row)
		})
}

func TypedTableUpdate(ctx context.Context, tx storage.Transaction, ti *TypedInfo,
	minSt, maxSt interface{}, fn func(row types.Row) (interface{}, error)) error {

	return typedTableSelect(ctx, tx, ti, minSt, maxSt,
		func(rows storage.Rows, row types.Row) error {
			update, err := fn(row)
			if err != nil {
				return err
			}

			if update != nil {
				rr, err := rows.Current()
				if err != nil {
					return err
				}

				cols, vals := ti.structToColsVals(update)
				return rr.Update(ctx, cols, vals)
			}
			return nil
		})
}

func TypedTableDelete(ctx context.Context, tx storage.Transaction, ti *TypedInfo,
	minSt, maxSt interface{}, fn func(row types.Row) (bool, error)) error {

	return typedTableSelect(ctx, tx, ti, minSt, maxSt,
		func(rows storage.Rows, row types.Row) error {
			delete, err := fn(row)
			if err != nil {
				return err
			}

			if delete {
				rr, err := rows.Current()
				if err != nil {
					return err
				}

				return rr.Delete(ctx)
			}
			return nil
		})
}
