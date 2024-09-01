package engine

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"slices"

	"github.com/leftmike/maho/pkg/storage"
	"github.com/leftmike/maho/pkg/types"
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

func (ti *TypedInfo) rowToStruct(row types.Row, st interface{}) {
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

type TypedTable struct {
	tbl storage.Table
	ti  *TypedInfo
}

type TypedRows struct {
	rows storage.Rows
	ti   *TypedInfo
}

type TypedRowRef struct {
	rr storage.RowRef
	ti *TypedInfo
}

func OpenTypedTable(ctx context.Context, tx storage.Transaction, ti *TypedInfo) (*TypedTable,
	error) {

	tbl, err := tx.OpenTable(ctx, ti.tid, ti.tn, ti.colNames, ti.colTypes, ti.primary)
	if err != nil {
		return nil, err
	}

	return &TypedTable{
		tbl: tbl,
		ti:  ti,
	}, nil
}

func CreateTypedTable(ctx context.Context, tx storage.Transaction, ti *TypedInfo) error {
	return tx.CreateTable(ctx, ti.tid, ti.tn, ti.colNames, ti.colTypes, ti.primary)
}

func (tt *TypedTable) TypedInfo() *TypedInfo {
	return tt.ti
}

func (tt *TypedTable) Rows(ctx context.Context, minSt, maxSt interface{}) (*TypedRows, error) {
	rows, err := tt.tbl.Rows(ctx, nil, tt.ti.structToRow(minSt), tt.ti.structToRow(maxSt), nil)
	if err != nil {
		return nil, err
	}

	return &TypedRows{
		rows: rows,
		ti:   tt.ti,
	}, nil
}

func (tt *TypedTable) Insert(ctx context.Context, structs ...interface{}) error {
	var rows []types.Row
	for _, st := range structs {
		rows = append(rows, tt.ti.structToRow(st))
	}

	return tt.tbl.Insert(ctx, rows)
}

func (tt *TypedTable) Lookup(ctx context.Context, st interface{}) error {
	tr, err := tt.Rows(ctx, st, st)
	if err != nil {
		return err
	}
	defer func() {
		tr.Close(ctx)
	}()

	err = tr.Next(ctx, st)
	if err != nil {
		return err
	}

	_, err = tr.rows.Next(ctx)
	if err == nil {
		panic("typed table: lookup returned more than one row")
	} else if err != io.EOF {
		return err
	}
	return nil
}

func (tr *TypedRows) Next(ctx context.Context, st interface{}) error {
	row, err := tr.rows.Next(ctx)
	if err != nil {
		return err
	}

	tr.ti.rowToStruct(row, st)
	return nil
}

func (tr *TypedRows) Current() (*TypedRowRef, error) {
	rr, err := tr.rows.Current()
	if err != nil {
		return nil, err
	}

	return &TypedRowRef{
		rr: rr,
		ti: tr.ti,
	}, nil
}

func (tr *TypedRows) Close(ctx context.Context) error {
	err := tr.rows.Close(ctx)
	tr.rows = nil
	return err
}

func (trr *TypedRowRef) Update(ctx context.Context, update interface{}) error {
	cols, vals := trr.ti.structToColsVals(update)
	return trr.rr.Update(ctx, cols, vals)
}

func (trr *TypedRowRef) Delete(ctx context.Context) error {
	return trr.rr.Delete(ctx)
}
