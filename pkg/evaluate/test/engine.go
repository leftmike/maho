package test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/leftmike/maho/pkg/engine"
	"github.com/leftmike/maho/pkg/storage"
	"github.com/leftmike/maho/pkg/types"
)

func NewMockEngine(t *testing.T, expect []interface{}) engine.Engine {
	return &mock{
		t:      t,
		expect: expect,
	}
}

func NewMockTransaction(t *testing.T, expect []interface{}) engine.Transaction {
	return &mock{
		t:      t,
		expect: expect,
	}
}

type mock struct {
	t      *testing.T
	expect []interface{}
}

type CreateDatabase struct {
	Database types.Identifier
	Options  storage.OptionsMap
}

func (m *mock) CreateDatabase(dn types.Identifier, opts storage.OptionsMap) error {
	expect, ok := m.next("create database")
	if !ok {
		return nil
	}

	cd, ok := expect.(CreateDatabase)
	if !ok {
		m.t.Errorf("mock: got create database want %#v", expect)
		return nil
	}
	if dn != cd.Database {
		m.t.Errorf("mock: create database: dn: got %s want %s", dn, cd.Database)
	}
	if !reflect.DeepEqual(opts, cd.Options) {
		m.t.Errorf("mock: create database: opts: got %v want %v", opts, cd.Options)
	}
	return nil
}

type DropDatabase struct {
	Database types.Identifier
	IfExists bool
}

func (m *mock) DropDatabase(dn types.Identifier, ifExists bool) error {
	expect, ok := m.next("drop database")
	if !ok {
		return nil
	}

	dd, ok := expect.(DropDatabase)
	if !ok {
		m.t.Errorf("mock: got drop database want %#v", expect)
		return nil
	}
	if dn != dd.Database {
		m.t.Errorf("mock: drop database: dn: got %s want %s", dn, dd.Database)
	}
	if ifExists != dd.IfExists {
		m.t.Errorf("mock: drop database: if exists: got %v want %v", ifExists, dd.IfExists)
	}
	return nil
}

type Begin struct{}

func (m *mock) next(what string) (interface{}, bool) {
	if len(m.expect) == 0 {
		m.t.Errorf("mock: got %s want nothing", what)
		return nil, false
	}
	expect := m.expect[0]
	m.expect = m.expect[1:]
	return expect, true
}

func (m *mock) Begin() engine.Transaction {
	expect, ok := m.next("begin")
	if !ok {
		return nil
	}

	_, ok = expect.(Begin)
	if !ok {
		m.t.Errorf("mock: got begin want %#v", expect)
		return nil
	}
	return m
}

type Commit struct{}

func (m *mock) Commit(ctx context.Context) error {
	expect, ok := m.next("commit")
	if !ok {
		return nil
	}

	_, ok = expect.(Commit)
	if !ok {
		m.t.Errorf("mock: got commit want %#v", expect)
		return nil
	}
	return nil
}

type Rollback struct{}

func (m *mock) Rollback() error {
	expect, ok := m.next("rollback")
	if !ok {
		return nil
	}

	_, ok = expect.(Rollback)
	if !ok {
		m.t.Errorf("mock: got rollback want %#v", expect)
		return nil
	}
	return nil
}

type CreateSchema struct {
	Schema types.SchemaName
	Fail   bool
}

func (m *mock) CreateSchema(ctx context.Context, sn types.SchemaName) error {
	expect, ok := m.next("create schema")
	if !ok {
		return nil
	}

	cs, ok := expect.(CreateSchema)
	if !ok {
		m.t.Errorf("mock: got create schema want %#v", expect)
		return nil
	}
	if sn != cs.Schema {
		m.t.Errorf("mock: create schema: sn: got %s want %s", sn, cs.Schema)
	}
	if cs.Fail {
		return errors.New("mock: create schema failed")
	}
	return nil
}

func (m *mock) DropSchema(ctx context.Context, ifExists bool, sn types.SchemaName) error {
	// XXX
	return nil
}

func (m *mock) ListSchemas(ctx context.Context, dn types.Identifier) ([]types.Identifier, error) {
	// XXX
	return nil, nil
}

func (m *mock) LookupTable(ctx context.Context, tn types.TableName) (engine.Table, error) {
	// XXX
	return nil, nil
}

func (m *mock) CreateTable(ctx context.Context, tn types.TableName,
	colNames []types.Identifier, colTypes []types.ColumnType) error {

	// XXX
	return nil
}

func (m *mock) DropTable(ctx context.Context, tn types.TableName) error {
	// XXX
	return nil
}

func (m *mock) ListTables(ctx context.Context, sn types.SchemaName) ([]types.Identifier, error) {
	// XXX
	return nil, nil
}
