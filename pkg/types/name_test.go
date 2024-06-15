package types_test

import (
	"testing"

	"github.com/leftmike/maho/pkg/types"
)

func TestName(t *testing.T) {
	tn := types.TableName{
		Database: types.ID("database", false),
		Schema:   types.ID("schema", false),
		Table:    types.ID("table", false),
	}

	s := tn.String()
	if s != "database.schema.table" {
		t.Errorf("%#v.String() got %s want database.schema.table", tn, s)
	}

	sn := tn.SchemaName()
	s = sn.String()

	if s != "database.schema" {
		t.Errorf("%#v.String() got %s want database.schema", sn, s)
	}

	tn = types.TableName{
		Schema: types.ID("schema", false),
		Table:  types.ID("table", false),
	}

	s = tn.String()
	if s != "schema.table" {
		t.Errorf("%#v.String() got %s want schema.table", tn, s)
	}

	sn = tn.SchemaName()
	s = sn.String()

	if s != "schema" {
		t.Errorf("%#v.String() got %s want schema", sn, s)
	}

	tn = types.TableName{
		Table: types.ID("table", false),
	}

	s = tn.String()
	if s != "table" {
		t.Errorf("%#v.String() got %s want table", tn, s)
	}
}
