package types_test

import (
	"testing"

	"github.com/leftmike/maho/pkg/types"
)

func TestName(t *testing.T) {
	tn := types.TableName{
		Database: types.ID("db", false),
		Schema:   types.ID("scm", false),
		Table:    types.ID("tbl", false),
	}

	s := tn.String()
	if s != "db.scm.tbl" {
		t.Errorf("%#v.String() got %s want db.scm.tbl", tn, s)
	}

	sn := tn.SchemaName()
	s = sn.String()

	if s != "db.scm" {
		t.Errorf("%#v.String() got %s want db.scm", sn, s)
	}

	tn = types.TableName{
		Schema: types.ID("scm", false),
		Table:  types.ID("tbl", false),
	}

	s = tn.String()
	if s != "scm.tbl" {
		t.Errorf("%#v.String() got %s want scm.tbl", tn, s)
	}

	sn = tn.SchemaName()
	s = sn.String()

	if s != "scm" {
		t.Errorf("%#v.String() got %s want scm", sn, s)
	}

	tn = types.TableName{
		Table: types.ID("tbl", false),
	}

	s = tn.String()
	if s != "tbl" {
		t.Errorf("%#v.String() got %s want tbl", tn, s)
	}
}
