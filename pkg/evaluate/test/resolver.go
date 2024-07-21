package test

import (
	"github.com/leftmike/maho/pkg/types"
)

type Resolver struct {
	Database types.Identifier
	Schema   types.Identifier
}

func (r Resolver) ResolveTable(tn types.TableName) types.TableName {
	if tn.Database == 0 {
		tn.Database = r.Database
		if tn.Schema == 0 {
			tn.Schema = r.Schema
		}
	}
	return tn
}

func (r Resolver) ResolveSchema(sn types.SchemaName) types.SchemaName {
	if sn.Database == 0 {
		sn.Database = r.Database
	}
	return sn
}
