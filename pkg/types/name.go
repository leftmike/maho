package types

import (
	"fmt"
)

type TableName struct {
	Database Identifier
	Schema   Identifier
	Table    Identifier
}

type SchemaName struct {
	Database Identifier
	Schema   Identifier
}

func (tn TableName) String() string {
	if tn.Database == 0 {
		if tn.Schema == 0 {
			return tn.Table.String()
		}
		return fmt.Sprintf("%s.%s", tn.Schema, tn.Table)
	}
	return fmt.Sprintf("%s.%s.%s", tn.Database, tn.Schema, tn.Table)
}

func (tn TableName) SchemaName() SchemaName {
	return SchemaName{tn.Database, tn.Schema}
}

func (sn SchemaName) String() string {
	if sn.Database == 0 {
		return sn.Schema.String()
	}
	return fmt.Sprintf("%s.%s", sn.Database, sn.Schema)
}
