package engine

import (
	"context"
	"testing"

	"github.com/leftmike/maho/storage/basic"
)

type begin struct{}

type commit struct{}

type rollback struct{}

type nextTableId struct {
	tid int64
}

func TestNextTableId(t *testing.T) {
	cases := []interface{}{
		begin{},
		nextTableId{
			tid: int64(maxReservedTableId + 1),
		},
		commit{},
		begin{},
		nextTableId{
			tid: int64(maxReservedTableId + 2),
		},
		nextTableId{
			tid: int64(maxReservedTableId + 3),
		},
		nextTableId{
			tid: int64(maxReservedTableId + 4),
		},
		nextTableId{
			tid: int64(maxReservedTableId + 5),
		},
		rollback{},
		begin{},
		nextTableId{
			tid: int64(maxReservedTableId + 2),
		},
		nextTableId{
			tid: int64(maxReservedTableId + 3),
		},
		nextTableId{
			tid: int64(maxReservedTableId + 4),
		},
		commit{},
		begin{},
		nextTableId{
			tid: int64(maxReservedTableId + 5),
		},
		rollback{},
	}

	s := t.TempDir()
	store, err := basic.NewStore(s)
	if err != nil {
		t.Fatalf("NewStore(%s) failed with %s", s, err)
	}
	err = Init(store)
	if err != nil {
		t.Fatalf("Init() failed with %s", err)
	}

	ctx := context.Background()
	eng := NewEngine(store)
	var tx *transaction
	for _, c := range cases {
		switch c := c.(type) {
		case begin:
			tx = eng.Begin().(*transaction)
		case commit:
			err := tx.Commit(ctx)
			if err != nil {
				t.Errorf("Commit() failed with %s", err)
			}
			tx = nil
		case rollback:
			err := tx.Rollback()
			if err != nil {
				t.Errorf("Rollback() failed with %s", err)
			}
			tx = nil
		case nextTableId:
			tid, err := tx.nextTableId(ctx)
			if err != nil {
				t.Errorf("nextTableId() failed with %s", err)
			} else if tid != c.tid {
				t.Errorf("nextTableId() got %d want %d", tid, c.tid)
			}
		}
	}
}
