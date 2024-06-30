package basic_test

import (
	"testing"

	"github.com/leftmike/maho/pkg/storage"
	"github.com/leftmike/maho/pkg/storage/basic"
	"github.com/leftmike/maho/pkg/storage/test"
)

func TestStore(t *testing.T) {
	newStore := func(dataDir string) (storage.Store, error) {
		return basic.NewStore(dataDir)
	}

	test.TestStore(t, "basic", newStore)
	test.TestCreateTable(t, "basic", newStore)
	test.TestDropTable(t, "basic", newStore)
	test.TestRows(t, "basic", newStore)
	test.TestInsert(t, "basic", newStore)
	test.TestDelete(t, "basic", newStore)
}
