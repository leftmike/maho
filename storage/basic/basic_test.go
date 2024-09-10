package basic_test

import (
	"testing"

	"github.com/leftmike/maho/storage"
	"github.com/leftmike/maho/storage/basic"
	"github.com/leftmike/maho/storage/test"
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
	test.TestUpdate(t, "basic", newStore)
	test.TestTable(t, "basic", newStore)
}
