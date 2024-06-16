package basic_test

import (
	"testing"

	"github.com/leftmike/maho/pkg/storage"
	"github.com/leftmike/maho/pkg/storage/basic"
	"github.com/leftmike/maho/pkg/storage/test"
)

func TestStore(t *testing.T) {
	test.StoreTest(t, "basic", func(dataDir string) (storage.Store, error) {
		return basic.NewStore(dataDir)
	})
}
