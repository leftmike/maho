package test

import (
	"testing"

	"github.com/leftmike/maho/pkg/storage"
)

func StoreTest(t *testing.T, store string, newStore func(dataDir string) (storage.Store, error)) {
	_, err := newStore("")
	if err != nil {
		t.Errorf("%s.NewStore() failed with %s", store, err)
	}
}
