package engine

import (
	"testing"

	"github.com/leftmike/maho/storage/basic"
)

func TestInit(t *testing.T) {
	dataDir := t.TempDir()
	store, err := basic.NewStore(dataDir)
	if err != nil {
		t.Fatalf("NewStore(%s) failed with %s", dataDir, err)
	}

	err = Init(store)
	if err != nil {
		t.Errorf("Init() failed with %s", err)
	}
}
