package test

import (
	"testing"
)

func TestStore(t *testing.T, store string, newStore NewStore) {
	t.Helper()

	_, err := newStore(t.TempDir())
	if err != nil {
		t.Errorf("%s.NewStore() failed with %s", store, err)
	}
}
