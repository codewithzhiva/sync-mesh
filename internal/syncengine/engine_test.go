package syncengine

import (
	"path/filepath"
	"reflect"
	"testing"
)

func TestDuplicateAndRestartRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sync.wal")
	engine, _ := Open(path)
	mutation := Mutation{ID: "m1", ClientID: "phone", Key: "title", Kind: "lww_set", Value: "offline", Timestamp: 1}
	first, created, err := engine.Apply(mutation)
	if err != nil || !created {
		t.Fatalf("apply %v %v", created, err)
	}
	second, created, _ := engine.Apply(mutation)
	if created || first.Sequence != second.Sequence {
		t.Fatal("duplicate changed state")
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if reopened.Snapshot().Registers["title"] != "offline" {
		t.Fatal("restart lost value")
	}
}

func TestConvergenceAcrossArrivalOrders(t *testing.T) {
	mutations := []Mutation{{ID: "a", ClientID: "phone", Key: "title", Kind: "lww_set", Value: "A", Timestamp: 10}, {ID: "b", ClientID: "laptop", Key: "title", Kind: "lww_set", Value: "B", Timestamp: 12}, {ID: "c", ClientID: "tablet", Key: "tags", Kind: "gset_add", Value: "urgent", Timestamp: 2}, {ID: "d", ClientID: "phone", Key: "tags", Kind: "gset_add", Value: "personal", Timestamp: 3}}
	left, _ := Open(filepath.Join(t.TempDir(), "left.wal"))
	right, _ := Open(filepath.Join(t.TempDir(), "right.wal"))
	for _, m := range mutations {
		_, _, _ = left.Apply(m)
	}
	for i := len(mutations) - 1; i >= 0; i-- {
		_, _, _ = right.Apply(mutations[i])
	}
	a, b := left.Snapshot(), right.Snapshot()
	a.Cursor = 0
	b.Cursor = 0
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("did not converge %#v %#v", a, b)
	}
}

func TestClientCursorIsResumable(t *testing.T) {
	engine, _ := Open(filepath.Join(t.TempDir(), "sync.wal"))
	for _, id := range []string{"1", "2", "3"} {
		_, _, _ = engine.Apply(Mutation{ID: id, ClientID: "device", Key: id, Kind: "lww_set", Value: id, Timestamp: 1})
	}
	first, cursor := engine.ChangesSince(0, 2)
	second, final := engine.ChangesSince(cursor, 2)
	if len(first) != 2 || len(second) != 1 || final != 3 {
		t.Fatalf("pages %d %d cursor %d", len(first), len(second), final)
	}
}
