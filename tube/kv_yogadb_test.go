package tube

import (
	"bytes"
	"testing"

	"github.com/glycerine/rpc25519/tube/leased"
)

func TestKVStoreYogaDBBasicAndRename(t *testing.T) {
	store := newKVStore()
	defer store.Close()

	if err := store.MakeTable("alpha"); err != nil {
		t.Fatal(err)
	}
	leaf := leased.NewLeaf("key1", []byte("value1"), "bytes")
	leaf.Version = 7
	if err := store.PutLeaf("alpha", leaf); err != nil {
		t.Fatal(err)
	}

	got, err := store.GetLeaf("alpha", "key1", false)
	if err != nil {
		t.Fatal(err)
	}
	if got.Key != "key1" || got.Vtype != "bytes" || got.Version != 7 || !bytes.Equal(got.Value, []byte("value1")) {
		t.Fatalf("unexpected leaf after put: %#v", got)
	}

	if err := store.RenameTable("alpha", "beta"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetLeaf("alpha", "key1", false); err != ErrKeyNotFound {
		t.Fatalf("expected old table lookup to miss after rename, got %v", err)
	}
	got, err = store.GetLeaf("beta", "key1", false)
	if err != nil {
		t.Fatal(err)
	}
	if got.Key != "key1" || !bytes.Equal(got.Value, []byte("value1")) {
		t.Fatalf("unexpected leaf after rename: %#v", got)
	}

	keys, err := store.ShowKeys("beta")
	if err != nil {
		t.Fatal(err)
	}
	if keys.Size() != 1 {
		t.Fatalf("expected one key after rename, got %d", keys.Size())
	}
	for k := range keys.Ascend() {
		if k != "key1" {
			t.Fatalf("unexpected key after rename: %q", k)
		}
	}
}

func TestKVStoreYogaDBSnapshotClone(t *testing.T) {
	store := newKVStore()
	defer store.Close()

	if err := store.PutLeaf("alpha", leased.NewLeaf("a", []byte("1"), "")); err != nil {
		t.Fatal(err)
	}
	if err := store.PutLeaf("beta", leased.NewLeaf("b", []byte("2"), "")); err != nil {
		t.Fatal(err)
	}

	clone := store.clone()
	defer clone.Close()

	if !clone.SnapshotValid {
		t.Fatalf("expected clone to keep portable snapshot fields")
	}
	if len(clone.LeafTables) != len(clone.Leafz) {
		t.Fatalf("leaf table count mismatch: %d table names for %d leaves", len(clone.LeafTables), len(clone.Leafz))
	}
	for _, tc := range []struct {
		table Key
		key   Key
		want  []byte
	}{
		{"alpha", "a", []byte("1")},
		{"beta", "b", []byte("2")},
	} {
		got, err := clone.GetLeaf(tc.table, tc.key, false)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got.Value, tc.want) {
			t.Fatalf("clone %q:%q value = %q, want %q", tc.table, tc.key, got.Value, tc.want)
		}
	}
}
