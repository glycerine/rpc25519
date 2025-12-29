package art

import (
	"bytes"
	"testing"
)

func TestBinaryKeyHandling(t *testing.T) {
	tree := NewArtTree()

	// Test keys with null bytes
	key1 := []byte{0, 1, 2, 0, 3}
	key2 := []byte{0, 1, 2, 0, 4}
	key3 := []byte{0, 1, 2, 0} // Prefix of key1 and key2
	key4 := []byte{0, 1}       // Prefix of all of them.

	// Insert tree
	tree.Insert(key1, []byte("value1"), "")
	tree.Insert(key2, []byte("value2"), "")
	tree.Insert(key3, []byte("value3"), "")
	tree.Insert(key4, []byte("value4"), "")

	if tree.Size() != 4 {
		t.Errorf("Expected tree size 4, got %d", tree.Size())
	}

	// Search test
	if v, _, ok, _ := tree.FindExact(key1); !ok || string(v) != "value1" {
		t.Errorf("Expected value1 for key1, got %#v", v)
	}
	if v, _, ok, _ := tree.FindExact(key2); !ok || string(v) != "value2" {
		t.Errorf("Expected value2 for key2, got %v", v)
	}
	if v, _, ok, _ := tree.FindExact(key3); !ok || string(v) != "value3" {
		t.Errorf("Expected value3 for key3, got %v", v)
	}
	if v, _, ok, _ := tree.FindExact(key4); !ok || string(v) != "value4" {
		t.Errorf("Expected value4 for key4, got %v", v)
	}

	// Prefix test
	var found []string
	var keys []string
	it := tree.Iter(nil, nil)
	for it.Next() {
		found = append(found, string(it.Value()))
		keys = append(keys, string(it.Key()))
	}
	//vv("keys = '%#v'", keys)
	//vv("found = '%#v'", found)

	if len(found) != 4 {
		t.Errorf("Expected 4 matches for prefix scan, got %d", len(found))
	}
	if len(keys) != 4 {
		t.Errorf("Expected 4 matches for prefix scan, got %d", len(keys))
	}

	// Remove test
	if gone, _ := tree.Remove(key1); !gone {
		t.Error("Failed to delete key1")
	}

	//vv("about to search for key1 = '%v'", key1)
	if v, _, ok, _ := tree.FindExact(key1); ok {
		t.Errorf("key1 still found after deletion: ok = %v; v = '%#v'", ok, v)
	}
	if tree.Size() != 3 {
		t.Errorf("Expected tree size 2 after deletion, got %d", tree.Size())
	}
}

func TestEmptyKeyHandling(t *testing.T) {
	tree := NewArtTree()

	// Test empty key
	emptyKey := []byte{}
	tree.Insert(emptyKey, []byte("empty"), "")

	if v, _, ok, _ := tree.FindExact(emptyKey); !ok || string(v) != "empty" {
		t.Errorf("Expected 'empty' for empty key, got %v", v)
	}

	if gone, _ := tree.Remove(emptyKey); !gone {
		t.Error("Failed to delete empty key")
	}
}

func TestLongBinaryKeys(t *testing.T) {
	tree := NewArtTree()

	// Create long keys with binary content
	key1 := bytes.Repeat([]byte{1, 0, 255}, 100)
	key2 := bytes.Repeat([]byte{1, 0, 254}, 100)

	tree.Insert(key1, []byte("long1"), "")
	tree.Insert(key2, []byte("long2"), "")

	if v, _, ok, _ := tree.FindExact(key1); !ok || string(v) != "long1" {
		t.Errorf("Expected 'long1' for key1, got %v", v)
	}
	if v, _, ok, _ := tree.FindExact(key2); !ok || string(v) != "long2" {
		t.Errorf("Expected 'long2' for key2, got %v", v)
	}
}
