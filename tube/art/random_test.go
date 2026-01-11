package art

import (
	"math/rand/v2"
	"sort"
	"testing"
)

func genKeys(n int, prefix string) []string {
	keys := make([]string, n)
	for i := 0; i < n; i++ {
		// Generate random suffix
		const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"
		length := 10 + rand.IntN(20)
		b := make([]byte, length)
		for i := range b {
			b[i] = charset[rand.IntN(len(charset))]
		}
		keys[i] = prefix + string(b)
	}
	// Dedup
	unique := make(map[string]bool)
	var deduped []string
	for _, k := range keys {
		if !unique[k] {
			unique[k] = true
			deduped = append(deduped, k)
		}
	}
	return deduped
}

func verifyAscendDescend(t *testing.T, keys []string) {
	sorted := make([]string, len(keys))
	copy(sorted, keys)
	sort.Strings(sorted)

	reversed := make([]string, len(keys))
	copy(reversed, keys)
	sort.Sort(sort.Reverse(sort.StringSlice(reversed)))

	tree := NewArtTree()
	for _, key := range keys {
		tree.Insert([]byte(key), nil, "")
	}

	i := 0
	// check Ascend
	for key, lf := range Ascend(tree, nil, nil) {
		_ = lf
		skey := string(key)
		if skey != sorted[i] {
			t.Fatalf("Ascend i=%v problem, want '%v', got '%v'", i, sorted[i], skey)
		}
		i++
	}
	if i != len(keys) {
		t.Fatalf("wanted %v keys back from Ascend, got %v", len(keys), i)
	}

	// verify that integer indexing works.
	i = 0
	for j := len(keys) - 1; j >= 0; j-- {
		lf, ok := tree.At(j)
		if !ok {
			break
		}
		skey := string(lf.Key)
		if skey != reversed[i] {
			t.Fatalf("At indexing in reverse j=%v problem, want '%v', got '%v'", j, reversed[i], skey)
		}
		i++
	}

	i = 0
	for key, lf := range Descend(tree, nil, nil) {
		_ = lf
		skey := string(key)
		if skey != reversed[i] {
			t.Fatalf("Descend i=%v problem, want '%v', got '%v'", i, reversed[i], skey)
		}
		i++
	}
	if i != len(keys) {
		t.Fatalf("wanted %v keys back from Descend, got %v", len(keys), i)
	}

	tree2 := tree.Clone() // for deleting the evens

	// verify delete in the middle of iteration works in reverse, while deleting the odds.
	i = 0
	for key, lf := range Descend(tree, nil, nil) {
		_ = lf
		skey := string(key)
		if skey != reversed[i] {
			t.Fatalf("Descend with Delete oods: i=%v problem, want '%v', got '%v'. tree.Len=%v", i, reversed[i], skey, tree.Size())
		}
		i++
		// remove the odd ones as we go.
		if i%2 == 1 {
			tree.Remove(Key(skey))
		}
	}
	if i != len(keys) {
		t.Fatalf("wanted %v keys back from Descend with delete odds, got %v", len(keys), i)
	}

	// verify delete in the middle of iteration works in reverse, while deleting the evens.
	i = 0
	for key, lf := range Descend(tree2, nil, nil) {
		_ = lf
		skey := string(key)
		if skey != reversed[i] {
			t.Fatalf("Descend with Delete (deleting evens): i=%v problem, want '%v', got '%v'. tree.Len=%v", i, reversed[i], skey, tree.Size())
		}
		i++
		// remove the evens as we go.
		if i%2 == 0 {
			tree.Remove(Key(skey))
		}
	}
	if i != len(keys) {
		t.Fatalf("wanted %v keys back from Descend (deleting evens), got %v", len(keys), i)
	}

}

func TestRandomizedAscendDescend(t *testing.T) {
	// 500 keys with shared prefix
	keys := genKeys(500, "member_")
	verifyAscendDescend(t, keys)
}

func FuzzAscendDescend(f *testing.F) {
	f.Add(500, "fuzz_prefix_")
	f.Fuzz(func(t *testing.T, n int, prefix string) {
		if n <= 0 {
			n = 10
		}
		if n > 1000 {
			n = 1000
		}
		keys := genKeys(n, prefix)
		verifyAscendDescend(t, keys)
	})
}
