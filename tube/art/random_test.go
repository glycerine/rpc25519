package art

import (
	"encoding/binary"
	"fmt"
	"math"
	mathrand2 "math/rand/v2"
	"sort"
	"testing"
)

func genKeys(n int, prefix string, seed0 uint16) []string {

	var seed [32]byte
	seed[0] = byte(seed0)
	seed[1] = byte(seed0 >> 8)
	prng := mathrand2.NewChaCha8(seed)

	// dedup
	seen := make(map[string]bool)

	var keys []string
	for range n {
		// use 0 and all range of bytes.
		length := 10 + unbiasedChoiceOf(prng, 5)
		b := make([]byte, length)
		for i := range b {
			b[i] = byte(unbiasedChoiceOf(prng, 256))
		}
		key := prefix + string(b)
		if !seen[key] {
			seen[key] = true
			keys = append(keys, key)
		}
	}
	//vv("genKeys n=%v, prefix='%v', seed0='%v'", n, prefix, seed0)
	//for i := range keys {
	//	fmt.Printf("%v\n", keys[i])
	//}
	return keys
}

// returns r >= 0
func pseudoRandNonNegInt64(prng *mathrand2.ChaCha8) (r int64) {
	b := make([]byte, 8)
	_, err := prng.Read(b)
	if err != nil {
		panic(err)
	}
	r = int64(binary.LittleEndian.Uint64(b))
	if r < 0 {
		if r == math.MinInt64 {
			return 0
		}
		r = -r
	}
	return r
}

// avoid modulo bias
func unbiasedChoiceOf(prng *mathrand2.ChaCha8, nChoices int64) (r int64) {
	if nChoices <= 1 {
		panic(fmt.Sprintf("nChoices must be in [2, MaxInt64]; we see %v", nChoices))
	}
	if nChoices == math.MaxInt64 {
		return pseudoRandNonNegInt64(prng)
	}

	// compute the last valid acceptable value,
	// possibly leaving a small window at the top of the
	// int64 range that will require drawing again.
	// we will accept all values <= redrawAbove and
	// modulo them by nChoices.
	redrawAbove := math.MaxInt64 - (((math.MaxInt64 % nChoices) + 1) % nChoices)
	// INVAR: redrawAbove % nChoices == (nChoices - 1).

	b := make([]byte, 8)

	for {
		_, err := prng.Read(b)
		if err != nil {
			panic(err)
		}
		r = int64(binary.LittleEndian.Uint64(b))
		if r < 0 {
			// there is 1 more negative integer than
			// positive integers in 2's complement
			// representation on integers, so the probability
			// is exactly 1/2 of entering here.
			//
			// Does this not bias
			// against 0 though? Yep.
			//
			// Without this next check,
			// 0 has probability 1/2^64. Whereas
			// every other positive integer has
			// probability 2/2^64... So
			// without this next line we are
			// (very subtlely) biased against zero.
			// To correct that, we
			// give 0 one more chance by
			// letting it have the last negative
			// number too, which we never
			// want to return anyway.
			if r == math.MinInt64 {
				return 0
			}
			r = -r
		}
		if r > redrawAbove {
			continue
		}
		return r % nChoices
	}
	panic("never reached")
	return r
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
		//vv("Ascend[i=%v] sees key '%v'", i, skey)
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
	keys := genKeys(500, "member_", uint16(0))
	verifyAscendDescend(t, keys)
}

func TestDeepTarget(t *testing.T) {
	return
	material := "\x00ecgl8lHmK9zw7smI392011-+_dOrxJn+or2humxOTGRHlYBl9\x00"
	deepTarget(t, []byte(material), 5)
}

func FuzzAscendDescend(f *testing.F) {
	vv("running FuzzAscendDescend with simple prefix variations.")

	f.Add(uint16(4), "seed", uint16(0))     // Boundary for Node4
	f.Add(uint16(4), "seed", uint16(255))   // Boundary for Node4
	f.Add(uint16(16), "seed", uint16(1))    // Boundary for Node16
	f.Add(uint16(16), "seed", uint16(254))  // Boundary for Node16
	f.Add(uint16(48), "seed", uint16(2))    // Boundary for Node48
	f.Add(uint16(48), "seed", uint16(253))  // Boundary for Node48
	f.Add(uint16(256), "seed", uint16(0))   // Boundary for Node256
	f.Add(uint16(256), "seed", uint16(255)) // Boundary for Node256

	f.Fuzz(func(t *testing.T, n uint16, prefix string, seed0 uint16) {
		count := int(n % 1000)

		keys := genKeys(count, prefix, seed0)
		verifyAscendDescend(t, keys)
	})
}

func FuzzNoPrefixAscendDescend(f *testing.F) {
	vv("running FuzzNoPrefixAscendDescend with no prefix")

	f.Add(uint16(4), uint16(0))     // Boundary for Node4
	f.Add(uint16(16), uint16(1))    // Boundary for Node16
	f.Add(uint16(48), uint16(254))  // Boundary for Node48
	f.Add(uint16(256), uint16(255)) // Boundary for Node256

	f.Fuzz(func(t *testing.T, n uint16, seed0 uint16) {
		count := int(n % 1000)

		// allow 0 length trees.

		keys := genKeys(count, "", seed0)
		verifyAscendDescend(t, keys)
	})
}

func deepTarget(t *testing.T, data []byte, keyLen byte) {
	// 1. Interpret the fuzz data as a list of keys
	// We split the random byte stream into chunks (keys).
	// This lets the fuzzer control the CONTENT
	// and the COUNT simultaneously.

	if keyLen == 0 || len(data) < 10 {
		t.Skip()
	}

	var keys []string
	// Simple separator strategy: fixed length keys.

	remaining := data

	// avoid duplicate keys
	seen := make(map[string]bool)

	for len(remaining) >= int(keyLen) {
		// Create a key from the byte stream
		currentKey := string(remaining[:keyLen])
		if !seen[currentKey] {
			seen[currentKey] = true
			keys = append(keys, currentKey)
		}

		// Advance
		remaining = remaining[keyLen:]

		// Limit to 1000 keys to keep test fast.
		if len(keys) >= 1000 {
			break
		}
	}

	//fmt.Printf("%#v\n", keys)

	// tree is built with random data now.
	verifyAscendDescend(t, keys)
}

func FuzzARTDeep(f *testing.F) {
	// Add some seed corpus
	f.Add([]byte("member_one\x00member_two\x00member_three\x00"), byte(10))
	f.Add([]byte("123\x00456\x00789\x00"), byte(5))

	f.Fuzz(deepTarget)
}
