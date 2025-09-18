package art

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	mathrand2 "math/rand/v2"
	"os"
	"sort"
	"strings"
	"testing"
)

var _ = strings.Contains

var (
	emptyKey = []byte{}
)

// Inserting a single value into the tree and removing it should result in a nil tree root.
func TestInsertAndRemove1(t *testing.T) {
	tree := NewArtTree()

	by := &TestBytes{Slc: []byte("data")}

	//vv("staring sz = %v", tree.Size())
	if updated := tree.Insert([]byte("test"), by.Slc); updated {
		t.Fatalf("why no fresh insert? sz = '%v'", tree.Size())
	}
	sz := tree.Size()
	//vv("sz = %v", sz)
	if sz != 1 {
		t.Fatalf("expected size 1, got %v", sz)
	}
	gone, goner := tree.Remove([]byte("test"))
	if !gone {
		t.Fatalf("why not gone? goner = '%v'", goner)
	}

	if tree.Size() != 0 {
		t.Fatalf("Unexpected tree size after inserting and removing: %v", tree.Size())
	}

}

// Inserting Two values into the tree and removing one of them
// should result in a tree root of type LEAF
func TestInsert2AndRemove1AndRootShouldBeLeafNode(t *testing.T) {
	tree := NewArtTree()

	by := &TestBytes{Slc: []byte("data")}

	tree.Insert([]byte("test"), by.Slc)  // []byte("data"))
	tree.Insert([]byte("test2"), by.Slc) // []byte("data"))

	tree.Remove([]byte("test"))

	if tree.Size() != 1 {
		t.Error("Unexpected tree size after inserting and removing")
	}
}

// After Inserting many values into the tree, we should be able to remove them all
// And expect nothing to exist in the tree.
func TestInsertManyWordsAndRemoveThemAll(t *testing.T) {
	tree := NewArtTree()

	file, err := os.Open("assets/words.txt")
	if err != nil {
		t.Error("Couldn't open words.txt")
	}

	defer file.Close()

	reader := bufio.NewReader(file)

	words := make(map[string]bool)
	var word_order []string // the del order makes for red/green test
	i := 0
	for ; ; i++ {
		if line, err := reader.ReadBytes('\n'); err != nil {
			break
		} else {
			by := &TestBytes{Slc: []byte(line)}
			if updated := tree.Insert([]byte(line), by.Slc); updated {
				panic(fmt.Sprintf("redundant '%v'", string(line)))
			}
			sline := string(line)
			words[sline] = true
			word_order = append(word_order, sline)
			sz := tree.Size()
			if sz != len(words) {
				panic(fmt.Sprintf("Insert did not maintain size; sz = %v, but len(words) = %v", sz, len(words)))
			}
			//if len(words)%1000 == 0 {
			//	fmt.Printf("words progres %v ...\n", len(words))
			//}
			//fmt.Printf("ok: added key '%v'\n", string(line))
		}
		if i > 500 { // underRaceDetector
			// Under the race detector and the pessimistic
			// (simulated) locking to keep the race detector happy,
			// we are too slow for 260k words.
			i++
			break
		}
	}
	//vv("inserted i = %v words vs len(words) = %v", i, len(words))
	//vv("tree = '%v'", tree.FlatString())
	if i != len(words) {
		t.Fatalf("i(%v) != len(words)=%v", i, len(words))
	}
	// read strings back from words map
	removed := make(map[string]bool)
	_ = removed
	// red intermittant: for line := range words {
	// use word_order to get a consistent deletion order:
	//permseed := 0 // red: 0,3,4,5,6,7,9
	// green: 1,2,8
	permseed := 0
	word_order = permute(word_order, permseed)
	//vv("permseed %v => word_order = '%#v'", permseed, word_order)
	for _, line := range word_order {
		//for line := range words {
		//fmt.Printf("map based: removing line '%v'\n", line)
		deleted, delval := tree.Remove([]byte(line))
		if !deleted {
			_, dup := removed[line]
			//vv("sz = %v", tree.Size()) // sz = 235872
			panic(fmt.Sprintf("Remove did not delete '%v', wat? dup = '%v'", line, dup))
		}
		got := string(delval.Value)
		if got != line {
			panic(fmt.Sprintf("delval: '%v' != Remove key line: '%v' as it should", got, line))
		}
		removed[line] = true
		i--
		sz := tree.Size()
		if sz != i {
			panic(fmt.Sprintf("Remove did not maintain Size; sz = %v, but i = %v", sz, i))
		}

		//vv("tree of sz %v = '%v'", sz, tree.FlatString())
	}

	sz := tree.Size()
	if sz != 0 {
		t.Errorf("Tree is not empty after adding and removing many words: size %v", sz)
	}
}

// After Inserting many values into the tree, we should be able to remove them all
// And expect nothing to exist in the tree.
func TestInsertManyUUIDsAndRemoveThemAll(t *testing.T) {
	tree := NewArtTree()

	words := loadTestFile("assets/uuid.txt")
	//words := make(map[string]bool)

	if true { // underRaceDetector {
		words = words[:500]
	}
	for _, line := range words {
		by := &TestBytes{Slc: []byte(line)}
		tree.Insert([]byte(line), by.Slc)
	}

	for _, line := range words {
		tree.Remove([]byte(line))
	}

	if tree.Size() != 0 {
		t.Error("Tree is not empty after adding and removing many uuids")
	}
}

func TestInsertWithSameByteSliceAddress(t *testing.T) {
	rand.Seed(42)
	key := make([]byte, 8)
	tree := NewArtTree()

	// Keep track of what we inserted
	keys := make(map[string]bool)

	for i := 0; i < 135; i++ {
		binary.BigEndian.PutUint64(key, uint64(rand.Int63()))
		// yeah, so this totally fails and catches
		// out the tree.go Insert() code.

		// make a copy to store
		key2 := append([]byte{}, key...)

		by := &TestBytes{Slc: append([]byte{}, key2...)}
		updated := tree.Insert(key2, by.Slc) // key2)
		_ = updated
		//vv("i=%v, inserting key '%v'; updated = %v", i, string(key2), updated)

		// Ensure that we can search these records later
		keys[string(key2)] = true
	}

	if tree.Size() != len(keys) {
		t.Errorf("Mismatched size of tree and expected values.  Expected: %d.  Actual: %d\n", len(keys), tree.Size())
	}

	for k, _ := range keys {
		n, _, ok := tree.FindExact([]byte(k))
		if !ok || n == nil {
			t.Errorf("Did not find entry for key: %v\n", []byte(k))
		}
	}
}

func Test_delete_insert_on_LongCommonPrefixes(t *testing.T) {
	tree := NewArtTree()
	paths := loadTestFile("assets/linux.txt")
	seenk := 0

	expect := len(paths)
	if true { //underRaceDetector { // -race is very slow
		expect = 1000
		paths = paths[:expect]
	}

	for i, w := range paths {
		_ = i
		if tree.Insert(w, w) {
			t.Fatalf("i=%v, could not add '%v', already in tree", i, string(w))
		}
		if tree.Size() != (i + 1) {
			t.Fatalf("expected %v paths in tree, got size: %v", i, tree.Size())
		}
		seenk++
	}
	if seenk != expect {
		t.Fatalf("expected %v paths in tree, got size: %v", expect, seenk)
	}
	if tree.Size() != expect {
		t.Fatalf("expected %v paths in tree, got size: %v", expect, tree.Size())
	}

	// verify that update actually happens
	for i, w := range paths {
		if i == 0 {
			continue
		}
		if updated := tree.Insert(w, paths[i-1]); !updated {
			t.Fatalf("i=%v, could not detect dup of '%v', bad: added to tree instead.", i, string(w))
		}
		if tree.Size() != expect {
			t.Fatalf("dups should not expand tree: expected %v paths in tree, got size: %v", expect, tree.Size())
		}
	}
	for i, w := range paths {
		if i == 0 {
			continue
		}
		g, _, ok := tree.FindExact(w)
		got := string(g)
		want := string(paths[i-1])
		if !ok || want != got {
			t.Fatalf("i=%v, expected value '%v' for key '%v': got '%v' instead", i, want, w, got)
		}
	}
	//fmt.Printf("yay: replace/updates seem to have worked.\n")

	for _, w := range paths {
		if gone, _ := tree.Remove(w); !gone {
			t.Fatalf("could not delete '%v'", string(w))
		}
	}
	if tree.Size() != 0 {
		// expected nothing left in tree, got size: 93789
		t.Fatalf("expected nothing left in tree, got size: %v", tree.Size())
	}

	fmt.Printf("past removes.\n")

	// // run stats
	// stats := tree.Stats()
	// fmt.Printf("st  = '%#v'\n", stats)
	// if stats.Keys != 0 {
	// 	t.Fatalf("expected nothing left in tree, got size: %v", stats.Keys)
	// }

	// put them all back in
	for i, w := range paths {
		_ = i
		if updated := tree.Insert(w, w); updated {
			t.Fatalf("i=%v, could not add '%v', already in tree", i, string(w))
		}
		// when does our stale pren happen? at i=7
		//vv("doing i = %v", i)
		//if i == 6 {
		//	verboseVerbose = true
		//}

		//verifyPren(tree.root) // panic, stale pren detected.

		//verifyLeafIndexAt(tree) // panic! wrong indexes here detected

		if tree.Size() != (i + 1) {
			// expected 0 paths in tree, got different
			t.Fatalf("expected %v paths in tree, got size: %v", i, tree.Size())
		}
	}

	fmt.Printf("past all back in.\n")

	// does this stop the incorrect LeafIndex() problems?
	// no. we did fix a stale pren issue, but there is another problem too.
	//fullTreeRedoPren(tree.root)

	// try to delete with prefixes that are in tree
	// but full paths that are not, and verify no delete happens.
	n := int(tree.Size())
	j := 0

	// verify we return exactly all the contents, and each exactly once.
	seen := make(map[string]int)
	for _, w := range paths {
		seen[string(w)] = -1
	}

	if true {
		// use the iter.Seq2 protocol to iterate
		j = 0
		for key, val := range Ascend(tree, nil, nil) {
			_ = key
			_ = val
			w := string(key) // val.(*Leaf).Key)
			when := seen[w]
			if when != -1 {
				t.Fatalf("got %v, want %v for when seen path w ='%v'; j = %v", when, -1, w, j)
			}
			seen[w] = j

			v := w + "@"
			//vv("on j = %v, deleting v = '%v' which should not be present.", j, v)
			if gone, _ := tree.Remove([]byte(v)); gone {
				t.Fatalf("unexpected Remove of key not in tree: '%v'", v)
			}
			if int(tree.Size()) != n {
				t.Fatalf("expected size %v, saw %v", n, tree.Size())
			}
			j++
		}
		// restore seen for next use below
		j = 0
		seen = make(map[string]int)
		for _, w := range paths {
			seen[string(w)] = -1
		}
	}

	fmt.Printf("begin iterating\n")

	// it is size 8 in minimal problem case here. ok to print.
	//vv("sz = %v; tree = '%v'", tree.Size(), tree)

	// finds a LeafIndex() problem, nothing to do with iteration!
	//verifyLeafIndexAt(tree)
	//vv("past verifyLeafIndexAt() here.") // seen

	// our SubN were off due to premature n.SubN-- in del()
	//verifySubN(tree.root)

	j = 0
	var beg, endx []byte
	it := tree.Iter(beg, endx)
	k := 0
	for it.Next() {
		key := it.Key()
		val := it.Value()
		idx := it.Index()
		_ = idx
		//vv("k=%v; idx=%v; see key = '%v'", k, idx, string(key))

		//verifySubN(tree.root) // detects problem k=2 printed, so on k=1

		_ = key
		w := string(val)
		when := seen[w]
		if when != -1 {
			t.Fatalf("got %v, want %v for when seen path w ='%v'", when, -1, w)
		}
		seen[w] = j

		v := w + "@"
		//vv("on j = %v, deleting v = '%v' which should not be present.", j, v)
		if gone, _ := tree.Remove([]byte(v)); gone { // seeing an incorrect 0 subn in here.
			t.Fatalf("unexpected Remove of key not in tree: '%v'", v)
		}
		if int(tree.Size()) != n {
			t.Fatalf("expected size %v, saw %v", n, tree.Size())
		}
		j++
		k++
	}

	//verifyLeafIndexAt(tree)
	//verifySubN(tree.root)

	fmt.Printf("done iterating. past @ should not removes.\n")

	//vv("at end of loop j = %v, vs tree.Size = %v", j, tree.Size())
	if j != n {
		// expected to stop earlier (asap);  j=93790; tree.Size=187579
		t.Fatalf("expected to stop earlier (asap);  j=%v; tree.Size=%v", j, tree.Size())
	}
	if j != expect {
		t.Fatalf("expected to have j=%v, but see j=%v; tree.Size=%v", expect, j, tree.Size())
	}

	// now delete with iter
	//vv("before deleting all, size = %v", tree.Size())
	if tree.Size() != expect {
		panic("why short?")
	}
	return
}

//
// Benchmarks
//

func loadTestFile2(path string) [][]byte {
	file, err := os.Open(path)
	if err != nil {
		panic("Couldn't open " + path)
	}
	defer file.Close()

	var words [][]byte
	reader := bufio.NewReader(file)
	for {
		if line, err := reader.ReadBytes(byte('\n')); err != nil {
			break
		} else {
			if len(line) > 0 {
				words = append(words, line[:len(line)-1])
			}
		}
	}
	return words
}

func BenchmarkWordsArtTreeInsert(b *testing.B) {
	words := loadTestFile("assets/words.txt")
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		tree := NewArtTree()
		for _, w := range words {
			tree.Insert(w, w)
		}
	}
}

func BenchmarkWordsArtTreeSearch(b *testing.B) {
	words := loadTestFile("assets/words.txt")
	tree := NewArtTree()
	for _, w := range words {
		tree.Insert(w, w)
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		for _, w := range words {
			tree.FindExact(w)
		}
	}
}

func BenchmarkWordsArtTreeRemove(b *testing.B) {
	words := loadTestFile("assets/words.txt")
	tree := NewArtTree()
	for _, w := range words {
		tree.Insert(w, w)
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		for _, w := range words {
			tree.Remove(w)
		}
	}
}

func BenchmarkUUIDsArtTreeInsert(b *testing.B) {
	words := loadTestFile("assets/uuid.txt")
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		tree := NewArtTree()
		for _, w := range words {
			tree.Insert(w, w)
		}
	}
}

func BenchmarkUUIDsArtTreeSearch(b *testing.B) {
	words := loadTestFile("assets/uuid.txt")
	tree := NewArtTree()
	for _, w := range words {
		tree.Insert(w, w)
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		for _, w := range words {
			tree.FindExact(w)
		}
	}
}

func BenchmarkMapSearch(b *testing.B) {
	words := loadTestFile("assets/words.txt")
	m := make(map[string]string)
	//tree := skiplist.New()
	for _, w := range words {
		//tree.Set(string(w), string(w))
		m[string(w)] = string(w)
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		for _, w := range words {
			_ = m[(string(w))]
		}
	}
}

func BenchmarkMapInsert(b *testing.B) {
	words := loadTestFile("assets/words.txt")
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		m := make(map[string]string)
		//tree := skiplist.New()
		for _, w := range words {
			//tree.Set(string(w), string(w))
			m[string(w)] = string(w)
		}
	}
}

func BenchmarkMapRemove(b *testing.B) {
	words := loadTestFile("assets/words.txt")
	m := make(map[string]string)
	//tree := skiplist.New()
	for _, w := range words {
		//tree.Set(string(w), string(w))
		m[string(w)] = string(w)
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		for _, w := range words {
			delete(m, (string(w)))
		}
	}
}

// helper for computing a permutation
type origpos struct {
	orig int
	pos  uint64
}

type permSlice []*origpos

func (p permSlice) Len() int { return len(p) }
func (p permSlice) Less(i, j int) bool {
	if p[i].pos == p[j].pos {
		return p[i].orig < p[j].orig
	}
	return p[i].pos < p[j].pos
}
func (p permSlice) Swap(i, j int) { p[i], p[j] = p[j], p[i] }

func permute(s []string, seed int) (r []string) {
	r = append([]string{}, s...)
	n := len(s)

	var seed32 [32]byte
	seed32[0] = byte(seed)
	chacha8 := mathrand2.NewChaCha8(seed32)
	perm := make([]*origpos, n)
	for i := range perm {
		perm[i] = &origpos{
			orig: i,
			pos:  chacha8.Uint64(),
		}
	}
	// generate the permuation
	sort.Sort(permSlice(perm))

	//vv("seed %v => perm %#v", seed, perm)

	// arrange r according to the permutation.
	for i := range perm {
		//fmt.Printf("from seed %v => perm[i=%v].orig = %v\n", seed, i, perm[i].orig)
		r[i] = s[perm[i].orig]
	}
	return r
}

func Test_n4replace_buggy_test(t *testing.T) {
	// n4.go replace method was buggy
	// and this was how we tracked it down.
	tree := NewArtTree()

	file, err := os.Open("assets/words.txt")
	if err != nil {
		t.Error("Couldn't open words.txt")
	}

	defer file.Close()

	reader := bufio.NewReader(file)

	words := make(map[string]bool)
	var word_order []string // the del order makes for red/green test
	i := 0
	for ; ; i++ {
		if line, err := reader.ReadBytes('\n'); err != nil {
			break
		} else {
			//by := &TestBytes{Slc: []byte(line)}
			tree.Insert([]byte(line), []byte(line))
			sline := string(line)
			words[sline] = true
			word_order = append(word_order, sline)
			sz := tree.Size()
			if sz != len(words) {
				panic(fmt.Sprintf("Insert did not maintain size; sz = %v, but len(words) = %v", sz, len(words)))
			}
			//if len(words)%1000 == 0 {
			//	fmt.Printf("words progres %v ...\n", len(words))
			//}
			//fmt.Printf("ok: added key '%v'\n", string(line))
		}
		if i > 2 { //  underRaceDetector && i > 500 {
			// Under the race detector and the pessimistic
			// (simulated) locking to keep the race detector happy,
			// we are too slow for 260k words.
			i++
			break
		}
	}
	//vv("inserted i = %v words vs len(words) = %v", i, len(words))
	//vv("tree = '%v'", tree.FlatString())
	if i != len(words) {
		t.Fatalf("i(%v) != len(words)=%v", i, len(words))
	}
	// read strings back from words map
	removed := make(map[string]bool)
	_ = removed
	// red intermittant: for line := range words {
	// use word_order to get a consistent deletion order:
	// permseed := 0 // red: 0,3,4,5,6,7,9
	// green: 1,2,8
	permseed := 3
	word_order = permute(word_order, permseed)
	//vv("permseed %v => word_order = '%#v'", permseed, word_order)
	for _, line := range word_order {
		//for line := range words {
		//fmt.Printf("map based: removing line '%v'\n", line)
		deleted, delval := tree.Remove([]byte(line))
		if !deleted {
			_, dup := removed[line]
			panic(fmt.Sprintf("Remove did not delete '%v', wat? dup = '%v'", line, dup))
		}
		got := string(delval.Value)
		if got != line {
			panic(fmt.Sprintf("delval: '%v' != Remove key line: '%v' as it should", got, line))
		}
		removed[line] = true
		i--
		sz := tree.Size()
		if sz != i {
			panic(fmt.Sprintf("Remove did not maintain Size; sz = %v, but i = %v", sz, i))
		}

		//vv("tree of sz %v = '%v'", sz, tree.FlatString())
	}

	sz := tree.Size()
	if sz != 0 {
		t.Errorf("Tree is not empty after adding and removing many words: size %v", sz)
	}
}

func verifyLeafIndexAt(tree *Tree) {
	sz := tree.Size()
	for i := range sz {
		lf, ok := tree.At(i)
		if !ok {
			panic("not okay?")
		}
		j, ok := tree.LeafIndex(lf)
		if !ok || j != i {
			// panic: want ok=true, j=i=7; got ok=true; j=6
			panic(fmt.Sprintf("want ok=true, want j=i=%v; got ok=%v; j=%v", i, ok, j))
		}
		lf2, idx2, ok2 := tree.Find(Exact, lf.Key)
		if !ok2 {
			panic("why not ok2?")
		}
		if idx2 != j {
			panic("idx2 != j")
		}
		if lf2 != lf {
			panic(fmt.Sprintf("lf2 != lf. lf2 = '%v';\n\n lf = '%v'", lf2, lf))
		}
	}
}

func Test_PrenInsert(t *testing.T) {

	tree := NewArtTree()
	paths := loadTestFile("assets/linux.txt")

	limit := 0
	if limit > 0 {
		paths = paths[:limit]
	}

	for i, w := range paths {
		_ = i
		if tree.Insert(w, w) {
			t.Fatalf("i=%v, could not add '%v', "+
				"already in tree", i, string(w))
		}
	}

	verifySubN(tree.root)
	verifyPren(tree.root)
}

func Test_Seq2_Iter_on_LongCommonPrefixes(t *testing.T) {

	tree := NewArtTree()
	paths := loadTestFile("assets/linux.txt")

	limit := 0
	if limit > 0 {
		paths = paths[:limit]
	}

	var sorted [][]byte
	for i, w := range paths {
		_ = i
		sorted = append(sorted, w)
	}
	sort.Sort(sliceByteSlice(sorted))

	for i, w := range sorted {
		_ = i
		if tree.Insert(w, w) {
			t.Fatalf("i=%v, could not add '%v', "+
				"already in tree", i, string(w))
		}
	}

	verifySubN(tree.root)
	verifyPren(tree.root)
	//vv("tree = %v", tree.stringNoKeys(-1))

	j := 0
	for key, leaf := range Ascend(tree, nil, nil) {
		_ = leaf
		keyb := []byte(key)
		if !bytes.Equal(keyb, sorted[j]) {
			t.Fatalf("Ascend got key '%v'; expected '%v'",
				string(key), string(sorted[j]))
		}
		j++
	}

	j = 0
	last := len(paths) - 1
	for key, leaf := range Descend(tree, nil, nil) {
		_ = leaf
		keyb := []byte(key)
		if !bytes.Equal(keyb, sorted[last-j]) {
			t.Fatalf("Descend got key '%v'; expected '%v'",
				string(key), string(sorted[j]))
		}
		j++
	}

}

// test that updates in-place (no actual insert of new leaf)
// keep the pren and subn correctly
func Test_pren_sub_on_update_in_place(t *testing.T) {
	tree := NewArtTree()
	paths := loadTestFile("assets/linux.txt")
	seenk := 0

	expect := len(paths)
	if true { //underRaceDetector { // -race is very slow
		expect = 1000
		paths = paths[:expect]
	}

	for i, w := range paths {
		_ = i
		if tree.Insert(w, w) {
			t.Fatalf("i=%v, could not add '%v', already in tree", i, string(w))
		}
		if tree.Size() != (i + 1) {
			t.Fatalf("expected %v paths in tree, got size: %v", i, tree.Size())
		}
		seenk++
	}
	if seenk != expect {
		t.Fatalf("expected %v paths in tree, got size: %v", expect, seenk)
	}
	if tree.Size() != expect {
		t.Fatalf("expected %v paths in tree, got size: %v", expect, tree.Size())
	}

	verifySubN(tree.root)
	verifyLeafIndexAt(tree)
	//fmt.Printf("past initial tree check.\n")

	// verify that update in place does not change pren/subn.
	// note this will be slow as it goes though the whole
	// tree on each i to check every node.
	for i, w := range paths {
		if i == 0 {
			continue
		}
		// write a new (previous) path to w
		if updated := tree.Insert(w, paths[i-1]); !updated {
			t.Fatalf("i=%v, could not detect dup of '%v', bad: added to tree instead.", i, string(w))
		}
		if tree.Size() != expect {
			t.Fatalf("dups should not expand tree: expected %v paths in tree, got size: %v", expect, tree.Size())
		}
		//vv("i=%v", i) // i=1 first time it is off!
		verifySubN(tree.root)
		verifyLeafIndexAt(tree)
		//vv("okay on i = %v", i)
	}

}
