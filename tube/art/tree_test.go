package art

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	googbtree "github.com/google/btree"
	mathrand "math/rand"
	mathrand2 "math/rand/v2"
	"os"
	"sort"
	"strconv"
	"testing"
	"time"
)

var _ = sort.Sort

type sliceByteSlice [][]byte

func (p sliceByteSlice) Len() int { return len(p) }
func (p sliceByteSlice) Less(i, j int) bool {
	return bytes.Compare(p[i], p[j]) <= 0
}
func (p sliceByteSlice) Swap(i, j int) { p[i], p[j] = p[j], p[i] }

type sliceByteSliceRev [][]byte

func (p sliceByteSliceRev) Len() int { return len(p) }
func (p sliceByteSliceRev) Less(i, j int) bool {
	return bytes.Compare(p[i], p[j]) > 0
}
func (p sliceByteSliceRev) Swap(i, j int) { p[i], p[j] = p[j], p[i] }

func TestArtTree_InsertBasic(t *testing.T) {
	tree := NewArtTree()
	// insert one key
	tree.Insert(Key("I'm Key"), ByteSliceValue("I'm Value"), "vtype9")

	// search it
	value, _, found, vtype := tree.FindExact(Key("I'm Key"))
	if got, want := string(value), "I'm Value"; got != want {
		t.Errorf("got value %v, want %v", got, want)
	}
	if !found {
		t.Error("expected key to be found")
	}
	if vtype != "vtype9" {
		t.Errorf("expected vtype9, got '%v'", vtype)
	}

	//insert another key
	tree.Insert(Key("I'm Key2"), ByteSliceValue("I'm Value2"), "")

	// search it
	value, _, found, vtype = tree.FindExact(Key("I'm Key2"))
	if got, want := value, []byte("I'm Value2"); !bytes.Equal(got, want) {
		t.Errorf("got value %v, want %v", got, want)
	}

	// should be found
	value, _, found, vtype = tree.FindExact(Key("I'm Key"))
	if got, want := value, []byte("I'm Value"); !bytes.Equal(got, want) {
		t.Errorf("got value %v, want %v", got, want)
	}
	if vtype != "vtype9" {
		t.Errorf("expected vtype9, got '%v'", vtype)
	}

	// lazy path expansion
	tree.Insert(Key("I'm"), ByteSliceValue("I'm"), "")

	// splitting, check depth on this one; should be 1.
	tree.Insert(Key("I"), ByteSliceValue("I"), "")

	tree.Remove(Key("I"))
}

type Set struct {
	key   Key
	value ByteSliceValue
}

func TestArtTree_InsertLongKey(t *testing.T) {
	tree := NewArtTree()
	tree.Insert(Key("sharedKey::1"), ByteSliceValue("value1"), "")
	tree.Insert(Key("sharedKey::1::created_at"), ByteSliceValue("created_at_value1"), "")

	value, _, found, _ := tree.FindExact(Key("sharedKey::1"))
	if !found {
		t.Error("expected key to be found")
	}
	if got, want := value, []byte("value1"); !bytes.Equal(got, want) {
		t.Errorf("got value %v, want %v", got, want)
	}

	value, _, found, _ = tree.FindExact(Key("sharedKey::1::created_at"))
	if !found {
		t.Error("expected key to be found")
	}
	if got, want := value, []byte("created_at_value1"); !bytes.Equal(got, want) {
		t.Errorf("got value %v, want %v", got, want)
	}

	//it := tree.Iter(nil, nil)
	//for i := 0; it.Next(); i++ {
	//	fmt.Printf("i=%v key: '%v'\n", i, string(it.Key()))
	//}
}

func TestArtTree_Insert2(t *testing.T) {
	tree := NewArtTree()
	sets := []Set{{
		Key("sharedKey::1"), ByteSliceValue("value1"),
	}, {
		Key("sharedKey::2"), ByteSliceValue("value2"),
	}, {
		Key("sharedKey::3"), ByteSliceValue("value3"),
	}, {
		Key("sharedKey::4"), ByteSliceValue("value4"),
	}, {
		Key("sharedKey::1::created_at"), ByteSliceValue("created_at_value1"),
	}, {
		Key("sharedKey::1::name"), ByteSliceValue("name_value1"),
	},
	}
	for _, set := range sets {
		tree.Insert(set.key, set.value, "")
	}
	for _, set := range sets {
		value, _, found, _ := tree.FindExact(set.key)
		if !found {
			t.Errorf("expected key %v to be found", set.key)
		}
		if got, want := value, set.value; !bytes.Equal(got, want) {
			t.Errorf("got value %v, want %v", got, want)
		}
	}
}

func TestArtTree_Insert3(t *testing.T) {
	tree := NewArtTree()
	tree.Insert(Key("sharedKey::1"), ByteSliceValue("value1"), "")
	tree.Insert(Key("sharedKey::2"), ByteSliceValue("value1"), "")
	tree.Insert(Key("sharedKey::3"), ByteSliceValue("value1"), "")
	tree.Insert(Key("sharedKey::4"), ByteSliceValue("value1"), "")

	tree.Insert(Key("sharedKey::1::created_at"), ByteSliceValue("created_at_value1"), "")

	tree.Insert(Key("sharedKey::1::name"), ByteSliceValue("name_value1"), "")

	value, _, found, _ := tree.FindExact(Key("sharedKey::1::created_at"))
	if !found {
		t.Error("expected key to be found")
	}
	if got, want := value, []byte("created_at_value1"); !bytes.Equal(got, want) {
		t.Errorf("got value %v, want %v", got, want)
	}
}

func TestTree_Update(t *testing.T) {
	tree := NewArtTree()
	key := Key("I'm Key")

	// insert an entry
	tree.Insert(key, ByteSliceValue("I'm Value"), "")

	// should be found
	value, _, found, _ := tree.FindExact(key)
	if got, want := value, []byte("I'm Value"); !bytes.Equal(got, want) {
		t.Errorf("got value %v, want %v", got, want)
	}
	if !found {
		t.Error("The inserted key should be found")
	}

	// try update inserted key
	updated := tree.Insert(key, ByteSliceValue("Value Updated"), "")
	if !updated {
		t.Error("expected key to be updated")
	}

	value, _, found, _ = tree.FindExact(key)
	if !found {
		t.Error("The inserted key should be found")
	}
	if got, want := value, []byte("Value Updated"); !bytes.Equal(got, want) {
		t.Errorf("got value %v, want %v", got, want)
	}
}

func TestArtTree_InsertSimilarPrefix(t *testing.T) {
	tree := NewArtTree()
	tree.Insert(Key{1}, []byte{1}, "")
	tree.Insert(Key{1, 1}, []byte{1, 1}, "")

	v, _, found, _ := tree.FindExact(Key{1, 1})
	if !found {
		t.Error("expected key to be found")
	}
	if got, want := v, []byte{1, 1}; !bytes.Equal(got, want) {
		t.Errorf("got value %v, want %v", got, want)
	}
}

func TestArtTree_InsertMoreKey(t *testing.T) {
	tree := NewArtTree()
	keys := []Key{{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, {1, 1, 1, 1}, {1, 1, 1}, {2, 1, 1}}
	for _, key := range keys {
		tree.Insert(key, ByteSliceValue(key), "")
	}
	for i, key := range keys {
		value, _, found, _ := tree.FindExact(key)
		if got, want := value, []byte(key); !bytes.Equal(got, want) {
			t.Errorf("[run:%d] expected %v but got %v", i, key, value)
		}
		if !found {
			t.Errorf("[run:%d] expected key to be found", i)
		}
	}
}

func TestArtTree_Remove(t *testing.T) {
	tree := NewArtTree()
	deleted, _ := tree.Remove(Key("wrong-key"))
	if deleted {
		t.Error("expected key not to be deleted")
	}

	tree.Insert(Key("sharedKey::1"), ByteSliceValue("value1"), "")
	tree.Insert(Key("sharedKey::2"), ByteSliceValue("value2"), "")

	deleted, value := tree.Remove(Key("sharedKey::2"))
	if got, want := string(value.Value), "value2"; got != want {
		t.Errorf("got value %v, want %v", got, want)
	}
	if !deleted {
		t.Error("expected key to be deleted")
	}

	deleted, value = tree.Remove(Key("sharedKey::3"))
	if value != nil {
		t.Errorf("expected nil value, got %v", value)
	}
	if deleted {
		t.Error("expected key not to be deleted")
	}

	tree.Insert(Key("sharedKey::3"), ByteSliceValue("value3"), "")

	deleted, value = tree.Remove(Key("sharedKey"))
	if value != nil {
		t.Errorf("expected nil value, got %v", value)
	}
	if deleted {
		t.Error("expected key not to be deleted")
	}

	tree.Insert(Key("sharedKey::4"), ByteSliceValue("value3"), "")

	deleted, value = tree.Remove(Key("sharedKey::5::xxx"))
	if value != nil {
		t.Errorf("expected nil value, got %v", value)
	}
	if deleted {
		t.Error("expected key not to be deleted")
	}

	deleted, value = tree.Remove(Key("sharedKey::4xsfdasd"))
	if value != nil {
		t.Errorf("expected nil value, got %v", value)
	}
	if deleted {
		t.Error("expected key not to be deleted")
	}

	tree.Insert(Key("sharedKey::4::created_at"), ByteSliceValue("value3"), "")
	deleted, value = tree.Remove(Key("sharedKey::4::created_at"))
	if !deleted {
		t.Error("expected key to be deleted")
	}
}

func TestArtTree_FindExact(t *testing.T) {
	tree := NewArtTree()
	value, _, found, _ := tree.FindExact(Key("wrong-key"))
	if value != nil {
		t.Errorf("expected nil value, got %v", value)
	}
	if found {
		t.Error("expected key not to be found")
	}

	tree.Insert(Key("sharedKey::1"), ByteSliceValue("value1"), "")

	//vv("tree = %v", tree)
	var idx int
	value, idx, found, _ = tree.FindExact(Key("sharedKey"))
	_ = idx
	//vv("idx = %v", idx)
	if value != nil {
		t.Errorf("expected nil value, got %v", string(value))
	}
	if found {
		t.Error("expected key not to be found")
	}

	value, _, found, _ = tree.FindExact(Key("sharedKey::2"))
	if value != nil {
		t.Errorf("expected nil value, got %v", string(value))
	}
	if found {
		t.Error("expected key not to be found")
	}

	tree.Insert(Key("sharedKey::2"), ByteSliceValue("value1"), "")

	value, _, found, _ = tree.FindExact(Key("sharedKey::3"))
	if value != nil {
		t.Errorf("expected nil value, got %v", string(value))
	}
	if found {
		t.Error("expected key not to be found")
	}

	value, _, found, _ = tree.FindExact(Key("sharedKey"))
	if value != nil {
		t.Errorf("expected nil value, got %v", value)
	}
	if found {
		t.Error("expected key not to be found")
	}
}

func TestArtTree_Remove2(t *testing.T) {
	tree := NewArtTree()
	sets := []Set{{
		Key("012345678:-1"), ByteSliceValue("value1"),
	}, {
		Key("012345678:-2"), ByteSliceValue("value2"),
	}, {
		Key("012345678:-3"), ByteSliceValue("value3"),
	}, {
		Key("012345678:-4"), ByteSliceValue("value4"),
	}, {
		Key("012345678:-1*&created_at"), ByteSliceValue("created_at_value1"),
	}, {
		Key("012345678:-1*&name"), ByteSliceValue("name_value1"),
	},
	}
	for _, set := range sets {
		tree.Insert(set.key, set.value, "")
	}
	for _, set := range sets {
		value, _, found, _ := tree.FindExact(set.key)
		if !found {
			t.Errorf("expected key %v to be found", set.key)
		}
		if got, want := value, set.value; !bytes.Equal(got, want) {
			t.Errorf("got value %v, want %v", got, want)
		}
	}
	for i, set := range sets {
		deleted, value := tree.Remove(set.key)
		if !deleted {
			t.Errorf("[run:%d] expected key %v to be deleted", i, set.key)
		}
		if got, want := value.Value, set.value; !bytes.Equal(got, want) {
			t.Errorf("[run:%d] got deleted value %v, but got %v", i, set.value, value)
		}
	}
}

type keyValueGenerator struct {
	cur       int
	generator func([]byte) []byte
}

func (g keyValueGenerator) getByteSliceValue(key Key) ByteSliceValue {
	return g.generator(key)
}

func (g *keyValueGenerator) prev() (Key, ByteSliceValue) {
	g.cur--
	k, v := g.get()
	return k, v
}

func (g *keyValueGenerator) get() (Key, ByteSliceValue) {
	var buf [8]byte
	binary.PutUvarint(buf[:], uint64(g.cur))
	return buf[:], g.generator(buf[:])
}

func (g *keyValueGenerator) next() (Key, ByteSliceValue) {
	k, v := g.get()
	g.cur++
	return k, v
}

func (g *keyValueGenerator) setCur(c int) {
	g.cur = c
}

func (g *keyValueGenerator) resetCur() {
	g.setCur(0)
}

func NewKeyValueGenerator() *keyValueGenerator {
	return &keyValueGenerator{cur: 0, generator: func(input []byte) []byte {
		return input
	}}
}

type CheckPoint struct {
	name       string
	totalNodes int
	expected   kind
}

func TestArtTree_Grow(t *testing.T) {
	checkPoints := []CheckPoint{
		{totalNodes: 5, expected: _Node16, name: "node4 growing test"},
		{totalNodes: 17, expected: _Node48, name: "node16 growing test"},
		{totalNodes: 49, expected: _Node256, name: "node256 growing test"},
	}
	for _, point := range checkPoints {
		tree := NewArtTree()
		g := NewKeyValueGenerator()
		for i := 0; i < point.totalNodes; i++ {
			k, b := g.next()
			tree.Insert(k, b, "")
		}
		if got := tree.size; got != int64(point.totalNodes) {
			t.Errorf("exected size %d but got %d", point.totalNodes, got)
		}
		if got := tree.root.kind(); got != point.expected {
			t.Errorf("exected kind %s got %s", point.expected, got)
		}
		g.resetCur()
		for i := 0; i < point.totalNodes; i++ {
			k, v := g.next()
			got, _, found, _ := tree.FindExact(k)
			if !found {
				t.Errorf("should found inserted (%v,%v) in test %s", k, v, point.name)
			}
			if !bytes.Equal(got, []byte(v)) {
				t.Errorf("should equal inserted (%v,%v) in test %s", k, v, point.name)
			}
		}
	}
}

func TestArtTree_Shrink(t *testing.T) {
	tree := NewArtTree()
	g := NewKeyValueGenerator()
	// fill up an 256 node
	for i := 0; i < 256; i++ {
		k, b := g.next()
		tree.Insert(k, b, "")
	}
	// check inserted
	g.resetCur()
	for i := 0; i < 256; i++ {
		k, v := g.next()
		got, _, found, _ := tree.FindExact(k)
		if !found {
			t.Error("expected key to be found")
		}
		if !bytes.Equal(got, []byte(v)) {
			t.Errorf("got %v, want %v", got, v)
		}
	}
	// deleting nodes
	for i := 255; i >= 0; i-- {
		if got := tree.size; got != int64(i+1) {
			t.Errorf("expected size %d but got %d", i+1, got)
		}
		k, v := g.prev()
		deleted, old := tree.Remove(k)
		if !deleted {
			t.Errorf("expected key %v to be deleted", k)
		}
		if !bytes.Equal(old.Value, v) {
			t.Errorf("got %v, want %v", old, v)
		}
		switch tree.size {
		case 48:
			if got := tree.root.kind(); got != _Node48 {
				t.Errorf("expected kind %s got %s", _Node48, got)
			}
		case 16:
			if got := tree.root.kind(); got != _Node16 {
				t.Errorf("expected kind %s got %s", _Node16, got)
			}
		case 4:
			if got := tree.root.kind(); got != _Node4 {
				t.Errorf("expected kind %s got %s", _Node4, got)
			}
		case 0:
			if tree.root != nil {
				t.Error("expected nil root")
			}
		}
	}
}

func TestArtTree_ShrinkConcatenating(t *testing.T) {
	tree := NewArtTree()
	tree.Insert(Key("sharedKey::1"), ByteSliceValue("value1"), "")
	tree.Insert(Key("sharedKey::2"), ByteSliceValue("value1"), "")
	tree.Insert(Key("sharedKey::3"), ByteSliceValue("value1"), "")
	tree.Insert(Key("sharedKey::4"), ByteSliceValue("value1"), "")

	tree.Insert(Key("sharedKey::1::nested::name"), ByteSliceValue("created_at_value1"), "")
	tree.Insert(Key("sharedKey::1::nested::job"), ByteSliceValue("name_value1"), "")

	tree.Insert(Key("sharedKey::1::nested::name::firstname"), ByteSliceValue("created_at_value1"), "")
	tree.Insert(Key("sharedKey::1::nested::name::lastname"), ByteSliceValue("created_at_value1"), "")

	tree.Remove(Key("sharedKey::1::nested::name"))

	_, _, found, _ := tree.FindExact(Key("sharedKey::1::nested::name"))
	if found {
		t.Error("expected key not to be found")
	}
}

func TestArtTree_LargeKeyShrink(t *testing.T) {
	tree := NewArtTree()
	g := NewLargeKeyValueGenerator([]byte("this a very long sharedKey::"))
	// fill up an 256 node
	for i := 0; i < 256; i++ {
		k, b := g.next()
		tree.Insert(k, b, "")
	}
	// check inserted
	g.resetCur()
	for i := 0; i < 256; i++ {
		k, v := g.next()
		got, _, found, _ := tree.FindExact(k)
		if !found {
			t.Error("expected key to be found")
		}
		if !bytes.Equal(got, v) {
			t.Error("expected value to be found")
		}
	}
	// deleting nodes
	for i := 255; i >= 0; i-- {
		if got := tree.size; got != int64(i+1) {
			t.Errorf("expected size %d but got %d", i+1, got)
		}
		k, v := g.prev()
		deleted, old := tree.Remove(k)
		if !deleted {
			t.Errorf("expected key %v to be deleted", k)
		}
		if !bytes.Equal(old.Value, v) {
			t.Errorf("got %v, want %v", old, v)
		}
		switch tree.size {
		case 48:
			if got := tree.root.kind(); got != _Node48 {
				t.Errorf("expected kind %s got %s", _Node48, got)
			}
		case 16:
			if got := tree.root.kind(); got != _Node16 {
				t.Errorf("expected kind %s got %s", _Node16, got)
			}
		case 4:
			if got := tree.root.kind(); got != _Node4 {
				t.Errorf("expected kind %s got %s", _Node4, got)
			}
		case 0:
			if tree.root != nil {
				t.Error("expected nil root")
			}
		}
	}
}

type largeKeyValueGenerator struct {
	cur       uint64
	generator func([]byte) []byte
	prefix    []byte
}

func NewLargeKeyValueGenerator(prefix []byte) *largeKeyValueGenerator {
	return &largeKeyValueGenerator{
		cur: 0,
		generator: func(input []byte) []byte {
			return input
		},
		prefix: prefix,
	}
}

func (g *largeKeyValueGenerator) get(cur uint64) (Key, ByteSliceValue) {
	prefixLen := len(g.prefix)
	var buf = make([]byte, prefixLen+8)
	copy(buf[:], g.prefix)
	binary.PutUvarint(buf[prefixLen:], cur)
	return buf, g.generator(buf)
}

func (g *largeKeyValueGenerator) prev() (Key, ByteSliceValue) {
	g.cur--
	k, v := g.get(g.cur)
	return k, v
}

func (g *largeKeyValueGenerator) next() (Key, ByteSliceValue) {
	k, v := g.get(g.cur)
	g.cur++
	return k, v
}

func (g *largeKeyValueGenerator) reset() {
	g.cur = 0
}

func (g *largeKeyValueGenerator) resetCur() {
	g.cur = 0
}

func TestArtTree_InsertOneAndDeleteOne(t *testing.T) {
	tree := NewArtTree()
	g := NewKeyValueGenerator()
	k, v := g.next()

	// insert one
	tree.Insert(k, v, "")

	// delete inserted
	deleted, oldValue := tree.Remove(k)
	if !bytes.Equal(oldValue.Value, v) {
		t.Errorf("got %v, want %v", oldValue, v)
	}
	if !deleted {
		t.Error("expected key to be deleted")
	}

	// should be not found
	got, _, found, _ := tree.FindExact(k)
	if got != nil {
		t.Errorf("expected nil value, got %v", got)
	}
	if found {
		t.Error("expected key not to be found")
	}

	// insert another one
	k, v = g.next()
	tree.Insert(k, v, "")

	// try to delete a non-exist key
	deleted, oldValue = tree.Remove(Key("wrong-key"))
	if oldValue != nil {
		t.Errorf("expected nil value, got %v", oldValue)
	}
	if deleted {
		t.Error("expected key not to be deleted")
	}
}

func TestArtTest_InsertAndDelete(t *testing.T) {
	tree := NewArtTree()
	g := NewKeyValueGenerator()
	// insert 1000
	N := 1_000_000
	if true { // underRaceDetector {
		N = 100
	}
	for i := 0; i < N; i++ {
		k, b := g.next()
		_ = tree.Insert(k, b, "")
	}
	g.resetCur()
	// check inserted kv
	for i := 0; i < N; i++ {
		k, v := g.next()
		got, _, found, _ := tree.FindExact(k)
		if !bytes.Equal(got, v) {
			t.Errorf("should insert key-value (%v:%v) but got %v", k, v, got)
		}
		if !found {
			t.Error("expected key to be found")
		}
	}
	g.resetCur()
	for i := 0; i < N; i++ {
		k, v := g.next()
		deleted, got := tree.Remove(k)
		if !bytes.Equal(got.Value, v) {
			t.Errorf("got %v, want %v", got, v)
		}
		if !deleted {
			t.Error("expected key to be deleted")
		}
	}
}

func TestArtTree_InsertLargeKeyAndDelete(t *testing.T) {
	tree := NewArtTree()
	g := NewLargeKeyValueGenerator([]byte("largeThanMax"))

	N := 1_000_000
	if true { // underRaceDetector {
		N = 100
	}
	for i := 0; i < N; i++ {
		k, b := g.next()
		_ = tree.Insert(k, b, "")
	}
	g.reset()
	// check inserted kv
	for i := 0; i < N; i++ {
		k, v := g.next()
		got, _, found, _ := tree.FindExact(k)
		if !bytes.Equal(got, v) {
			t.Errorf("should insert key-value (%v:%v)", k, v)
		}
		if !found {
			t.Error("expected key to be found")
		}
	}
	g.resetCur()
	for i := 0; i < N; i++ {
		k, v := g.next()
		deleted, got := tree.Remove(k)
		if !bytes.Equal(got.Value, v) {
			t.Errorf("got %v, want %v", got, v)
		}
		if !deleted {
			t.Error("expected key to be deleted")
		}
	}
}

// Benchmark
func loadTestFile(path string) [][]byte {
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

type KV struct {
	Key            []byte
	ByteSliceValue []byte
}

func TestTree_InsertWordSets(t *testing.T) {
	words := loadTestFile("./assets/words.txt")
	tree := NewArtTree()

	if true { // underRaceDetector {
		words = words[:500]
	}
	for _, w := range words {
		tree.Insert(w, w, "")
	}
	for i, w := range words {
		v, _, found, _ := tree.FindExact(w)
		if !found {
			t.Errorf("[run:%d] should found %s,but got %s", i, w, v)
		}
		if !bytes.Equal(v, w) {
			t.Errorf("[run:%d] should found %s,but got %s", i, w, v)
		}
	}

	for i, w := range words {
		deleted, v := tree.Remove(w)
		if !deleted {
			t.Errorf("[run:%d] should got %s,but got %s", i, w, v)
		}
		if !bytes.Equal(v.Value, w) {
			t.Errorf("[run:%d] should got %s,but got %s", i, w, v)
		}
	}
}

func Compare(a, b KV) bool {
	return bytes.Compare(a.Key, b.Key) < 0
}

func BenchmarkWordsArtInsert(b *testing.B) {
	words := loadTestFile("./assets/words.txt")
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		tree := NewArtTree()
		for _, w := range words {
			tree.Insert(w, w, "")
		}
	}
}

func BenchmarkWordsMapInsert(b *testing.B) {
	words := loadTestFile("./assets/words.txt")
	var strWords []string
	for _, word := range words {
		strWords = append(strWords, string(word))
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		m := make(map[string]string)
		for _, w := range strWords {
			m[w] = w
		}
	}
}

func TestDeleteConcurrentInsert(t *testing.T) {

	paths := loadTestFile("assets/linux.txt")
	n := len(paths)
	_ = n

	m := make(map[string]string)
	l := NewArtTree()
	for k := range paths {
		l.Insert(paths[k], paths[k], "")
		s := string(paths[k])
		m[s] = s
	}

	for i := 0; i <= 10; i++ {
		readFrac := float32(i) / 10.0
		_ = readFrac
		fmt.Println()
		fmt.Printf("on remove Frac %v\n", readFrac)
		rng := mathrand.New(mathrand.NewSource(time.Now().UnixNano()))
		for i := range 2 {
			fmt.Printf("%v ...", i)
			for k := range paths {
				s := string(paths[k])
				if rng.Float32() < readFrac {
					l.Remove(paths[k])
					delete(m, s)
				} else {
					l.Insert(paths[k], paths[k], "")
					m[s] = s
				}
			}
			if !mapTreeSame(m, l) {
				t.Fatal("map and tree different")
			}
		}
	}
}

func mapTreeSame(m map[string]string, tree *Tree) bool {
	nmap := len(m)
	ntree := tree.Size()
	if nmap != ntree {
		panic(fmt.Sprintf("nmap = %v; ntree = %v", nmap, ntree)) // 0,-1
	}
	for k := range m {
		_, _, found, _ := tree.FindExact([]byte(k))
		if !found {
			panic(fmt.Sprintf("in map: '%v'; not in tree", k))
		}
	}
	return true
}

func TestArtTree_SearchMod_GTE(t *testing.T) {
	tree := NewArtTree()
	tree.Insert(Key("sharedKey::1"), ByteSliceValue("value1"), "")
	tree.Insert(Key("sharedKey::1::created_at"), ByteSliceValue("created_at_value1"), "")

	v, _, found, _ := tree.FindGTE(Key("sharedKey::1"))
	if !found {
		t.Error("expected key to be found")
	}
	if got, want := string(v), string(ByteSliceValue("value1")); got != want {
		t.Errorf("got value %v, want %v", got, want)
	}

	v, _, found, _ = tree.FindGT(Key("sharedKey::1"))
	if !found {
		t.Error("expected key to be found")
	}
	if got, want := string(v), string(ByteSliceValue("created_at_value1")); got != want {
		t.Errorf("got value %v, want %v", got, want)
	}
}

func TestArtTree_SearchMod_GT_requires_backtracking(t *testing.T) {
	tree := NewArtTree()
	tree.Insert(Key("a14"), ByteSliceValue("a14"), "")
	tree.Insert(Key("b01"), ByteSliceValue("b01"), "")

	v, _, found, _ := tree.FindGT(Key("a14"))
	if !found {
		t.Error("expected key to be found")
	}
	if got, want := string(v), string(ByteSliceValue("b01")); got != want {
		t.Errorf("got value %v, want %v", got, want)
	}
}

func TestArtTree_SearchMod_LT_requires_backtracking(t *testing.T) {
	tree := NewArtTree()
	tree.Insert(Key("a14"), ByteSliceValue("a14"), "")
	tree.Insert(Key("b01"), ByteSliceValue("b01"), "")

	lf, _, found, _ := tree.FindLT(Key("b01"))
	if !found {
		t.Error("expected key to be found")
	}
	if got, want := string(lf), string(ByteSliceValue("a14")); got != want {
		t.Errorf("got value %v, want %v", got, want)
	}
}

func Test_808_ArtTree_SearchMod_big_GTE(t *testing.T) {

	// GTE should work on big trees

	tree := NewArtTree()
	//paths := loadTestFile("assets/linux.txt")
	// paths are not sorted.

	// check the tree against sorted paths
	sorted := loadTestFile("assets/linux.txt")
	sort.Sort(sliceByteSlice(sorted))

	// set true for quicker debugging
	smalltest := false
	if smalltest {
		sorted = sorted[:1200] // 306,600 green, 1200 red on i=302
		//sorted = sorted[93774:]
	}

	for i, w := range sorted {
		// only insert the evens, so we can search GTE on odds.
		if i%2 == 0 {
			if tree.Insert(w, w, "") {
				t.Fatalf("i=%v, could not add '%v', already in tree", i, string(w))
			}
		}
	}

	sz := tree.Size()
	if false {
		for i := 250; i < 320; i++ {
			if i%2 == 0 {
				fmt.Printf("sorted[%02d] %v      *in tree*\n", i, string(sorted[i]))
			} else {
				fmt.Printf("sorted[%02d] %v\n", i, string(sorted[i]))
			}
		}
	}
	var key []byte

	// do GTE the odd keys, expecting to get the next even.
	var wrong []int

	for i := 304; i < sz; i += 2 {

		key = sorted[i-1]
		lf, _, found := tree.Find(GTE, key)
		if !found {
			panic(fmt.Sprintf("could not find key GTE '%v' at i=%v", string(key), i))
		}

		lfkey := string(lf.Key)
		wanted := string(sorted[i])
		if lfkey != wanted {
			wrong = append(wrong, i)
			panic(fmt.Sprintf("on i = %v, wanted = '%v' but lfkey ='%v'", i, wanted, lfkey))
		}
	}

	// verify nil gives the first key in the tree in GTE
	lf, _, found := tree.Find(GTE, nil)
	if !found {
		panic("not found GTE nil key")
	}
	if bytes.Compare(lf.Key, sorted[0]) != 0 {
		panic("nil GTE search did not give first leaf")
	}
}

func Test_808_ArtTree_SearchMod_big_LT_only(t *testing.T) {
	// LT should work on big trees

	tree := NewArtTree()
	paths := loadTestFile("assets/linux.txt")
	// paths are not sorted.

	// check the tree against sorted paths
	sorted := loadTestFile("assets/linux.txt")
	sort.Sort(sliceByteSliceRev(sorted))

	for i, w := range paths {
		_ = i
		if tree.Insert(w, w, "") {
			t.Fatalf("i=%v, could not add '%v', already in tree", i, string(w))
		}
	}

	sz := tree.Size()
	prob := "../../torvalds/linux/virt"
	expect := "../../torvalds/linux/usr/initramfs_data.S"
	lf, _, found := tree.Find(LT, []byte(prob))
	if !found {
		panic("LT prob not found")
	}
	lfkey := string(lf.Key)
	if lfkey != expect {
		panic(fmt.Sprintf("want '%v'; got '%v'", expect, lfkey))
	}

	var key []byte
	for i := 0; i < sz; i++ {
		lf, _, found := tree.Find(LT, key)
		if !found {
			panic(fmt.Sprintf("could not find key LT '%v' at i=%v", string(key), i))
		}
		_ = lf
		lfkey := string(lf.Key)
		wanted := string(sorted[i])
		if lfkey != wanted {
			panic(fmt.Sprintf("on i = %v, wanted = '%v' but lfkey ='%v'", i, wanted, lfkey))
		}
		key = lf.Key
	}
}

func Test909_ArtTree_SearchMod_numbered_GTE(t *testing.T) {

	// GTE should work on big trees, but make them
	// numbered for ease of inspection.

	tree := NewArtTree()

	var sorted [][]byte
	for i := range 10_000 {
		sorted = append(sorted, []byte(fmt.Sprintf("%03d", i)))
	}
	sort.Sort(sliceByteSlice(sorted))

	for i, w := range sorted {
		// only insert the evens, so we can search GTE on odds.
		if i%2 == 0 {
			if tree.Insert(w, w, "") {
				t.Fatalf("i=%v, could not add '%v', already in tree", i, string(w))
			}
		}
	}

	sz := tree.Size()
	if false {
		for i := range sorted {
			if i%2 == 0 {
				fmt.Printf("sorted[%02d] %v      *in tree*\n", i, string(sorted[i]))
			} else {
				fmt.Printf("sorted[%02d] %v\n", i, string(sorted[i]))
			}
		}
	}
	var key []byte

	// do GTE the odd keys, expecting to get the next even.
	var wrong []int

	for i := 2; i < sz; i += 2 {

		key = sorted[i-1]
		lf, _, found := tree.Find(GTE, key)
		if !found {
			panic(fmt.Sprintf("could not find key GTE '%v' at i=%v", string(key), i))
		}

		lfkey := string(lf.Key)
		wanted := string(sorted[i])
		if lfkey != wanted {
			wrong = append(wrong, i)
			panic(fmt.Sprintf("on i = %v, wanted = '%v' but lfkey ='%v'", i, wanted, lfkey))
		}
	}

	// verify nil gives the first key in the tree in GTE
	lf, _, found := tree.Find(GTE, nil)
	if !found {
		panic("not found GTE nil key")
	}
	if bytes.Compare(lf.Key, sorted[0]) != 0 {
		panic("nil GTE search did not give first leaf")
	}
}

func Test505_ArtTree_SearchMod_random_numbered_GTE(t *testing.T) {

	// GTE should work on big trees, but make them
	// numbered for ease of inspection.

	// j=total number of leaves in the tree.
	for j := 1; j < 5000; j++ {

		tree := NewArtTree()

		var seed32 [32]byte
		chacha8 := mathrand2.NewChaCha8(seed32)

		var sorted [][]byte
		var N uint64 = 100000 // domain for leaf keys.

		// j = number of leaves in the tree.

		smallest := int(N + 1)
		used := make(map[int]bool) // tree may dedup, but sorted needs too.
		for range j {
			r := int(chacha8.Uint64() % N)
			// disallow 0 so we can query with it,
			// knowing it is not in the tree; and
			// that smallerThanSmallestInTree will not be negative.
			if r == 0 || used[r] {
				continue
			}
			if r < smallest {
				smallest = r
			}
			used[r] = true
			sorted = append(sorted, []byte(fmt.Sprintf("%06d", r)))
		}
		sort.Sort(sliceByteSlice(sorted))

		smallerThanSmallestInTree := []byte(fmt.Sprintf("%06d", smallest-1))
		zeroKey := []byte(fmt.Sprintf("%06d", 0))

		for i, w := range sorted {
			// only insert the evens, so we can search GTE on odds.
			if i%2 == 0 {
				if tree.Insert(w, w, "") {
					// make sure leaves are unique.
					t.Fatalf("i=%v, could not add '%v', already in tree", i, string(w))
				}
			}
		}

		sz := tree.Size()
		showlist := func(want int, got string) {

			for i, nd := range sorted {
				extra := ""
				ssi := string(sorted[i])
				if i == want {
					extra += " <<< want!"
				}
				if ssi == got {
					extra += " <<< got!"
				}
				if i%2 == 0 {
					fmt.Printf("%p sorted[%02d] %v      *in tree*  %v\n", nd, i, ssi, extra)
				} else {
					fmt.Printf("%p sorted[%02d] %v  %v\n", nd, i, ssi, extra)
				}
			}
		}
		_ = showlist

		var key []byte

		// do GTE the odd keys, expecting to get the next even.
		var wrong []int

		// check the GTE(key) when key < key0 (smallest in tree).
		for kk := range 2 {
			query := smallerThanSmallestInTree
			if kk == 1 {
				query = zeroKey
			}
			lf, idx, found := tree.Find(GTE, query)
			if found && idx == 0 &&
				bytes.Equal(lf.Key, sorted[0]) {
				// good. correct. okay.
			} else {
				if lf == nil {
					panic(fmt.Sprintf("bad GTE smaller-than-smallest response! want key='%v', found=%v (wanted true); idx=%v (wanted 0)", string(sorted[0]), found, idx))
				} else {
					//vv("sz = %v", tree.Size())
					panic(fmt.Sprintf("bad GTE smaller-than-smallest response! want key='%v', got='%v'; found=%v; idx=%v", string(sorted[0]), string(lf.Key), found, idx))
				}
			}
		}

		for i := 2; i < sz; i += 2 {

			key = sorted[i-1]
			lf, _, found := tree.Find(GTE, key)
			if !found {
				panic(fmt.Sprintf("could not find key GTE '%v' at i=%v", string(key), i))
			}
			wanted := string(sorted[i])
			if lf == nil {
				wrong = append(wrong, i)

				panic(fmt.Sprintf("on i = %v, (GTE key '%v') wanted = '%v' but lf was nil", i, string(key), wanted))
			}
			lfkey := string(lf.Key)
			if lfkey != wanted {
				wrong = append(wrong, i)

				if false {
					fmt.Printf("\n\n======= problem! now debugging with a Search2 call! ======\n\n")

					regularSearchLeaf, _, regularFound, _ := tree.FindExact(sorted[i])
					_ = regularSearchLeaf
					_ = regularFound
				}

				panic(fmt.Sprintf("on j=%v; i = %v, (GTE key '%v') wanted = '%v' but lfkey ='%v'", j, i, string(key), wanted, lfkey))
			}
		}

		// verify nil gives the first key in the tree in GTE
		lf, _, found := tree.Find(GTE, nil)
		if !found {
			panic("not found GTE nil key")
		}
		if bytes.Compare(lf.Key, sorted[0]) != 0 {
			panic("nil GTE search did not give first leaf")
		}
	}
}

func Test506_ArtTree_SearchMod_random_numbered_GT_(t *testing.T) {
	// j=total number of leaves in the tree.
	for j := 1; j < 5000; j++ {
		tree := NewArtTree()

		// bump up from 0 to test query 0,1 not in tree.
		first := 2

		// Generate sorted keys.
		sorted := make([][]byte, 0, j)
		for i := 0; i < j; i++ {
			if i%2 == 0 {
				k := fmt.Sprintf("%06d", i)
				sorted = append(sorted, []byte(k))

				if i >= first {
					tree.Insert(Key(k), ByteSliceValue(k), "")
				}
			} else {
				k := fmt.Sprintf("%06d", i)
				sorted = append(sorted, []byte(k))
			}
		}

		sz := tree.Size()
		showlist := func(want int, got string) {
			for i, nd := range sorted {
				extra := ""
				ssi := string(sorted[i])
				if i == want {
					extra += " <<< want!"
				}
				if ssi == got {
					extra += " <<< got!"
				}
				if i%2 == 0 {
					fmt.Printf("%p sorted[%02d] %v      *in tree*  %v\n", nd, i, ssi, extra)
				} else {
					fmt.Printf("%p sorted[%02d] %v  %v\n", nd, i, ssi, extra)
				}
			}
		}

		var key []byte
		var wrong []int

		lim := sz - 2
		for i := 0; i < lim; i++ {
			var wanted string
			var wanti int
			if i%2 == 0 {
				wanti = i + 2
			} else {
				wanti = i + 1
			}
			wanted = string(sorted[wanti])

			key = sorted[i]
			lf, _, found := tree.Find(GT, key)
			if !found {
				showlist(wanti, "")
				panic(fmt.Sprintf("could not find key GT '%v' at i=%v", string(key), i))
			}

			if lf == nil {
				wrong = append(wrong, i)
				panic(fmt.Sprintf("on i = %v, (GT key '%v') wanted = '%v' but lf was nil", i, string(key), wanted))
			}
			lfkey := string(lf.Key)
			if lfkey != wanted {
				wrong = append(wrong, i)
				panic(fmt.Sprintf("on j=%v; i = %v, (GT key '%v') wanted = '%v' but lfkey ='%v'", j, i, string(key), wanted, lfkey))
			}
		}

		// verify nil gives the first key in the tree in GT
		lf, _, found := tree.Find(GT, nil)
		if sz == 0 {
			if found || lf != nil {
				t.Fatal("should not find a leaf in an empty tree")
			}
			continue
		}
		if !found {
			t.Fatalf("expected to find first key with nil GT search; j=%v;", j)
		}
		if lf == nil {
			t.Fatal("expected non-nil leaf for nil GT search")
		}
		firstKey := sorted[first] // First even index since odds aren't in tree
		if !bytes.Equal(lf.Key, firstKey) {
			t.Fatalf("expected first key %v but got %v", string(firstKey), string(lf.Key))
		}
	}
}

func Test507_ArtTree_SearchMod_random_numbered_LTE(t *testing.T) {
	// j=total number of leaves in the tree.
	for j := 1; j < 5000; j++ {
		tree := NewArtTree()

		largestIdx := -1
		// Generate sorted keys
		sorted := make([][]byte, 0, j)
		inTree := make([][]byte, 0, j)
		for i := 0; i < j; i++ {
			if i%2 == 0 {
				k := fmt.Sprintf("%06d", i)
				sorted = append(sorted, []byte(k))
				tree.Insert(Key(k), ByteSliceValue(k), "")
				largestIdx++
				inTree = append(inTree, []byte(k))
			} else {
				k := fmt.Sprintf("%06d", i)
				sorted = append(sorted, []byte(k))
			}
		}

		larger1 := fmt.Sprintf("%06d", j)
		larger2 := fmt.Sprintf("%06d", j+1)

		sz := tree.Size()
		showlist := func(want int, got string) {
			for i, nd := range sorted {
				extra := ""
				ssi := string(sorted[i])
				if i == want {
					extra += " <<< want!"
				}
				if ssi == got {
					extra += " <<< got!"
				}
				if i%2 == 0 {
					fmt.Printf("%p sorted[%02d] %v      *in tree*  %v\n", nd, i, ssi, extra)
				} else {
					fmt.Printf("%p sorted[%02d] %v  %v\n", nd, i, ssi, extra)
				}
			}
		}

		var key []byte
		var wrong []int

		for i := 0; i < sz-1; i += 2 {
			key = sorted[i+1]
			lf, _, found := tree.Find(LTE, key)
			if !found {
				showlist(i, "")
				panic(fmt.Sprintf("could not find key LTE '%v' at i=%v", string(key), i))
			}

			wanted := string(sorted[i])
			if lf == nil {
				wrong = append(wrong, i)
				panic(fmt.Sprintf("on i = %v, (LTE key '%v') wanted = '%v' but lf was nil", i, string(key), wanted))
			}
			lfkey := string(lf.Key)
			if lfkey != wanted {
				wrong = append(wrong, i)
				panic(fmt.Sprintf("on j=%v; i = %v, (LTE key '%v') wanted = '%v' but lfkey ='%v'", j, i, string(key), wanted, lfkey))
			}
		}

		// verify nil gives the last key in the tree in LTE
		lf, _, found := tree.Find(LTE, nil)
		if !found {
			t.Fatal("expected to find last key with nil LTE search")
		}
		if lf == nil {
			t.Fatal("expected non-nil leaf for nil LTE search")
		}

		// verify LTE queries for keys larger than the
		// largest in the tree return the last key.
		if sz > 0 {

			for kk := range 2 {
				query := []byte(larger1)
				if kk > 0 {
					query = []byte(larger2)
				}

				lf, idx, found := tree.Find(LTE, query)
				if found && lf != nil && idx == largestIdx {
					// good.
				} else {
					//vv("tree = %v", tree)
					//vv("lf='%v'; idx=%v; found=%v; largestIdx=%v", lf, idx, found, largestIdx)
					showlist(-1, string(lf.Key))
					panic(fmt.Sprintf("could not find key LTE '%v' at j=%v", string(query), j))
				}

				wanted := string(inTree[len(inTree)-1])
				lfkey := string(lf.Key)
				if lfkey != wanted {
					//vv("tree = %v", tree)
					panic(fmt.Sprintf("on j=%v; (LTE key '%v') wanted = '%v' but lfkey ='%v'; ", j, string(query), wanted, lfkey))
				}
			}
		}
	}
}

func Test508_ArtTree_SearchMod_random_numbered_LT_(t *testing.T) {
	// j=total number of leaves in the tree.
	for j := 1; j < 5000; j++ {
		tree := NewArtTree()

		largestIdx := -1
		// Generate sorted keys
		sorted := make([][]byte, 0, j)
		inTree := make([][]byte, 0, j)
		for i := 0; i < j; i++ {
			if i%2 == 0 {
				k := fmt.Sprintf("%06d", i)
				sorted = append(sorted, []byte(k))
				tree.Insert(Key(k), ByteSliceValue(k), "")
				largestIdx++
				inTree = append(inTree, []byte(k))
			} else {
				k := fmt.Sprintf("%06d", i)
				sorted = append(sorted, []byte(k))
			}
		}

		larger1 := fmt.Sprintf("%06d", j)
		larger2 := fmt.Sprintf("%06d", j+1)

		sz := tree.Size()
		showlist := func(want int, got string) {
			for i, nd := range sorted {
				extra := ""
				ssi := string(sorted[i])
				if i == want {
					extra += " <<< want!"
				}
				if ssi == got {
					extra += " <<< got!"
				}
				if i%2 == 0 {
					fmt.Printf("%p sorted[%02d] %v      *in tree*  %v\n", nd, i, ssi, extra)
				} else {
					fmt.Printf("%p sorted[%02d] %v  %v\n", nd, i, ssi, extra)
				}
			}
		}

		var key []byte
		var wrong []int

		lim := sz - 2
		for i := 2; i < lim; i++ {
			var wanted string
			var wanti int
			if i%2 == 0 {
				wanti = i - 2
			} else {
				wanti = i - 1
			}
			wanted = string(sorted[wanti])

			key = sorted[i]
			lf, _, found := tree.Find(LT, key)
			if !found {
				showlist(wanti, "")
				panic(fmt.Sprintf("could not find key LT '%v' at i=%v", string(key), i))
			}

			if lf == nil {
				panic(fmt.Sprintf("on i = %v, (LT key '%v') wanted = '%v' but lf was nil", i, string(key), wanted))
			}
			lfkey := string(lf.Key)
			if lfkey != wanted {
				wrong = append(wrong, i)
				panic(fmt.Sprintf("on j=%v; i = %v, (LT key '%v') wanted = '%v' but lfkey ='%v'", j, i, string(key), wanted, lfkey))
			}
		}

		// verify nil gives the first key in the tree in LT
		lf, _, found := tree.Find(LT, nil)
		if !found {
			t.Fatal("expected to find first key with nil LT search")
		}
		if lf == nil {
			t.Fatal("expected non-nil leaf for nil LT search")
		}

		// verify LT queries for keys larger than the
		// largest in the tree return the last key.
		if sz > 0 {

			for kk := range 2 {
				query := []byte(larger1)
				if kk > 0 {
					query = []byte(larger2)
				}

				lf, idx, found := tree.Find(LT, query)
				if found && lf != nil && idx == largestIdx {
					// good.
				} else {
					//vv("tree = %v", tree)
					//vv("lf='%v'; idx=%v; found=%v; largestIdx=%v", lf, idx, found, largestIdx)
					showlist(-1, string(lf.Key))
					panic(fmt.Sprintf("could not find key LT '%v' at j=%v", string(query), j))
				}

				wanted := string(inTree[len(inTree)-1])
				lfkey := string(lf.Key)
				if lfkey != wanted {
					//vv("tree = %v", tree)
					panic(fmt.Sprintf("on j=%v; (LTE key '%v') wanted = '%v' but lfkey ='%v'; ", j, string(query), wanted, lfkey))
				}
			}
		}
	}
}

// SubN updates
func Test510_SubN_maintained_for_At_indexing_(t *testing.T) {

	// j=total number of leaves in the tree.
	for j := 1; j < 1000; j++ {

		tree := NewArtTree()

		var seed32 [32]byte
		chacha8 := mathrand2.NewChaCha8(seed32)

		var sorted [][]byte
		var N uint64 = 100000 // domain for leaf keys.

		// j = number of leaves in the tree.

		used := make(map[int]bool) // tree may dedup, but sorted needs too.
		for range j {
			r := int(chacha8.Uint64() % N)
			if used[r] {
				continue
			}
			used[r] = true
			sorted = append(sorted, []byte(fmt.Sprintf("%06d", r)))
		}
		sort.Sort(sliceByteSlice(sorted))

		var lastLeaf *Leaf
		_ = lastLeaf
		for i, w := range sorted {

			key2 := Key(append([]byte{}, w...))
			lf := NewLeaf(key2, key2, "")
			if tree.InsertLeaf(lf) {
				// make sure leaves are unique.
				t.Fatalf("i=%v, could not add '%v', already in tree", i, string(w))
			}
			lastLeaf = lf

			// after each insert, verify correct SubN counts.
			verifySubN(tree.root)
		}

		sz := tree.Size()

		var key []byte

		for i := range sz {
			key = sorted[i]
			tree.Remove(key)

			//vv("at sz = %v; i=%v;j=%v", tree.Size(), i, j)
			// tree_test.go:1598 2025-03-11 02:23:25.347 -0500 CDT at sz = 2; i=0;j=3  detects panic: leafcount=2, but n.SubN = 3; node=' 0xc000012370 node4, key '(zero)' childkeys: ['0', '8'] (treedepth 0) compressed='0' path='(paths commented out atm)' (subN: 3; pren: na)

			// after each delete, verify correct SubN counts.
			if tree.root != nil {
				verifySubN(tree.root)
			}
		}
	}
}

// verifySubN:
// walk through the subtree at root, counting children.
// At each inner node, verify that SubN
// has an accurate count of children, or panic.
func verifySubN(root *bnode) (leafcount int) {

	if root == nil {
		panic("root should never be nil")
		return 0
	}
	if root.isLeaf {
		return 1
	} else {

		inode := root.inner.Node
		switch n := inode.(type) {
		case *node4:
			for i := range n.children {
				if i < n.lth {
					leafcount += verifySubN(n.children[i])
				}
			}
		case *node16:
			for i := range n.children {
				if i < n.lth {
					leafcount += verifySubN(n.children[i])
				}
			}
		case *node48:
			for _, k := range n.keys {

				if k == 0 {
					continue
				}
				child := n.children[k-1]
				leafcount += verifySubN(child)
			}
		case *node256:
			for _, child := range n.children {
				if child != nil {
					leafcount += verifySubN(child)
				}
			}
		}

		if root.inner.SubN != leafcount {
			panic(fmt.Sprintf("leafcount=%v, but n.SubN = %v; node='%v'", leafcount, root.inner.SubN, root))
		}
		if leafcount == 0 {
			panic("leafcount of 0?")
		}
		if root.inner.SubN == 0 {
			panic("root.inner.SubN of 0?")
		}
	}
	return leafcount
}

func verifyPren(b *bnode) (leafcount int) {

	return // pren is lazy not eager now

	if b == nil {
		return 0
	}
	if b.isLeaf {
		return 1
	} else {
		var pren int
		var subn int

		inode := b.inner.Node
		switch n := inode.(type) {
		case *node4:
			for i, ch := range n.children {
				if i < n.lth {
					subn = verifyPren(n.children[i])
					leafcount += subn

					if ch.pren != pren {
						panic(fmt.Sprintf("%p n4 pren is off: child.pren = %v; manual pren=%v", ch, ch.pren, pren))
					}
					pren += subn
				}
			}
		case *node16:
			for i, ch := range n.children {
				if i < n.lth {
					subn = verifyPren(n.children[i])
					leafcount += subn

					if ch.pren != pren {
						panic(fmt.Sprintf("%p n16 pren is off: child.pren = %v; manual pren=%v;\n ch = '%v'", ch, ch.pren, pren, ch))

					}
					pren += subn
				}
			}
		case *node48:
			for _, k := range n.keys {

				if k == 0 {
					continue
				}
				child := n.children[k-1]
				subn = verifyPren(child)
				leafcount += subn
				if child.pren != pren {
					panic(fmt.Sprintf("%p n48 pren is off: child.pren = %v; manual pren=%v", child, child.pren, pren))
				}
				pren += subn
			}
		case *node256:
			for _, child := range n.children {
				if child != nil {
					subn = verifyPren(child)
					leafcount += subn
					if child.pren != pren {
						panic(fmt.Sprintf("%p n256 pren is off: child.pren = %v; manual pren=%v", child, child.pren, pren))
					}
					pren += subn
				}
			}
		}

		if b.inner.SubN != leafcount {
			panic(fmt.Sprintf("leafcount=%v, but n.SubN = %v", leafcount, b.inner.SubN))
		}
	}
	return leafcount
}

// At(i) returns the ith-element in the sorted tree.
func Test511_At_index_the_tree_like_an_array(t *testing.T) {

	// j=total number of leaves in the tree.
	for j := 1; j < 500; j++ {

		tree := NewArtTree()

		var seed32 [32]byte
		chacha8 := mathrand2.NewChaCha8(seed32)

		var sorted [][]byte
		var N uint64 = 100000 // domain for leaf keys.

		// j = number of leaves in the tree.

		used := make(map[int]bool) // tree may dedup, but sorted needs too.
		for range j {
			r := int(chacha8.Uint64() % N)
			if used[r] {
				continue
			}
			used[r] = true
			sorted = append(sorted, []byte(fmt.Sprintf("%06d", r)))
		}
		sort.Sort(sliceByteSlice(sorted))

		var lastLeaf *Leaf
		_ = lastLeaf
		for i, w := range sorted {

			key2 := Key(append([]byte{}, w...))
			lf := NewLeaf(key2, key2, "")
			if tree.InsertLeaf(lf) {
				// make sure leaves are unique.
				t.Fatalf("i=%v, could not add '%v', already in tree", i, string(w))
			}
			lastLeaf = lf
		}

		sz := tree.Size()

		for i := range sz {
			lf, ok := tree.At(i)
			if !ok {
				panic(fmt.Sprintf("missing leaf!?! j=%v; i=%v not ok", j, i))
			}
			// test Atv too.
			val, ok2 := tree.Atv(i)
			if !ok2 {
				panic(fmt.Sprintf("missing leaf!?! j=%v; i=%v not ok2", j, i))
			}
			got := string(lf.Key)
			want := string(sorted[i])
			if got != want {
				panic(fmt.Sprintf("at j=%v; i=%v, want '%v'; got '%v'", j, i, want, got))
			}
			got2 := string(val)
			if got2 != want {
				panic(fmt.Sprintf("at j=%v; i=%v, want '%v'; got2 '%v'", j, i, want, got2))
			}
		}
	}
}

// LeafIndex and At(i) are inverses.
// At(j) returns the j-th leaf in the sorted tree.
// Then calling idx, ok := tree.LeafIndex(leaf)
// should give back j. Test this holds.
func Test512_LeafIndex_inverse_of_At(t *testing.T) {

	// j=total number of leaves in the tree.
	//for j := 1; j < 1000; j++ { // 10_000 => 42 sec (sans removes)
	// 1000 => 28 sec with removes.
	for j := 1; j < 500; j++ { // 0.10 sec, 3.5 sec with removes.

		//if j%100 == 0 {
		//	//vv("on j = %v", j)
		//}

		tree := NewArtTree()

		var seed32 [32]byte
		chacha8 := mathrand2.NewChaCha8(seed32)

		var sorted [][]byte
		var N uint64 = 100000 // domain for leaf keys.

		// j = number of leaves in the tree.

		used := make(map[int]bool) // tree may dedup, but sorted needs too.
		for range j {
			r := int(chacha8.Uint64() % N)
			if used[r] {
				continue
			}
			used[r] = true
			sorted = append(sorted, []byte(fmt.Sprintf("%06d", r)))
		}
		sort.Sort(sliceByteSlice(sorted))

		var lastLeaf *Leaf
		_ = lastLeaf
		for i, w := range sorted {

			key2 := Key(append([]byte{}, w...))
			lf := NewLeaf(key2, key2, "")
			if tree.InsertLeaf(lf) {
				// make sure leaves are unique.
				t.Fatalf("i=%v, could not add '%v', already in tree", i, string(w))
			}
			lastLeaf = lf
		}

		//vv("verifying SubN after removal")
		sz := tree.Size()

		//vv("starting tree = '%v'", tree)

		for i := range sz {
			lf, ok := tree.At(i)
			if !ok {
				panic(fmt.Sprintf("missing leaf!?! j=%v; i=%v not ok", j, i))
			}
			j, ok := tree.LeafIndex(lf)
			if !ok || j != i {
				t.Fatalf("want ok=true, j=i=%v; got ok=%v; j=%v", i, ok, j)
			}
			// test Atv too.
			val, ok2 := tree.Atv(i)
			if !ok2 {
				panic(fmt.Sprintf("missing leaf!?! j=%v; i=%v not ok2", j, i))
			}
			got := string(lf.Key)
			want := string(sorted[i])
			if got != want {
				panic(fmt.Sprintf("at j=%v; i=%v, want '%v'; got '%v'", j, i, want, got))
			}
			got2 := string(val)
			if got2 != want {
				panic(fmt.Sprintf("at j=%v; i=%v, want '%v'; got2 '%v'", j, i, want, got2))
			}
		}
		// must also verify after removes
		for sz > 3 {
			lf, ok := tree.At(sz - 1)
			if !ok {
				//panic("not okay?")
				continue
			}
			tree.Remove(lf.Key)
			sz = tree.Size()
			for i := range sz {
				lf, ok := tree.At(i)
				if !ok {
					panic(fmt.Sprintf("missing leaf!?! j=%v; i=%v not ok", j, i))
				}
				j, ok := tree.LeafIndex(lf)
				if !ok || j != i {
					t.Fatalf("want ok=true, j=i=%v; got ok=%v; j=%v", i, ok, j)
				}
			}
		}

	}
}

func TestArtTree777_FindGT_key_before_keys_in_tree(t *testing.T) {
	tree := NewArtTree()
	tree.Insert(Key("a14"), ByteSliceValue("a14"), "")
	tree.Insert(Key("b01"), ByteSliceValue("b01"), "")

	lf, _, found := tree.Find(GT, Key("a10"))
	//vv("lf back from Find(GT,'a10') = '%v'", lf)
	if !found {
		t.Error("expected key to be found")
	}
	v := lf.Value
	if got, want := string(v), "a14"; got != want {
		t.Errorf("got value %v, want %v", got, want)
	}

	// also GTE

	v, _, found, _ = tree.FindGT(Key("a10"))
	if !found {
		t.Error("expected key to be found")
	}
	if got, want := string(v), string(ByteSliceValue("a14")); got != want {
		t.Errorf("got value %v, want %v", got, want)
	}

}

func TestArtTree777_FindLT_key_after_keys_in_tree(t *testing.T) {
	tree := NewArtTree()
	tree.Insert(Key("a14"), ByteSliceValue("a14"), "")
	tree.Insert(Key("b01"), ByteSliceValue("b01"), "")

	v, _, found, _ := tree.FindLT(Key("c00"))
	if !found {
		t.Error("expected key to be found")
	}
	if got, want := string(v), string(ByteSliceValue("b01")); got != want {
		t.Errorf("got value %v, want %v", got, want)
	}

	// also LTE

	v, _, found, _ = tree.FindLTE(Key("c00"))
	if !found {
		t.Error("expected key to be found")
	}
	if got, want := string(v), "b01"; got != want {
		t.Errorf("got value %v, want %v", got, want)
	}

}

func Test600_fuzz_compare_random_insert_delete_to_map(t *testing.T) {

	// do random insertions, reads, and deletions and
	// confirm with a standard Go map that we match
	// the results.

	// M = total number of leaves in the starting tree.
	// K = number of random operations (get, insert, del) to do.
	M := 100
	K := 10_000_000

	tree := NewArtTree()

	var seed32 [32]byte
	chacha8 := mathrand2.NewChaCha8(seed32)

	var N uint64 = 100000 // domain for leaf keys.

	var sorted [][]byte
	used := make(map[int]bool)
	gomap := make(map[string]bool)
	j := 0
	_ = j
	for len(used) < M {
		r := int(chacha8.Uint64() % N)
		if used[r] {
			continue
		}
		used[r] = true

		key := fmt.Sprintf("%06d", r)
		gomap[key] = true
		key1 := []byte(key)
		sorted = append(sorted, key1)

		key2 := append([]byte{}, key1...)

		if tree.Insert(key2, []byte(fmt.Sprintf("%v", r)), "") {
			t.Fatalf("r=%v, could not add '%v', already in tree", r, string(key2))
		}
		j++
	}
	sort.Sort(sliceByteSlice(sorted))

	var key []byte

	for k := range K {

		i := int(chacha8.Uint64() % uint64(M))
		key = sorted[i]

		op := int(chacha8.Uint64() % 3)
		switch op {
		case 0:
			// read
			_, _, found, _ := tree.FindExact(key)
			found2 := gomap[string(key)]
			if found != found2 {
				panic(fmt.Sprintf("k=%v; found=%v; found2=%v", k, found, found2))
			}
		case 1:
			// insert
			gomap[string(key)] = true
			tree.Insert(key, key, "")
			if tree.Size() != len(gomap) {
				panic(fmt.Sprintf("after insert: tree.Size() = %v, versus len(gomap) = %v", tree.Size(), len(gomap)))
			}
		case 2:
			// delete
			tree.Remove(key)
			delete(gomap, string(key))
			if tree.Size() != len(gomap) {
				panic(fmt.Sprintf("after delete: tree.Size() = %v, versus len(gomap) = %v", tree.Size(), len(gomap)))
			}
		}

	}
}

type Kint struct {
	Key []byte
	Val int
}

func Test620_unlocked_read_comparison(t *testing.T) {

	// with data already in, how fast are we vs a map?

	// K = total number of keys (leaves) in the starting tree.
	// nothing fancy, just sequential integers -> strings.
	K := 10_000_000

	var keys []string
	var keyb [][]byte
	for k := range K {
		key := fmt.Sprintf("%09d", k)
		keys = append(keys, key)
		keyb = append(keyb, []byte(key))
	}

	tree := NewArtTree()
	tree.SkipLocking = true

	gomap := make(map[string]int)
	t0 := time.Now()
	for k, key := range keys {
		gomap[key] = k
	}
	e0 := time.Since(t0)
	rate0 := e0 / time.Duration(K)
	fmt.Printf("map time to store %v keys: %v (%v/op)\n", K, e0, rate0)

	t0 = time.Now()
	for k, v := range gomap {
		_, _ = k, v
		//if keys[v] != k {
		//	panic(fmt.Sprintf("gomap gave %v instead of %v", k, keys[v]))
		//}
	}
	e0 = time.Since(t0)
	rate0 = e0 / time.Duration(K)
	fmt.Printf("map reads %v keys: elapsed %v (%v/op)\n", K, e0, rate0)

	// deletes in gomap
	t1 := time.Now()
	for _, ks := range keys {
		delete(gomap, ks)
	}
	e1 := time.Since(t1)
	rate1 := e1 / time.Duration(K)
	fmt.Printf("map deletes %v keys: elapsed %v (%v/op)\n", K, e1, rate1)

	t1 = time.Now()
	for k, kb := range keyb {
		tree.Insert(kb, []byte(fmt.Sprintf("%v", k)), "")
	}
	e1 = time.Since(t1)
	rate1 = e1 / time.Duration(K)
	fmt.Printf("uart.Tree time to store %v keys: %v (%v/op)\n", K, e1, rate1)

	t1 = time.Now()
	for lf := range Ascend(tree, nil, nil) {
		_ = lf
	}
	e1 = time.Since(t1)
	rate1 = e1 / time.Duration(K)
	fmt.Printf("Ascend(tree) reads %v keys: elapsed %v (%v/op)\n", K, e1, rate1)

	// try the native iterator instead of iter.Seq

	it := tree.Iter(nil, nil)
	t1 = time.Now()
	for it.Next() {
		_ = it.Key
	}
	e1 = time.Since(t1)
	rate1 = e1 / time.Duration(K)
	fmt.Printf("uart Iter() reads %v keys: elapsed %v (%v/op)\n", K, e1, rate1)

	// and the integer indexing:

	t1 = time.Now()
	var lf *Leaf
	var ok bool
	var v int
	for i := range K {
		lf, ok = tree.At(i)
		v, _ = strconv.Atoi(string(lf.Value))
		if !ok || v != i {
			panic(fmt.Sprintf("At(i=%v) gave %v instead of %v", i, v, i))
		}

	}
	e1 = time.Since(t1)
	rate1 = e1 / time.Duration(K)
	fmt.Printf("tree.At(i) reads %v keys: elapsed %v (%v/op)\n", K, e1, rate1)

	// we would like sequential iteration from
	// larger than 0 to work too. start from 10.
	t1 = time.Now()
	beg := 10
	for i := beg; i < K; i++ {
		lf, ok = tree.At(i)
		v, _ = strconv.Atoi(string(lf.Value))
		if !ok || v != i {
			panic(fmt.Sprintf("At(i=%v) gave %v instead of %v", i, v, i))
		}

	}
	e1 = time.Since(t1)
	rate1 = e1 / time.Duration(K)
	fmt.Printf("tree.At(i) reads from %v: %v keys: elapsed %v (%v/op)\n", beg, K-beg, e1, rate1)

	// Atfar should work the same as un-cached At
	t1 = time.Now()
	for i := range K {
		lf, ok = tree.Atfar(i)
		v, _ = strconv.Atoi(string(lf.Value))
		if !ok || v != i {
			panic(fmt.Sprintf("Atfar(i=%v) gave %v instead of %v", i, v, i))
		}

	}
	e1 = time.Since(t1)
	rate1 = e1 / time.Duration(K)
	fmt.Printf("tree.Atfar(i) reads %v keys: elapsed %v (%v/op)\n", K, e1, rate1)

	// with locking on
	tree.SkipLocking = false
	t1 = time.Now()
	for k := range K {
		tree.Atfar(k)
	}
	e1 = time.Since(t1)
	rate1 = e1 / time.Duration(K)
	fmt.Printf("Atfar() read-locked reads %v keys: elapsed %v (%v/op)\n", K, e1, rate1)

	// deletes in ART
	t1 = time.Now()
	for _, kb := range keyb {
		tree.Remove(kb)
	}
	e1 = time.Since(t1)
	rate1 = e1 / time.Duration(K)
	fmt.Printf("my ART: delete %v keys: elapsed %v (%v/op)\n", K, e1, rate1)

	// google/btree load and read

	degree := 3_000 // fastest

	g := googbtree.NewG[*Kint](degree, googbtree.LessFunc[*Kint](func(a, b *Kint) bool {
		return bytes.Compare(a.Key, b.Key) < 0
	}))

	t1 = time.Now()
	for k, kb := range keyb {
		kint := &Kint{
			Key: kb,
			Val: k,
		}
		//g.ReplaceOrInsert(ks)
		g.ReplaceOrInsert(kint)
	}
	e1 = time.Since(t1)
	rate1 = e1 / time.Duration(K)
	fmt.Printf("google/btree time to store %v keys: %v (%v/op)\n", K, e1, rate1)

	t1 = time.Now()
	g.Ascend(func(kint *Kint) bool { return true })

	e1 = time.Since(t1)
	rate1 = e1 / time.Duration(K)
	fmt.Printf("google/btree reads %v keys: elapsed %v (%v/op)\n", K, e1, rate1)

	// deletes
	query := &Kint{}
	t1 = time.Now()
	for k, kb := range keyb {
		_ = k
		query.Key = kb

		goner, gone := g.Delete(query)
		if !ok || 0 != bytes.Compare(goner.Key, kb) {
			panic(fmt.Sprintf("gone=%v; Delete should have returned '%v', got '%v'", gone, string(kb), string(goner.Key)))
		}
	}

	e1 = time.Since(t1)
	rate1 = e1 / time.Duration(K)
	fmt.Printf("google/btree delete %v keys: elapsed %v (%v/op)\n", K, e1, rate1)

}

/* titrate google/btree degree
degree 30
google/btree time to store 10000000 keys: 2.267972469s (226ns/op)
google/btree reads 10000000 keys: elapsed 41.859876ms (4ns/op)

degree 300
google/btree time to store 10000000 keys: 1.982752059s (198ns/op)
google/btree reads 10000000 keys: elapsed 30.67029ms (3ns/op)

degree 2000
google/btree time to store 10000000 keys: 1.933673346s (193ns/op)
google/btree reads 10000000 keys: elapsed 29.448824ms (2ns/op)

degree 3000 <<<<< seems like the sweet spot.
google/btree time to store 10000000 keys: 1.846107171s (184ns/op)
google/btree reads 10000000 keys: elapsed 26.261723ms (2ns/op)
google/btree time to store 10000000 keys: 1.924329849s (192ns/op)
google/btree reads 10000000 keys: elapsed 26.404445ms (2ns/op)

degree 5_000
google/btree time to store 10000000 keys: 1.992577096s (199ns/op)
google/btree reads 10000000 keys: elapsed 27.08961ms (2ns/op)

degree 10_000
google/btree time to store 10000000 keys: 2.032974012s (203ns/op)
google/btree reads 10000000 keys: elapsed 25.954454ms (2ns/op)

degree 15_000
google/btree time to store 10000000 keys: 1.953724369s (195ns/op)
google/btree reads 10000000 keys: elapsed 27.160363ms (2ns/op)

degree 30_000
google/btree time to store 10000000 keys: 1.858228497s (185ns/op)
google/btree reads 10000000 keys: elapsed 24.901334ms (2ns/op)

*/
