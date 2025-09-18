package art

import (
	"bufio"
	"bytes"
	"fmt"
	"time"
	//"bytes"
	//"encoding/binary"
	"github.com/stretchr/testify/assert"
	//"github.com/tidwall/btree"
	"os"
	"testing"
)

// saves the leaves to disk.
func TestTree_SaverSimple(t *testing.T) {
	tree := NewArtTree()
	// insert one key
	tree.Insert(Key("I'm Key"), []byte("I'm Value"))
	//insert another key
	tree.Insert(Key("I'm Key2"), []byte("I'm Value2"))

	//vv("as string tree = \n%v", tree.String())

	// search first
	value, _, found := tree.FindExact(Key("I'm Key"))
	assert.True(t, found)
	//vv("before serz, first search value = '%#v'", value)
	assert.Equal(t, []byte("I'm Value"), value)

	// save
	path := "out.saver_test"
	fd, err := os.Create(path)
	panicOn(err)
	saver, err := tree.NewTreeSaver(fd)
	panicOn(err)
	err = saver.Save()
	panicOn(err)
	fd.Close()
	if want, got := 2, saver.NumLeafWrit(); want != got {
		t.Fatalf("bad num leaf writ: want %v, got %v", want, got)
	}

	// load
	fd2, err := os.Open(path)
	panicOn(err)
	defer fd2.Close()

	reader := bufio.NewReader(fd2)
	loader, err := NewTreeLoader(reader)
	panicOn(err)
	tree2, err := loader.Load()
	panicOn(err)

	if tree2 == nil {
		panic("Why is tree2 nil?")
	}
	// search it
	value, _, found = tree2.FindExact(Key("I'm Key"))
	assert.True(t, found)
	//vv("value = '%#v'", value)
	assert.Equal(t, []byte("I'm Value"), value)

	// search it
	value, _, found = tree2.FindExact(Key("I'm Key2"))
	assert.True(t, found)
	assert.Equal(t, []byte("I'm Value2"), value)
}

func TestDepthString_MediumDeepTree(t *testing.T) {
	tree := NewArtTree()
	paths := loadTestFile("assets/medium.txt")
	for k := range paths {
		tree.Insert(paths[k], paths[k])
	}

	//vv("as string tree = \n%v", tree.String())

}

func testSaveTree(path string, tree *Tree) {
	fd, err := os.Create(path)
	panicOn(err)
	defer fd.Close()
	saver, err := tree.NewTreeSaver(fd)
	panicOn(err)
	err = saver.Save()
	panicOn(err)
}

func testLoadTree(path string) (tree *Tree, err error) {
	fd2, err := os.Open(path)
	panicOn(err)
	defer fd2.Close()
	reader := bufio.NewReader(fd2)
	loader, err := NewTreeLoader(reader)
	panicOn(err)
	return loader.Load()
}

func TestTree_Saver_LinuxPaths(t *testing.T) {
	tree := NewArtTree()
	t0 := time.Now()
	paths := loadTestFile("assets/linux.txt")
	e0 := time.Since(t0)
	t1 := time.Now()
	for k := range paths {
		tree.Insert(paths[k], paths[k])
		//tree.Insert(paths[k], nil)
	}
	e1 := time.Since(t1)

	t2 := time.Now()
	path := "out.tree_linux_leaves"
	testSaveTree(path, tree)
	e2 := time.Since(t2)

	t3 := time.Now()
	tree2, err := testLoadTree(path)
	panicOn(err)
	e3 := time.Since(t3)
	_ = tree2

	// verify the read from disk
	t4 := time.Now()
	for k := range paths {
		key := paths[k]
		v, _, found := tree.FindExact(key)
		if !found {
			panic(fmt.Sprintf("missing key '%v'", string(key)))
		}
		if v == nil {
			panic(fmt.Sprintf("no value back from disk for key '%v'", string(key)))
		}
		if v != nil {
			gotVal := v
			if !bytes.Equal(gotVal, key) {
				panic(fmt.Sprintf("value from disk wrong; want '%v'; got '%v'",
					string(key), string(gotVal)))
			}
		}
	}
	e4 := time.Since(t4)

	fmt.Printf("time to load %v paths from disk: %v\n", len(paths), e0)
	fmt.Printf("time to insert paths into tree: %v\n", e1)
	fmt.Printf("time to save tree to disk: %v\n", e2)
	fmt.Printf("time to read disk tree into memory: %v\n", e3)
	fmt.Printf("time to search tree to verify all paths present: %v\n", e4)
}

// just a map[string][]byte for comparisoin.
func TestBasicMap_LinuxPaths(t *testing.T) {

	//time to load 93790 paths from disk: 27.915326ms
	//time to insert paths into map: 29.895015ms

	m := make(map[string][]byte)
	t0 := time.Now()
	paths := loadTestFile("assets/linux.txt")
	e0 := time.Since(t0)
	t1 := time.Now()
	for k := range paths {
		//tree.Insert(paths[k], paths[k])
		m[string(paths[k])] = paths[k]
	}
	e1 := time.Since(t1)

	fmt.Printf("time to load %v paths from disk: %v\n", len(paths), e0)
	fmt.Printf("time to insert paths into map: %v\n", e1)
}

const (
	maxPrefixLen int = 10
)

func loadTestFile3(path string) []*KVI {
	file, err := os.Open(path)
	if err != nil {
		panic("Couldn't open " + path)
	}
	defer file.Close()

	var words []*KVI
	reader := bufio.NewReader(file)
	for i := 0; ; i++ {
		if line, err := reader.ReadBytes(byte('\n')); err != nil {
			break
		} else {
			if len(line) > 0 {
				words = append(words, &KVI{Key: line[:len(line)-1], Vidx: i})
			}
		}
	}
	return words
}

type KVI struct {
	Key  []byte
	Vidx int
}
