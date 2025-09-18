package art

import (
	"bytes"
	"fmt"
	"io"
	"sync"
	//"time"

	"github.com/glycerine/greenpack/msgp"
	"github.com/klauspost/compress/s2"
)

// gain from compression: 13MB -> 1.6MB for the linux paths (8x).
const globalFullSkipS2 = false

var ArtTreeSaverMagic string = "art.ArtTreeSaverMagic:"

type TreeSaver struct {
	w           *msgp.Writer
	ws2         *s2.Writer
	mut         sync.Mutex
	numLeafWrit int
	tree        *Tree

	skipS2 bool // compression or not
}

func (tree *Tree) PostLoadHook() {
	//vv("Tree.PostLoadHook() called")

	//vv("PostLoadHook sees %v order Leafz", len(tree.Leafz))
	for _, lf := range tree.Leafz {
		if lf == nil {
			panic("why nil leaf?")
		}
		tree.InsertLeaf(lf)
	}
	tree.Leafz = nil // allow GC of deletions
}

func (tree *Tree) PreSaveHook() {
	//vv("Tree.PreSaveHook() called \n%v", stack())

	if int64(len(tree.Leafz)) != tree.size {
		tree.Leafz = make([]*Leaf, tree.size)
	}
	//vv("tree Pre-save hook is recording "+
	//	"the order of %v leaves", tree.size)
	var i int64
	it := tree.Iter(nil, nil)
	for it.Next() {
		//vv("at i = %v, writing it.Leaf() to Leafz[i]: '%v'", i, it.Leaf())
		tree.Leafz[i] = it.Leaf()
		i++
	}
	if i != tree.size {
		panic("why not whole set of leaf?")
	}
}

func (tree *Tree) NewTreeSaver(w io.Writer) (*TreeSaver, error) {
	r := &TreeSaver{
		tree:   tree,
		skipS2: globalFullSkipS2,
	}

	// write magic uncompressed.
	_, err := w.Write([]byte(ArtTreeSaverMagic))
	if err != nil {
		return nil, fmt.Errorf("error writing ArtTreeSaverMagic: '%v'", err)
	}
	if r.skipS2 {
		r.w = msgp.NewWriter(w)
	} else {
		r.ws2 = s2.NewWriter(w)
		r.w = msgp.NewWriter(r.ws2)
	}

	return r, nil
}

func (s *TreeSaver) NumLeafWrit() (r int) {
	s.mut.Lock()
	r = s.numLeafWrit
	s.mut.Unlock()
	return
}

func (s *TreeSaver) Save() error {
	s.mut.Lock()
	defer s.mut.Unlock()

	// not ByteSlice framed. okay.
	err := s.tree.EncodeMsg(s.w)
	if err != nil {
		return fmt.Errorf("error on EncodeMsg "+
			"of tree: '%v'", err)
	}
	s.numLeafWrit = int(s.tree.size)

	err = s.w.Flush()
	var err2 error
	if !s.skipS2 {
		err2 = s.ws2.Close() // does Flush first.
	}
	if err != nil {
		return err
	}
	return err2
}

func (s *TreeSaver) Flush() (err error) {
	s.mut.Lock()
	defer s.mut.Unlock()
	err = s.w.Flush()
	var err2 error
	if !s.skipS2 {
		err2 = s.ws2.Flush()
	}
	if err != nil {
		return err
	}
	return err2
}

type TreeLoader struct {
	r    *msgp.Reader
	rs2  *s2.Reader
	tree *Tree

	skipS2 bool
}

func NewTreeLoader(r io.Reader) (*TreeLoader, error) {
	s := &TreeLoader{
		tree:   NewArtTree(),
		skipS2: globalFullSkipS2,
	}

	// read magic uncompressed.
	buf := make([]byte, len(ArtTreeSaverMagic))
	_, err := io.ReadFull(r, buf)
	if err != nil {
		return nil, fmt.Errorf("error reading ArtTreeSaverMagic: '%v'", err)
	}
	if !bytes.Equal(buf, []byte(ArtTreeSaverMagic)) {
		return nil, fmt.Errorf("error did not find '%v' at start", ArtTreeSaverMagic)
	}

	if s.skipS2 {
		s.r = msgp.NewReader(r)
	} else {
		s.rs2 = s2.NewReader(r)
		s.r = msgp.NewReader(s.rs2)
	}
	return s, nil
}

func (s *TreeLoader) Load() (tree *Tree, err error) {

	tree = s.tree

	err = tree.DecodeMsg(s.r)
	if err != nil {
		return nil, fmt.Errorf("error on "+
			"tree DecodeMsg: '%v'", err)
	}

	return tree, nil
}
