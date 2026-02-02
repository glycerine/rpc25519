package art

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"time"
)

//go:generate greenpack

var _ = sync.RWMutex{}

const (
	XTypBytes int = 0
)

// used by tests; kind of a default value type.
type ByteSliceValue []byte

type TestBytes struct {
	Slc []byte `zid:"0"`
}

// ByteSlice is an alias for []byte. It
// can be ignored in the uart Unserserialized
// ART project, as it is only used for
// serialization purposes elsewhere.
//
// ByteSlice is a simple wrapper header on all msgpack
// messages; has the length and the bytes.
// Allows us length delimited messages;
// with length knowledge up front.
type ByteSlice []byte

// Key is the []byte which the tree
// sorts in lexicographic (shortlex) order,
// which means that shorter keys sort
// before longer keys with the same prefix.
//
// Key is an arbitrary string of bytes, and
// in particular can contain the 0 byte
// anywhere in the slice.
type Key []byte

// Leaf holds a Key and a Value together,
// and is stored in the Tree.
//
// Users must take care not to modify the
// Key on any leaf still in the tree (for
// example, the leaf returned from a Find() call),
// since it is used internally in the sorted order
// that the Tree maintains. The leaf
// must "own" its Key bytes, and the
// the user must copy them if they
// want to make changes.
//
// The leaf returned from Remove can
// be modified in any way desired, as it is no
// longer in the tree.
//
// In contrast, users should feel free to update
// the leaf.Value on any leaf. This can be
// much more efficient than doing
// an insert to update a Key's value
// if the leaf is already in hand.
type Leaf struct {
	// keybyte holds parent's name for us;
	// it is a kind of "back-pointer" value
	// that is primary useful for debug logging
	// and diagnostics.
	// keybyte will be somewhere in our Key,
	// but we don't know where because it
	// depends on the other keys the parent
	// has/path compression.
	keybyte byte

	Key   Key    `zid:"0"`
	Value []byte `zid:"1"`

	// version for CAS on version support
	Version int64 `zid:"2"`

	// optional type/description of the type of Value.
	Vtype string `zid:"3"`

	// optional metadata
	Leasor            string    `zid:"4"`
	LeaseUntilTm      time.Time `zid:"5"`
	WriteRaftLogIndex int64     `zid:"6"`
	LeaseEpoch        int64     `zid:"7"`

	// if lease goes stale then move this to the /dead table.
	AutoDelete   bool   `zid:"8"`
	LeasorPeerID string `zid:"9"`

	// when did this leasor first obtain the lease
	LeaseEpochT0 time.Time `zid:"10"`

	// how long to renew an expired lease.
	// i.e. if we renewed an expired lease, how long between possible
	// renewal and the actual renewal did it take? i.e. were our
	// members efficiently/effectively contending for czar?
	LeaseRenewalElap time.Duration `zid:"11"`
}

func (s *Leaf) Clone() (r *Leaf) {
	r = &Leaf{
		Key:               append([]byte{}, s.Key...),
		Value:             append([]byte{}, s.Value...),
		Version:           s.Version,
		Vtype:             s.Vtype,
		Leasor:            s.Leasor,
		LeaseUntilTm:      s.LeaseUntilTm,
		WriteRaftLogIndex: s.WriteRaftLogIndex,
		LeaseEpoch:        s.LeaseEpoch,
		AutoDelete:        s.AutoDelete,
		LeasorPeerID:      s.LeasorPeerID,
		LeaseEpochT0:      s.LeaseEpochT0,
		LeaseRenewalElap:  s.LeaseRenewalElap,
	}
	return
}

func (n *Leaf) depth() int {
	return len(n.Key)
}

func NewLeaf(key Key, v []byte, vtype string) *Leaf {
	return &Leaf{
		Key:   key,
		Value: v,
		Vtype: vtype,
	}
}

func (lf *Leaf) kind() kind {
	return _Leafy
}

func (lf *Leaf) insert(other *Leaf, depth int, selfb *bnode, tree *Tree, par *inner) (value *bnode, updated bool) {

	if lf == other {
		// due to restarts (now elided though),
		// we might be trying to put ourselves in
		// the tree twice.
		return selfb, false
	}

	if other.equal(lf.Key) {
		value = bnodeLeaf(other)
		updated = true
		// avoid forcing a full re-compute of pren.
		value.pren = selfb.pren
		return
	}

	longestPrefix := comparePrefix(lf.Key, other.Key, depth)
	//vv("longestPrefix = %v; lf.Key='%v', other.key='%v', depth=%v", longestPrefix, string(lf.Key), string(other.Key), depth)
	n4 := &node4{}
	nn := &inner{
		Node: n4,

		// keep commented out path stuff for debugging!
		//path: append([]byte{}, lf.Key[:depth+longestPrefix]...),
		SubN: 2,
	}
	//vv("assigned path '%v' to %p", string(nn.path), nn)
	if longestPrefix > 0 {
		nn.compressed = append([]byte{}, lf.Key[depth:depth+longestPrefix]...)
	}
	//vv("leaf insert: lef nn.PrefixLen = %v (longestPrefix)", nn.PrefixLen)

	child0key := lf.Key.At(depth + longestPrefix)
	child1key := other.Key.At(depth + longestPrefix)

	//vv("child0key = 0x%x; lf.Key = '%v' (len %v); depth=%v; longestPrefix=%v; depth+longestPrefix=%v", child0key, string(lf.Key), len(lf.Key), depth, longestPrefix, depth+longestPrefix)

	nn.Node.addChild(child0key, bnodeLeaf(lf))
	nn.Node.addChild(child1key, bnodeLeaf(other))

	selfb.isLeaf = false
	selfb.inner = nn
	return selfb, false
}

func (lf *Leaf) del(key Key, depth int, selfb *bnode, parentUpdate func(*bnode)) (deleted bool, deletedNode *bnode) {

	if !lf.equalUnlocked(key) {
		return false, nil
	}

	parentUpdate(nil)

	return true, selfb
}

func (lf *Leaf) get(key Key, i int, selfb *bnode) (value *bnode, found bool, dir direc, id int) {
	cmp := bytes.Compare(key, lf.Key)
	//pp("top of Leaf get, cmp = %v from lf.Key='%v'; key='%v'", cmp, string(lf.Key), string(key))
	//defer func() {
	//pp("Leaf '%v' returns found=%v, dir=%v", string(lf.Key), found, dir)
	//}()

	// return ourselves even if not exact match, to avoid
	// a second recursive descent on GTE, for example.
	return selfb, cmp == 0, direc(cmp), 0
}

func (lf *Leaf) addPrefixBefore(node *inner, key byte) {
	// Leaf does not store prefixes, only inner.
}

func (lf *Leaf) isLeaf() bool {
	return true
}

func (lf *Leaf) String() string {
	//return fmt.Sprintf("leaf[%q]", string(lf.Key))
	return lf.FlatString(0, 0)
}

// used by get
func (lf *Leaf) equal(other []byte) (equal bool) {
	return bytes.Compare(lf.Key, other) == 0
}

// use by del, already holding Lock
func (lf *Leaf) equalUnlocked(other []byte) (equal bool) {
	equal = bytes.Compare(lf.Key, other) == 0
	return
}

func (n *Leaf) FlatString(depth int, recurse int) (s string) {
	rep := strings.Repeat("    ", depth)
	return fmt.Sprintf(`%[1]v %p leaf: key '%v' (len %v)%v`,
		rep,
		n,
		viznlString(n.Key),
		len(n.Key),
		"\n",
	)
}

func (n *Leaf) stringNoKeys(depth int) (s string) {
	rep := strings.Repeat("    ", depth)
	return fmt.Sprintf(`%[1]v %p leaf:%v`,
		rep,
		n,
		"\n",
	)
}

func (n *Leaf) str() string {
	return string(n.Key)
}

// essential utility.
func panicOn(err error) {
	if err != nil {
		panic(err)
	}
}

//func (lf *Leaf) PreSaveHook() {
// 	if lf.Value == nil {
// 		return
// 	}
// 	switch x := lf.Value.(type) {
// 	case ByteSliceValue:
// 		lf.XTyp = XTypBytes
// 		lf.X = []byte(x)
// 	case []byte:
// 		lf.XTyp = XTypBytes
// 		lf.X = x
// 	default:
// 		panic(fmt.Sprintf("add a case here for your data type: %T", lf.Value))
// 	}
// }
//
// func (lf *Leaf) PostLoadHook() {
// 	switch lf.XTyp {
// 	case XTypBytes:
// 		lf.Value = ByteSliceValue(lf.X)
// 	default:
// 		panic("add a case here for your data type")
// 	}
// }
