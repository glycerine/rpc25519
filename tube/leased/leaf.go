package leased

import (
	"fmt"
	"strings"
	"time"
)

//go:generate greenpack

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
	Key   string `zid:"0"`
	Value []byte `zid:"1"`

	// version for CAS on version support
	Version int64 `zid:"2"`

	// optional type/description of the type of Value.
	Vtype uint64 `zid:"3"`

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
		Key:               s.Key,
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

func (z *Leaf) MetaString() (r string) {
	r = "&Leaf{\n"
	r += fmt.Sprintf("              Key: %v\n", string(z.Key))
	r += fmt.Sprintf("            Value: (omit)\n")
	r += fmt.Sprintf("          Version: %v\n", z.Version)
	r += fmt.Sprintf("            Vtype: \"%v\"\n", z.Vtype)
	r += fmt.Sprintf("           Leasor: \"%v\"\n", z.Leasor)
	r += fmt.Sprintf("     LeaseUntilTm: %v\n", nice(z.LeaseUntilTm))
	r += fmt.Sprintf("WriteRaftLogIndex: %v\n", z.WriteRaftLogIndex)
	r += fmt.Sprintf("       LeaseEpoch: %v\n", z.LeaseEpoch)
	r += fmt.Sprintf("       AutoDelete: %v\n", z.AutoDelete)
	r += fmt.Sprintf("     LeasorPeerID: \"%v\",\n", z.LeasorPeerID)
	r += fmt.Sprintf("     LeaseEpochT0: %v\n", nice(z.LeaseEpochT0))
	r += fmt.Sprintf(" LeaseRenewalElap: %v\n", z.LeaseRenewalElap)
	r += "}\n"
	return
}

func NewLeaf(key string, v []byte, vtype uint64) *Leaf {
	return &Leaf{
		Key:   key,
		Value: v,
		Vtype: vtype,
	}
}

func (lf *Leaf) String() string {
	//return fmt.Sprintf("leaf[%q]", string(lf.Key))
	return lf.FlatString(0, 0)
}

func (n *Leaf) FlatString(depth int, recurse int) (s string) {
	rep := strings.Repeat("    ", depth)
	return fmt.Sprintf(`%[1]v %p leaf: key '%v' (len %v)%v`,
		rep,
		n,
		string(n.Key),
		len(n.Key),
		"\n",
	)
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

const rfc3339MsecTz0 = "2006-01-02T15:04:05.000Z07:00"

func nice(tm time.Time) string {
	return tm.Format(rfc3339MsecTz0)
}
