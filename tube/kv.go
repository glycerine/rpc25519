package tube

import (
	"fmt"
	"time"

	"github.com/glycerine/rpc25519/tube/art"
)

//go:generate greenpack

type ArtTable struct {
	Tree *art.Tree `zid:"0"`
}

func newArtTable() *ArtTable {
	r := &ArtTable{
		Tree: art.NewArtTree(),
	}
	r.Tree.SkipLocking = true
	return r
}

func (s *ArtTable) clone() (r *ArtTable) {
	r = &ArtTable{
		Tree: art.NewArtTree(),
	}
	r.Tree.SkipLocking = true
	iter := s.Tree.Iter(nil, nil)
	for iter.Next() {
		lf := iter.Leaf()
		r.Tree.InsertLeaf(lf.Clone())
	}
	return
}

type KVStore struct {
	m map[Key]*ArtTable

	Keys []Key       `zid:"0"`
	Vals []*ArtTable `zid:"1"`
}

func (s *KVStore) String() (r string) {
	if s == nil {
		return "<nil>"
	}
	for tableName, tab := range s.m {
		iter := tab.Tree.Iter(nil, nil)
		for iter.Next() {
			lf := iter.Leaf()
			r += fmt.Sprintf("table:'%v' key:'%v' value:'%v' vtype:'%v'\n", tableName, string(lf.Key), string(lf.Value), lf.Vtype)
		}
	}
	return
}

// Merge merges r into s, overwriting any
// conflicting prior keys in s with r's values,
// which are cloned from r before being saved into s.
// If you wish to give priority to s instead,
// call r.Merge(s) instead of s.Merge(r).
func (s *KVStore) Merge(r *KVStore) {
	if s == nil || r == nil {
		return
	}
	for rTableName, rTable := range r.m {
		sPrior, ok := s.m[rTableName]
		if !ok {
			// just add
			s.m[rTableName] = rTable.clone()
			continue
		}
		// need to merge rTable into sPrior from s.
		iter := rTable.Tree.Iter(nil, nil)
		for iter.Next() {
			lf := iter.Leaf()
			sPrior.Tree.InsertLeaf(lf.Clone())
		}
	}
}

func (s *KVStore) PreSaveHook() {
	//vv("KVStore.PreSaveHook")
	s.Keys = s.Keys[:0]
	s.Vals = s.Vals[:0]
	for k, v := range s.m {
		s.Keys = append(s.Keys, k)
		s.Vals = append(s.Vals, v)
	}
}

func (s *KVStore) PostLoadHook() {
	//vv("KVStore.PostLoadHook")
	if s.m == nil {
		s.m = make(map[Key]*ArtTable)
	}
	for i, k := range s.Keys {
		s.m[k] = s.Vals[i]
	}
	s.Keys = nil
	s.Vals = nil
}

func newKVStore() (r *KVStore) {
	r = &KVStore{
		m: make(map[Key]*ArtTable),
	}
	return
}

func (s *KVStore) clone() (r *KVStore) {
	r = &KVStore{
		m: make(map[Key]*ArtTable),
	}
	for k, v := range s.m {
		r.m[k] = v.clone()
	}
	return
}

func (state *RaftState) DumpStdoutAnnotatePath(path string) {
	if state == nil {
		fmt.Printf("\n(none) empty RaftState from path '%v'.\n", path)
	} else {
		fmt.Printf("\nRaftState from path '%v':\n%v\n", path, state.String())
		if state.KVstore != nil {
			fmt.Printf("KVstore: (len %v)\n", state.KVstore.Len())
			for table, tab := range state.KVstore.All() {
				fmt.Printf("    table '%v' (len %v):\n", table, tab.Len())
				var extra string
				for key, leaf := range tab.All() {
					if leaf.Leasor == "" {
						extra = ""
					} else {
						extra = fmt.Sprintf("[LeaseEpoch: %v, Leasor:'%v'; until '%v' (in %v; t0: '%v')] WriteRaftLogIndex:%v", leaf.LeaseEpoch, leaf.Leasor, nice(leaf.LeaseUntilTm), leaf.LeaseUntilTm.Sub(time.Now()), nice(leaf.LeaseEpochT0), leaf.WriteRaftLogIndex)
					}
					fmt.Printf("       key: '%v' (version %v): %v%v\n", key, leaf.Version, extra, StringFromVtype(leaf.Value, leaf.Vtype))
				}
			}
		} else {
			fmt.Printf("(nil KVstore)\n")
		}
	}
}
