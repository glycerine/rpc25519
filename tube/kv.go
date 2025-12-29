package tube

import (
	"github.com/glycerine/rpc25519/tube/art"
)

//go:generate greenpack

type ArtTable struct {
	Tree *art.Tree `zid:"0"`
}

func newArtTable() *ArtTable {
	return &ArtTable{
		Tree: art.NewArtTree(),
	}
}

func (s *ArtTable) clone() (r *ArtTable) {
	r = &ArtTable{
		Tree: art.NewArtTree(),
	}
	iter := s.Tree.Iter(nil, nil)
	for iter.Next() {
		k := iter.Key()
		v := iter.Value()
		var vtyp string
		lf := iter.Leaf()
		if lf != nil {
			vtyp = lf.Vtype
		}
		r.Tree.Insert(k, append([]byte{}, v...), vtyp)
	}
	return
}

type KVStore struct {
	m map[Key]*ArtTable

	Keys []Key       `zid:"0"`
	Vals []*ArtTable `zid:"1"`
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
