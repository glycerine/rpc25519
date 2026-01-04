package tube

import (
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
