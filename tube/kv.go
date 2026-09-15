package tube

import (
	"fmt"
	"iter"
	"sort"
	"strings"
	"time"

	"github.com/glycerine/rpc25519/tube/leased"
	"github.com/glycerine/yogadb"
)

//go:generate greenpack

const kvCompositeSep = "\x00"

// KVScan is the serializable result set returned by ShowKeys and range scans.
type KVScan struct {
	Leafz []*leased.Leaf `zid:"0"`
}

func newKVScan() *KVScan {
	return &KVScan{}
}

func (s *KVScan) InsertLeaf(lf *leased.Leaf) {
	if s == nil || lf == nil {
		return
	}
	s.Leafz = append(s.Leafz, lf.Clone())
}

func (s *KVScan) Size() int {
	if s == nil {
		return 0
	}
	return len(s.Leafz)
}

func (s *KVScan) sorted(desc bool) []*leased.Leaf {
	if s == nil || len(s.Leafz) == 0 {
		return nil
	}
	leafz := make([]*leased.Leaf, len(s.Leafz))
	copy(leafz, s.Leafz)
	sort.Slice(leafz, func(i, j int) bool {
		if desc {
			return leafz[i].Key > leafz[j].Key
		}
		return leafz[i].Key < leafz[j].Key
	})
	return leafz
}

func (s *KVScan) Ascend() iter.Seq2[Key, *leased.Leaf] {
	return func(yield func(Key, *leased.Leaf) bool) {
		for _, lf := range s.sorted(false) {
			if !yield(Key(lf.Key), lf) {
				return
			}
		}
	}
}

func (s *KVScan) Descend() iter.Seq2[Key, *leased.Leaf] {
	return func(yield func(Key, *leased.Leaf) bool) {
		for _, lf := range s.sorted(true) {
			if !yield(Key(lf.Key), lf) {
				return
			}
		}
	}
}

type KVTable struct {
	store *KVStore
	name  Key
}

func (s *KVTable) Len() int {
	if s == nil || s.store == nil {
		return 0
	}
	scan, err := s.store.scanTable(s.name, "", "", false, false, nil)
	if err != nil {
		return 0
	}
	return scan.Size()
}

func (s *KVTable) All() iter.Seq2[Key, *leased.Leaf] {
	return func(yield func(Key, *leased.Leaf) bool) {
		if s == nil || s.store == nil {
			return
		}
		scan, err := s.store.scanTable(s.name, "", "", false, false, nil)
		if err != nil {
			return
		}
		for key, leaf := range scan.Ascend() {
			if !yield(key, leaf) {
				return
			}
		}
	}
}

func (s *KVTable) String() (r string) {
	for k, lf := range s.All() {
		r += fmt.Sprintf("%v : %v\n", k, string(lf.Value))
	}
	return
}

type KVStore struct {
	db     *yogadb.FlexDB `msg:"-"`
	path   string         `msg:"-"`
	noDisk bool           `msg:"-"`

	// Leafz/Tables are populated only for portable state snapshots. The live
	// store is yogadb; ordinary persistor saves leave these empty.
	Leafz         []*leased.Leaf `zid:"0"`
	Tables        []Key          `zid:"1"`
	SnapshotValid bool           `zid:"2"`
	LeafTables    []Key          `zid:"3"`
}

func newKVStore() (r *KVStore) {
	r = &KVStore{}
	panicOn(r.Open("", true))
	return
}

func newKVStoreAt(path string, noDisk bool) (r *KVStore, err error) {
	r = &KVStore{}
	err = r.Open(path, noDisk)
	return
}

func newKVStoreMemoryPath() string {
	return "tube-kv-mem-" + cryRand15B()
}

func (s *KVStore) Open(path string, noDisk bool) error {
	if s == nil {
		return nil
	}
	loadSnapshot := s.SnapshotValid
	tables := cloneKeys(s.Tables)
	leafTables := cloneKeys(s.LeafTables)
	leafz := cloneLeaves(s.Leafz)

	if s.db != nil {
		s.db.Close()
		s.db = nil
	}
	if path == "" {
		path = newKVStoreMemoryPath()
	}
	cfg := &yogadb.Config{
		NoDisk:                 noDisk,
		DisableBackgroundFlush: true,
	}
	db, err := yogadb.OpenFlexDB(path, cfg)
	if err != nil {
		return err
	}
	db.AllowReads()
	s.db = db
	s.path = path
	s.noDisk = noDisk

	if loadSnapshot {
		if err := s.replaceWithSnapshot(tables, leafTables, leafz); err != nil {
			return err
		}
		s.Leafz = nil
		s.Tables = nil
		s.LeafTables = nil
		s.SnapshotValid = false
	}
	return nil
}

func (s *KVStore) Attach(path string, noDisk bool) error {
	tables, leafTables, leafz, err := s.snapshotData()
	if err != nil {
		return err
	}
	s.Tables = tables
	s.LeafTables = leafTables
	s.Leafz = leafz
	s.SnapshotValid = true
	return s.Open(path, noDisk)
}

func (s *KVStore) Close() {
	if s == nil || s.db == nil {
		return
	}
	s.db.Close()
	s.db = nil
}

func (s *KVStore) isPersistent() bool {
	return s != nil && s.db != nil && !s.noDisk && s.path != ""
}

func (s *KVStore) ensureDB() error {
	if s == nil {
		return fmt.Errorf("nil KVStore")
	}
	if s.db != nil {
		return nil
	}
	return s.Open("", true)
}

func (s *KVStore) PostLoadHook() {
	panicOn(s.Open("", true))
}

func cloneKeys(in []Key) []Key {
	if len(in) == 0 {
		return nil
	}
	out := make([]Key, len(in))
	copy(out, in)
	return out
}

func cloneLeaves(in []*leased.Leaf) []*leased.Leaf {
	if len(in) == 0 {
		return nil
	}
	out := make([]*leased.Leaf, 0, len(in))
	for _, lf := range in {
		if lf != nil {
			out = append(out, lf.Clone())
		}
	}
	return out
}

func compositeKey(table, key Key) string {
	return string(table) + kvCompositeSep + string(key)
}

func tablePrefix(table Key) string {
	return string(table) + kvCompositeSep
}

func splitCompositeKey(k string) (table, key Key, ok bool) {
	i := strings.IndexByte(k, 0)
	if i < 0 {
		return "", "", false
	}
	return Key(k[:i]), Key(k[i+1:]), true
}

func prefixEnd(prefix string) string {
	if prefix == "" {
		return ""
	}
	b := []byte(prefix)
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] != 0xff {
			b[i]++
			return string(b[:i+1])
		}
	}
	return ""
}

type yogaReader interface {
	Get(key string) ([]byte, bool, uint64, yogadb.HLC, error)
}

func decodeStoredLeaf(table, key Key, val []byte) (*leased.Leaf, error) {
	lf := &leased.Leaf{}
	left, err := lf.UnmarshalMsg(val)
	if err != nil {
		return nil, err
	}
	if len(left) != 0 {
		return nil, fmt.Errorf("leased leaf decode left %d extra bytes", len(left))
	}
	lf.Key = string(key)
	return lf, nil
}

func putLeafTx(tx *yogadb.WriteTx, table Key, lf *leased.Leaf) error {
	if lf == nil {
		return nil
	}
	lf2 := lf.Clone()
	b, err := lf2.MarshalMsg(nil)
	if err != nil {
		return err
	}
	if _, err := tx.Put(tablePrefix(table), []byte("table"), 0); err != nil {
		return err
	}
	_, err = tx.Put(compositeKey(table, Key(lf2.Key)), b, 0)
	return err
}

func getLeafTx(tx yogaReader, table, key Key) (*leased.Leaf, bool, error) {
	val, found, _, _, err := tx.Get(compositeKey(table, key))
	if err != nil || !found {
		return nil, false, err
	}
	lf, err := decodeStoredLeaf(table, key, val)
	if err != nil {
		return nil, false, err
	}
	return lf, true, nil
}

func tableExistsTx(tx yogaReader, table Key) (bool, error) {
	_, found, _, _, err := tx.Get(tablePrefix(table))
	if err != nil || found {
		return found, err
	}
	prefix := tablePrefix(table)
	end := prefixEnd(prefix)
	found = false
	if scanner, ok := tx.(interface {
		AscendRange(string, string, func(string, []byte, uint64, yogadb.HLC) bool)
	}); ok {
		scanner.AscendRange(prefix, end, func(k string, _ []byte, _ uint64, _ yogadb.HLC) bool {
			if k != prefix {
				found = true
				return false
			}
			return true
		})
	}
	return found, nil
}

func (s *KVStore) MakeTable(table Key) error {
	if table == "" {
		return nil
	}
	if err := s.ensureDB(); err != nil {
		return err
	}
	return s.db.Update(func(tx *yogadb.WriteTx) error {
		_, err := tx.Put(tablePrefix(table), []byte("table"), 0)
		return err
	})
}

func (s *KVStore) DeleteTable(table Key) error {
	if err := s.ensureDB(); err != nil {
		return err
	}
	if table == "" {
		return nil
	}
	prefix := tablePrefix(table)
	end := prefixEnd(prefix)
	return s.db.Update(func(tx *yogadb.WriteTx) error {
		_, _, err := tx.DeleteRange(true, prefix, end, true, false)
		return err
	})
}

func (s *KVStore) RenameTable(oldName, newName Key) error {
	if err := s.ensureDB(); err != nil {
		return err
	}
	if oldName == "" {
		return fmt.Errorf("error in rename table: no existing table name supplied.")
	}
	if newName == "" {
		return fmt.Errorf("error in rename table: no new table name supplied.")
	}
	if oldName == newName {
		return nil
	}
	return s.db.Update(func(tx *yogadb.WriteTx) error {
		exists, err := tableExistsTx(tx, newName)
		if err != nil {
			return err
		}
		if exists {
			return fmt.Errorf("error in rename table: target new table '%v' already exists.", newName)
		}
		exists, err = tableExistsTx(tx, oldName)
		if err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("error in rename table: existing table '%v' not found.", oldName)
		}

		var leafz []*leased.Leaf
		var scanErr error
		prefix := tablePrefix(oldName)
		end := prefixEnd(prefix)
		tx.AscendRange(prefix, end, func(k string, val []byte, _ uint64, _ yogadb.HLC) bool {
			_, key, ok := splitCompositeKey(k)
			if !ok || key == "" {
				return true
			}
			lf, err := decodeStoredLeaf(oldName, key, val)
			if err != nil {
				scanErr = err
				return false
			}
			leafz = append(leafz, lf)
			return true
		})
		if scanErr != nil {
			return scanErr
		}
		_, _, err = tx.DeleteRange(true, prefix, end, true, false)
		if err != nil {
			return err
		}
		if _, err := tx.Put(tablePrefix(newName), []byte("table"), 0); err != nil {
			return err
		}
		for _, lf := range leafz {
			if err := putLeafTx(tx, newName, lf); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *KVStore) PutLeaf(table Key, lf *leased.Leaf) error {
	if err := s.ensureDB(); err != nil {
		return err
	}
	return s.db.Update(func(tx *yogadb.WriteTx) error {
		return putLeafTx(tx, table, lf)
	})
}

func (s *KVStore) DeleteLeaf(table, key Key) error {
	if err := s.ensureDB(); err != nil {
		return err
	}
	return s.db.Update(func(tx *yogadb.WriteTx) error {
		return tx.Delete(compositeKey(table, key))
	})
}

func (s *KVStore) GetLeaf(table, key Key, autoDelete bool) (*leased.Leaf, error) {
	if err := s.ensureDB(); err != nil {
		return nil, err
	}
	if autoDelete {
		var out *leased.Leaf
		err := s.db.Update(func(tx *yogadb.WriteTx) error {
			lf, found, err := getLeafTx(tx, table, key)
			if err != nil || !found {
				return err
			}
			if lf.AutoDelete && table != "dead" && lf.Leasor != "" && lf.LeaseUntilTm.Before(time.Now()) {
				if err := putLeafTx(tx, "dead", lf); err != nil {
					return err
				}
				if err := tx.Delete(compositeKey(table, key)); err != nil {
					return err
				}
				return nil
			}
			out = lf
			return nil
		})
		if err != nil {
			return nil, err
		}
		if out == nil {
			return nil, ErrKeyNotFound
		}
		return out, nil
	}
	var out *leased.Leaf
	err := s.db.View(func(tx *yogadb.ReadOnlyTx) error {
		lf, found, err := getLeafTx(tx, table, key)
		if err != nil || !found {
			return err
		}
		out = lf
		return nil
	})
	if err != nil {
		return nil, err
	}
	if out == nil {
		return nil, ErrKeyNotFound
	}
	return out, nil
}

func (s *KVStore) scanTable(table Key, start Key, endx Key, descend bool, prefixMode bool, now *time.Time) (*KVScan, error) {
	if err := s.ensureDB(); err != nil {
		return nil, err
	}
	results := newKVScan()
	err := s.db.Update(func(tx *yogadb.WriteTx) error {
		exists, err := tableExistsTx(tx, table)
		if err != nil {
			return err
		}
		if !exists {
			return ErrKeyNotFound
		}
		base := tablePrefix(table)
		var lower, upper string
		if prefixMode {
			lower = base + string(start)
			upper = prefixEnd(lower)
		} else {
			lower = base + string(start)
			if endx == "" {
				upper = prefixEnd(base)
			} else {
				upper = base + string(endx)
			}
		}
		if upper == "" {
			upper = prefixEnd(base)
		}
		var scanErr error
		if !descend {
			tx.AscendRange(lower, upper, func(k string, val []byte, _ uint64, _ yogadb.HLC) bool {
				_, key, ok := splitCompositeKey(k)
				if !ok || key == "" {
					return true
				}
				lf, err := decodeStoredLeaf(table, key, val)
				if err != nil {
					scanErr = err
					return false
				}
				if s.expireIfNeeded(tx, table, key, lf, now) {
					return true
				}
				results.InsertLeaf(lf)
				return true
			})
			return scanErr
		}
		tx.Descend(upper, func(k string, val []byte, _ uint64, _ yogadb.HLC) bool {
			if k >= upper {
				return true
			}
			if k < lower {
				return false
			}
			_, key, ok := splitCompositeKey(k)
			if !ok || key == "" {
				return true
			}
			lf, err := decodeStoredLeaf(table, key, val)
			if err != nil {
				scanErr = err
				return false
			}
			if s.expireIfNeeded(tx, table, key, lf, now) {
				return true
			}
			results.InsertLeaf(lf)
			return true
		})
		return scanErr
	})
	if err != nil {
		return nil, err
	}
	return results, nil
}

func (s *KVStore) expireIfNeeded(tx *yogadb.WriteTx, table, key Key, lf *leased.Leaf, now *time.Time) bool {
	if now == nil || !lf.AutoDelete || table == "dead" || lf.Leasor == "" || !lf.LeaseUntilTm.Before(*now) {
		return false
	}
	if err := putLeafTx(tx, "dead", lf); err != nil {
		panicOn(err)
	}
	panicOn(tx.Delete(compositeKey(table, key)))
	return true
}

func (s *KVStore) ShowKeys(table Key) (*KVScan, error) {
	if err := s.ensureDB(); err != nil {
		return nil, err
	}
	results := newKVScan()
	if table == "" {
		tables, _, _, err := s.snapshotData()
		if err != nil {
			return nil, err
		}
		if len(tables) == 0 {
			return nil, ErrKeyNotFound
		}
		for _, tableName := range tables {
			results.InsertLeaf(leased.NewLeaf(string(tableName), nil, ""))
		}
		return results, nil
	}
	now := time.Now()
	return s.scanTable(table, "", "", false, false, &now)
}

func (s *KVStore) Len() int {
	tables, _, _, err := s.snapshotData()
	if err != nil {
		return 0
	}
	return len(tables)
}

func (s *KVStore) All() iter.Seq2[Key, *KVTable] {
	return func(yield func(Key, *KVTable) bool) {
		tables, _, _, err := s.snapshotData()
		if err != nil {
			return
		}
		for _, tableName := range tables {
			if !yield(tableName, &KVTable{store: s, name: tableName}) {
				return
			}
		}
	}
}

func (s *KVStore) String() (r string) {
	if s == nil {
		return "<nil>"
	}
	for tableName, tab := range s.All() {
		for _, leaf := range tab.store.mustTableLeaves(tableName) {
			r += fmt.Sprintf("table:'%v' key:'%v' value:'%v' vtype:'%v'\n", tableName, leaf.Key, string(leaf.Value), leaf.Vtype)
		}
	}
	return
}

func (s *KVStore) mustTableLeaves(table Key) []*leased.Leaf {
	scan, err := s.scanTable(table, "", "", false, false, nil)
	if err != nil {
		return nil
	}
	return scan.sorted(false)
}

func (s *KVStore) Merge(r *KVStore) {
	if s == nil || r == nil {
		return
	}
	_, leafTables, leafz, err := r.snapshotData()
	panicOn(err)
	for i, lf := range leafz {
		if i >= len(leafTables) {
			break
		}
		panicOn(s.PutLeaf(leafTables[i], lf))
	}
}

func (s *KVStore) clone() (r *KVStore) {
	tables, leafTables, leafz, err := s.snapshotData()
	panicOn(err)
	r = &KVStore{
		Tables:        cloneKeys(tables),
		LeafTables:    cloneKeys(leafTables),
		Leafz:         cloneLeaves(leafz),
		SnapshotValid: true,
	}
	panicOn(r.Open("", true))
	r.Tables = cloneKeys(tables)
	r.LeafTables = cloneKeys(leafTables)
	r.Leafz = cloneLeaves(leafz)
	r.SnapshotValid = true
	return
}

func (s *KVStore) snapshotData() (tables []Key, leafTables []Key, leafz []*leased.Leaf, err error) {
	if s == nil {
		return nil, nil, nil, nil
	}
	if err = s.ensureDB(); err != nil {
		return nil, nil, nil, err
	}
	seen := make(map[Key]bool)
	var scanErr error
	err = s.db.View(func(tx *yogadb.ReadOnlyTx) error {
		tx.Ascend("", func(k string, val []byte, _ uint64, _ yogadb.HLC) bool {
			table, key, ok := splitCompositeKey(k)
			if !ok {
				return true
			}
			if !seen[table] {
				seen[table] = true
				tables = append(tables, table)
			}
			if key == "" {
				return true
			}
			lf, err := decodeStoredLeaf(table, key, val)
			if err != nil {
				scanErr = err
				return false
			}
			leafTables = append(leafTables, table)
			leafz = append(leafz, lf)
			return true
		})
		return scanErr
	})
	return
}

func (s *KVStore) replaceWithSnapshot(tables []Key, leafTables []Key, leafz []*leased.Leaf) error {
	if err := s.ensureDB(); err != nil {
		return err
	}
	return s.db.Update(func(tx *yogadb.WriteTx) error {
		if _, err := tx.Clear(true); err != nil {
			return err
		}
		for _, table := range tables {
			if _, err := tx.Put(tablePrefix(table), []byte("table"), 0); err != nil {
				return err
			}
		}
		for i, lf := range leafz {
			if lf == nil {
				continue
			}
			if i >= len(leafTables) {
				return fmt.Errorf("kv snapshot leaf table count mismatch: %d tables for %d leaves", len(leafTables), len(leafz))
			}
			table := leafTables[i]
			if err := putLeafTx(tx, table, lf); err != nil {
				return err
			}
		}
		return nil
	})
}

func (state *RaftState) kvstoreRangeScan(tkt *Ticket, tktTable, tktKey, tktKeyEndx Key, descend bool) (results *KVScan, err error) {
	if state == nil || state.KVstore == nil {
		return nil, ErrKeyNotFound
	}
	now := time.Now()
	return state.KVstore.scanTable(tktTable, tktKey, tktKeyEndx, descend, false, &now)
}

func (state *RaftState) kvstorePrefixScan(tkt *Ticket, tktTable, tktPrefix Key, descend bool) (results *KVScan, err error) {
	if state == nil || state.KVstore == nil {
		return nil, ErrKeyNotFound
	}
	now := time.Now()
	return state.KVstore.scanTable(tktTable, tktPrefix, "", descend, true, &now)
}

func (state *RaftState) KVStoreRead(tkt *Ticket, tktTable, tktKey Key) ([]byte, string, error) {
	lf, err := state.KVStoreReadLeaf(tkt, tktTable, tktKey)
	if err != nil {
		return nil, "", err
	}
	return lf.Value, lf.Vtype, nil
}

func (state *RaftState) KVStoreReadLeaf(tkt *Ticket, tktTable, tktKey Key) (*leased.Leaf, error) {
	if state == nil || state.KVstore == nil {
		return nil, ErrKeyNotFound
	}
	return state.KVstore.GetLeaf(tktTable, tktKey, true)
}

func (state *RaftState) ensureDeadzone() error {
	if state == nil || state.KVstore == nil {
		return ErrKeyNotFound
	}
	return state.KVstore.MakeTable("dead")
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
