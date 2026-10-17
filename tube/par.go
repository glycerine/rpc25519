package tube

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc64"
	"io"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/glycerine/blake3"
	"github.com/glycerine/greenpack/msgp"
	rpc "github.com/glycerine/rpc25519"
	"github.com/glycerine/yogadb"
)

var crc64table = crc64.MakeTable(crc64.ECMA)

// parUndo protects destructive WAL/application changes. Publishing this before-image
// directory precedes every destructive write; removing its name commits the new
// bundle. Recovery is idempotent and happens before storage is opened. A crash
// during recovery simply repeats it. Unpublished staging directories are inert.
// Members 0/1 are WAL/identifiers, optional 2 is this node's saved state, and
// optional 3 preserves an unreadable application store opaquely.
type parUndo struct {
	path string
	io   parUndoIO
}

// Per-operation hooks exercise publication failures without changing global
// filesystem behavior or racing another node's recovery. Zero values use disk.
type parUndoIO struct {
	renameFile func(string, string) error
	syncFile   func(*os.File) error
}

func (p parUndoIO) rename(from, to string) error {
	if p.renameFile != nil {
		return p.renameFile(from, to)
	}
	return os.Rename(from, to)
}

func (p parUndoIO) sync(fd *os.File) error {
	if p.syncFile != nil {
		return p.syncFile(fd)
	}
	return fd.Sync()
}

func (p parUndoIO) syncDir(path string) (err error) {
	fd, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, fd.Close()) }()
	return p.sync(fd)
}

type parUndoFile struct {
	Size    int64
	Hash    string
	Missing bool
}

func parSyncDir(path string) (err error) {
	return (parUndoIO{}).syncDir(path)
}

// parCopyChecked streams large logs rather than allocating their claimed size.
// When dst is empty it only checks the source.
func parCopyChecked(src, dst string) (record parUndoFile, err error) {
	return (parUndoIO{}).copyChecked(src, dst)
}

func (p parUndoIO) copyChecked(src, dst string) (record parUndoFile, err error) {
	in, err := os.Open(src)
	if err != nil {
		return record, err
	}
	defer func() { err = errors.Join(err, in.Close()) }()
	h := blake3.New(64, nil)
	var out *os.File
	w := io.Writer(h)
	if dst != "" {
		out, err = os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			return record, err
		}
		defer func() { err = errors.Join(err, out.Close()) }()
		w = io.MultiWriter(out, h)
	}
	record.Size, err = io.Copy(w, in)
	if err == nil && out != nil {
		err = p.sync(out)
	}
	record.Hash = blake3ToString33B(h)
	return
}

// An unreadable application cannot supply a portable row before-image. Preserve
// its files opaquely instead, including an absent path or a non-directory path.
// The digest covers relative names, file types, sizes, and checked file bytes.
func (p parUndoIO) copyApplication(src, dst string) (record parUndoFile, err error) {
	if _, err := os.Lstat(src); errors.Is(err, os.ErrNotExist) {
		return parUndoFile{Missing: true}, nil
	} else if err != nil {
		return record, err
	}
	h := blake3.New(64, nil)
	var directories []string
	err = filepath.WalkDir(src, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		to := ""
		if dst != "" {
			to = filepath.Join(dst, rel)
		}
		if entry.IsDir() {
			fmt.Fprintf(h, "%d:%s:directory;", len(rel), rel)
			if to != "" {
				if err := os.Mkdir(to, 0700); err != nil {
					return err
				}
				directories = append(directories, to)
			}
			return nil
		}
		if !entry.Type().IsRegular() {
			return ErrPAREvidence // Never follow a link out of the store/journal.
		}
		file, err := p.copyChecked(path, to)
		if err != nil {
			return err
		}
		if file.Size > math.MaxInt64-record.Size {
			return ErrPARBounds
		}
		record.Size += file.Size
		fmt.Fprintf(h, "%d:%s:file:%d:%s;", len(rel), rel, file.Size, file.Hash)
		return nil
	})
	if err != nil {
		return record, err
	}
	for i := len(directories) - 1; i >= 0; i-- {
		if err := p.syncDir(directories[i]); err != nil {
			return record, err
		}
	}
	record.Hash = blake3ToString33B(h)
	return record, nil
}

func (p parUndoIO) restoreApplication(src, dst string, want parUndoFile) error {
	if want.Missing {
		if err := os.RemoveAll(dst); err != nil {
			return err
		}
		return p.syncDir(filepath.Dir(dst))
	}
	stage, err := os.MkdirTemp(filepath.Dir(dst), ".par-app-restore-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(stage)
	tmp := filepath.Join(stage, "store")
	got, err := p.copyApplication(src, tmp)
	if err != nil {
		return err
	}
	if got != want {
		return ErrPARChecksum
	}
	if err := os.RemoveAll(dst); err != nil {
		return err
	}
	if err := p.rename(tmp, dst); err != nil {
		return err
	}
	return p.syncDir(filepath.Dir(dst))
}

func parBeginUndo(path string, state ...*RaftState) (undo *parUndo, err error) {
	return parBeginUndoWithIO(path, parUndoIO{}, state...)
}

func parBeginUndoWithIO(path string, disk parUndoIO, state ...*RaftState) (undo *parUndo, err error) {
	name := path + ".par-undo"
	if _, err = os.Stat(name); !errors.Is(err, os.ErrNotExist) {
		if err == nil {
			err = ErrPARRecovering
		}
		return nil, err
	}
	tmp, err := os.MkdirTemp(filepath.Dir(path), ".par-stage-")
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			os.RemoveAll(tmp)
		}
	}()
	records := make([]parUndoFile, 2)
	for i, suffix := range []string{"", ".parlog"} {
		records[i], err = disk.copyChecked(path+suffix, filepath.Join(tmp, fmt.Sprint(i)))
		if errors.Is(err, os.ErrNotExist) {
			records[i], err = parUndoFile{Missing: true}, nil
		}
		if err != nil {
			return nil, err
		}
	}
	if len(state) > 0 && state[0] != nil {
		// A portable before-image also repairs a partially replaced YogaDB.
		// Its metadata is this node's own history, never a donor's vote.
		rawApplication := state[0].KVstore != nil && state[0].KVstore.parFault != nil
		var old *RaftState
		if rawApplication {
			copy := *state[0]
			kv := state[0].KVstore
			copy.KVstore = &KVStore{Leafz: kv.Leafz, Tables: kv.Tables,
				LeafTables: kv.LeafTables, SnapshotValid: kv.SnapshotValid}
			old = &copy
		} else {
			old = state[0].clone()
		}
		if old.KVstore != nil {
			defer old.KVstore.Close()
		}
		data, encodeErr := parEncodeState(old)
		if encodeErr != nil {
			return nil, encodeErr
		}
		statePath := filepath.Join(tmp, "2")
		if err = os.WriteFile(statePath, data, 0600); err != nil {
			return nil, err
		}
		fd, openErr := os.OpenFile(statePath, os.O_RDWR, 0)
		if openErr != nil {
			return nil, openErr
		}
		if err = errors.Join(disk.sync(fd), fd.Close()); err != nil {
			return nil, err
		}
		record, readErr := parCopyChecked(statePath, "")
		if readErr != nil {
			return nil, readErr
		}
		records = append(records, record)
		if rawApplication {
			kvPath := filepath.Join(filepath.Dir(path), "tube.yogadb")
			if filepath.Clean(state[0].KVstore.parFaultPath) != kvPath {
				return nil, ErrPARIdentity
			}
			record, err := disk.copyApplication(kvPath, filepath.Join(tmp, "3"))
			if err != nil {
				return nil, err
			}
			records = append(records, record)
		}
	}
	body, err := json.Marshal(records)
	if err != nil {
		return nil, err
	}
	h := blake3.New(64, nil)
	h.Write(body)
	manifest, err := json.Marshal(struct{ Body, Hash string }{string(body), blake3ToString33B(h)})
	if err != nil {
		return nil, err
	}
	fd, err := os.OpenFile(filepath.Join(tmp, "manifest"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return nil, err
	}
	n, writeErr := fd.Write(manifest)
	if writeErr == nil && n != len(manifest) {
		writeErr = io.ErrShortWrite
	}
	err = errors.Join(writeErr, disk.sync(fd), fd.Close())
	if err != nil {
		return nil, err
	}
	if err = disk.syncDir(tmp); err != nil {
		return nil, err
	}
	if err = disk.rename(tmp, name); err != nil {
		return nil, err
	}
	if err = disk.syncDir(filepath.Dir(path)); err != nil {
		return nil, err
	}
	return &parUndo{path: path, io: disk}, nil
}

// finish only removes recovery evidence after the caller's replacement files
// and directory changes are durable. Failure leaves the operation unacknowledged.
func (u *parUndo) finish() error {
	if u == nil {
		return nil
	}
	for _, suffix := range []string{"", ".parlog"} {
		fd, err := os.OpenFile(u.path+suffix, os.O_RDWR, 0)
		if errors.Is(err, os.ErrNotExist) {
			continue // A rollback may deliberately restore an originally absent file.
		}
		if err != nil {
			return err
		}
		err = errors.Join(u.io.sync(fd), fd.Close())
		if err != nil {
			return err
		}
	}
	dir := filepath.Dir(u.path)
	if err := u.io.syncDir(dir); err != nil {
		return err
	}
	// Rename away the whole journal atomically; deleting its children first
	// would turn a crash during cleanup into an unreadable pending operation.
	done := u.path + ".par-done-" + cryRand15B()
	if err := u.io.rename(u.path+".par-undo", done); err != nil {
		return err
	}
	if err := u.io.syncDir(dir); err != nil {
		return err
	}
	return os.RemoveAll(done)
}

func parRecoverUndo(path string) error {
	return parRecoverUndoWithIO(path, parUndoIO{})
}

func parRecoverUndoWithIO(path string, disk parUndoIO) error {
	name := path + ".par-undo"
	fd, err := os.Open(filepath.Join(name, "manifest"))
	if errors.Is(err, os.ErrNotExist) {
		if _, statErr := os.Stat(name); errors.Is(statErr, os.ErrNotExist) {
			return nil
		}
		return ErrPAREvidence
	}
	if err != nil {
		return err
	}
	data, err := io.ReadAll(io.LimitReader(fd, 8193))
	err = errors.Join(err, fd.Close())
	if err != nil {
		return err
	}
	var envelope struct{ Body, Hash string }
	if len(data) > 8192 || json.Unmarshal(data, &envelope) != nil {
		return ErrPAREvidence
	}
	h := blake3.New(64, nil)
	h.Write([]byte(envelope.Body))
	if blake3ToString33B(h) != envelope.Hash {
		return ErrPARChecksum
	}
	var records []parUndoFile
	if json.Unmarshal([]byte(envelope.Body), &records) != nil || len(records) < 2 || len(records) > 4 {
		return ErrPAREvidence
	}
	// Check the entire before-image before touching either live file.
	for i, want := range records {
		if want.Missing {
			if want.Size != 0 || want.Hash != "" {
				return ErrPAREvidence
			}
			continue
		}
		var got parUndoFile
		var err error
		if i == 3 {
			got, err = disk.copyApplication(filepath.Join(name, "3"), "")
		} else {
			got, err = parCopyChecked(filepath.Join(name, fmt.Sprint(i)), "")
		}
		if err != nil {
			return err
		}
		if got != want {
			return ErrPARChecksum
		}
	}
	destinations := []string{path, path + ".parlog", filepath.Join(filepath.Dir(path), "persistor.raftstate.tube.msgp")}
	for i := range records {
		if i == 3 {
			if err := disk.restoreApplication(filepath.Join(name, "3"), filepath.Join(filepath.Dir(path), "tube.yogadb"), records[i]); err != nil {
				return err
			}
			continue
		}
		if records[i].Missing {
			if err := os.Remove(destinations[i]); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
			if err := disk.syncDir(filepath.Dir(path)); err != nil {
				return err
			}
			continue
		}
		tmp := path + ".par-restore-" + cryRand15B()
		got, err := disk.copyChecked(filepath.Join(name, fmt.Sprint(i)), tmp)
		if err == nil && got != records[i] {
			err = ErrPARChecksum
		}
		if err == nil {
			err = disk.rename(tmp, destinations[i])
		}
		if err != nil {
			os.Remove(tmp)
			return err
		}
		if err := disk.syncDir(filepath.Dir(path)); err != nil {
			return err
		}
	}
	return (&parUndo{path: path, io: disk}).finish()
}

// parEncodeState uses the existing persistor wire format. Snapshot rows are
// included only when the caller supplied a portable state clone.
func parEncodeState(state *RaftState) ([]byte, error) {
	body, err := state.MarshalMsg(nil)
	if err != nil {
		return nil, err
	}
	framed, err := ByteSlice(body).MarshalMsg(nil)
	if err != nil {
		return nil, err
	}
	h := blake3.New(64, nil)
	h.Write(body)
	check, err := (&Blake3sum{Sum33B: blake3ToString33B(h)}).MarshalMsg(nil)
	if err != nil {
		return nil, err
	}
	trailer, err := ByteSlice(check).MarshalMsg(nil)
	return append(framed, trailer...), err
}

func parDecodeState(data []byte) (*RaftState, error) {
	body, n, err := parFrame(data)
	if err != nil {
		return nil, err
	}
	check, m, err := parFrame(data[n:])
	if err != nil {
		return nil, err
	}
	if n+m != len(data) {
		return nil, ErrPARFrame
	}
	var sum Blake3sum
	if rest, err := sum.UnmarshalMsg(check); err != nil || len(rest) != 0 {
		return nil, ErrPARFrame
	}
	h := blake3.New(64, nil)
	h.Write(body)
	if sum.Sum33B != blake3ToString33B(h) {
		return nil, ErrPARChecksum
	}
	state := new(RaftState)
	if rest, err := state.UnmarshalMsg(body); err != nil || len(rest) != 0 {
		if state.KVstore != nil {
			state.KVstore.Close()
		}
		return nil, ErrPARFrame
	}
	return state, nil
}

// This raw YogaDB key has no composite-key separator, so it cannot collide
// with user tables/keys and is omitted from portable application row snapshots.
const parApplicationKey = "tube PAR application checkpoint"

// Only preflight validation that has made no writes may return a normal command
// error inside an enclosing batch. Other storage errors poison that transaction
// so a partially failed command cannot be committed with the batch's checkpoint.
type parValidationError struct{ err error }

func (e *parValidationError) Error() string { return e.err.Error() }
func (e *parValidationError) Unwrap() error { return e.err }

func (s *KVStore) parUpdate(fn func(*yogadb.WriteTx) error) error {
	if s.parFault != nil {
		return ErrPARRecovering
	}
	var err error
	if s.parTx != nil {
		err = fn(s.parTx)
	} else {
		err = s.db.Update(fn)
	}
	if validation, ok := err.(*parValidationError); ok {
		return validation.err
	}
	if s.parTx != nil && s.parTxFailure == nil && err != nil && err != ErrKeyNotFound {
		s.parTxFailure = err
	}
	return err
}

func (s *KVStore) parView(fn func(yogadb.ReadOnlyDB) error) error {
	if s.parTx != nil {
		err := fn(s.parTx)
		if s.parTxFailure == nil && err != nil && err != ErrKeyNotFound {
			s.parTxFailure = err
		}
		return err
	}
	return s.db.View(func(tx *yogadb.ReadOnlyTx) error { return fn(tx) })
}

func parApplicationBytes(state *RaftState) ([]byte, error) {
	copy := *state
	copy.KVstore = nil // Rows and this checkpoint commit in the same transaction.
	return parEncodeState(&copy)
}

func (s *raftStatePersistor) parSaveApplication(state *RaftState) error {
	kv := state.KVstore
	if kv == nil {
		return nil
	}
	if kv.parFault != nil {
		return ErrPARRecovering
	}
	if kv.parAppReady && kv.parAppIndex == state.LastApplied {
		return kv.db.Sync()
	}
	data, err := parApplicationBytes(state)
	if err != nil {
		return err
	}
	if err := kv.db.Update(func(tx *yogadb.WriteTx) error {
		_, err := tx.Put(parApplicationKey, data, 0)
		return err
	}); err != nil {
		return err
	}
	kv.parAppReady, kv.parAppIndex = true, state.LastApplied
	return nil
}

// Intermediate membership saves may publish new local metadata, but must keep
// application/session fields at the transaction's previous durable checkpoint.
func parPriorApplication(state *RaftState) (*RaftState, error) {
	data, found, _, _, err := state.KVstore.parTx.Get(parApplicationKey)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, ErrPAREvidence
	}
	prior, err := parDecodeState(data)
	if err != nil {
		return nil, err
	}
	if prior.KVstore != nil {
		prior.KVstore.Close()
		return nil, ErrPAREvidence
	}
	copy := *state
	copy.LastApplied, copy.LastAppliedTerm, copy.LastAppliedLogHLC = prior.LastApplied, prior.LastAppliedTerm, prior.LastAppliedLogHLC
	copy.SessTable = prior.SessTable
	if copy.CompactionDiscardedLast.Index > copy.LastApplied {
		return nil, ErrPAREvidence
	}
	return &copy, nil
}

type parApplication struct {
	kv *KVStore
	tx *yogadb.WriteTx
}

func (s *TubeNode) parBeginApplication() (*parApplication, error) {
	if s.par == nil || s.cfg.NoDisk || s.state.LastApplied >= s.state.CommitIndex {
		return nil, nil
	}
	if s.par.applying || s.saver == nil {
		return nil, ErrPARConflict
	}
	if s.state.KVstore == nil {
		s.state.KVstore = newKVStore()
	}
	kv := s.state.KVstore
	if kv.parFault != nil {
		return nil, ErrPARRecovering
	}
	if !kv.isPersistent() {
		if err := kv.Attach(s.saver.kvPath, false); err != nil {
			return nil, err
		}
	}
	if !kv.parAppReady || kv.parAppIndex != s.state.LastApplied {
		if err := s.saver.parSaveApplication(s.state); err != nil {
			return nil, err
		}
	}
	tx, err := kv.db.BeginUpdate()
	if err != nil {
		return nil, err
	}
	kv.parTx, kv.parTxFailure, s.par.applying = tx, nil, true
	return &parApplication{kv: kv, tx: tx}, nil
}

func (a *parApplication) abort() {
	if a == nil || a.tx == nil {
		return
	}
	err := a.tx.Rollback()
	a.kv.parTx, a.kv.parTxFailure, a.tx = nil, nil, nil
	panicOn(err)
}

func (a *parApplication) commit(state *RaftState) error {
	if state.KVstore != a.kv || a.kv.parTx != a.tx {
		return ErrPARConflict
	}
	if a.kv.parTxFailure != nil {
		return a.kv.parTxFailure
	}
	data, err := parApplicationBytes(state)
	if err != nil {
		return err
	}
	if _, err := a.tx.Put(parApplicationKey, data, 0); err != nil {
		return err
	}
	err = a.tx.Commit()
	a.kv.parTx, a.tx = nil, nil
	if err == nil {
		a.kv.parAppReady, a.kv.parAppIndex = true, state.LastApplied
	}
	return err
}

func (s *raftStatePersistor) parRestoreApplication(state *RaftState, portable bool) (*RaftState, error) {
	kv := state.KVstore
	if kv == nil {
		return state, nil
	}
	if portable {
		// Snapshot installation/undo replaced all rows from a checked portable
		// image. Its boundary seeds the checkpoint, rather than old DB metadata.
		if err := s.parSaveApplication(state); err != nil {
			return nil, err
		}
	}
	var data []byte
	var found bool
	err := kv.db.View(func(tx *yogadb.ReadOnlyTx) error {
		var err error
		data, found, _, _, err = tx.Get(parApplicationKey)
		return err
	})
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, ErrPAREvidence
	}
	checkpoint, err := parDecodeState(data)
	if err != nil {
		return nil, err
	}
	if checkpoint.KVstore != nil {
		checkpoint.KVstore.Close()
		return nil, ErrPAREvidence
	}
	if checkpoint.CurrentTerm > state.CurrentTerm {
		// A checked application image cannot establish or replace this node's
		// voting history. Do not hide a local metadata contradiction with a
		// peer application snapshot.
		return nil, ErrPARConflict
	}
	if checkpoint.ClusterID != state.ClusterID || checkpoint.PeerName != state.PeerName ||
		checkpoint.LastApplied < 0 || checkpoint.LastAppliedTerm < 0 || checkpoint.LastApplied < state.LastApplied || checkpoint.CommitIndex < checkpoint.LastApplied ||
		checkpoint.LastApplied < checkpoint.CompactionDiscardedLast.Index {
		return nil, ErrPAREvidence
	}
	kv.parAppReady, kv.parAppIndex = true, checkpoint.LastApplied
	if checkpoint.LastApplied == state.LastApplied {
		if checkpoint.LastAppliedTerm != state.LastAppliedTerm {
			return nil, ErrPARConflict
		}
		return state, nil
	}
	// The YogaDB transaction committed before the ordinary state file. Restore
	// its application/session result, retaining this node's newer local voting
	// history and any later logless configuration metadata.
	checkpoint.KVstore = kv
	checkpoint.CurrentTerm, checkpoint.VotedFor, checkpoint.VotedForName = state.CurrentTerm, state.VotedFor, state.VotedForName
	checkpoint.HaveVoted, checkpoint.HaveVotedTerm = state.HaveVoted, state.HaveVotedTerm
	checkpoint.PeerID = state.PeerID
	if state.CommitIndex > checkpoint.CommitIndex {
		checkpoint.CommitIndex, checkpoint.CommitIndexEntryTerm = state.CommitIndex, state.CommitIndexEntryTerm
	}
	if state.CompactionDiscardedLast.Index > checkpoint.CompactionDiscardedLast.Index {
		checkpoint.CompactionDiscardedLast = state.CompactionDiscardedLast
	}
	if state.MC != nil && (checkpoint.MC == nil || state.MC.VersionGT(checkpoint.MC)) {
		checkpoint.MC, checkpoint.ShadowReplicas, checkpoint.Observers = state.MC, state.ShadowReplicas, state.Observers
	}
	return checkpoint, nil
}

// The first state save has no prior image. Publish its checked portable staging
// file before either vote copy, and retire it after the normal state publication.
// Its presence identifies an unfinished first save. Voting metadata alone is
// insufficient to reconstruct missing state.
const parInitialStateSuffix = ".pre_rename.initial"

func (s *raftStatePersistor) parStageInitialState(state *RaftState) error {
	path := s.path + parInitialStateSuffix
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		if err == nil {
			return ErrPARRecovering // Reopen the interrupted save before retrying.
		}
		return err
	}
	image := state.clone()
	if image.KVstore != nil {
		defer image.KVstore.Close()
	}
	data, err := parEncodeState(image)
	if err != nil {
		return err
	}
	return s.parIO.writeAtomic(path, data)
}

// Called only while the selected state file is empty. The staged initial image
// carries both local metadata and portable rows, including cuts before the
// application store was attached. Keep the image until startup has installed
// those rows and published the ordinary state file, so recovery can itself crash.
func (s *raftStatePersistor) parReadInitialState() (*RaftState, error) {
	path := s.path + parInitialStateSuffix
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	state, err := parDecodeState(data)
	if err != nil {
		return nil, err
	}
	if state.KVstore != nil && !state.KVstore.SnapshotValid {
		state.KVstore.Close()
		return nil, ErrPAREvidence
	}
	if s.name != "" && state.PeerName != s.name {
		if state.KVstore != nil {
			state.KVstore.Close()
		}
		return nil, ErrPARIdentity
	}
	return state, nil
}

func (s *raftStatePersistor) parRetireInitialState() error {
	path := s.path + parInitialStateSuffix
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	// A retry may see the selected state rename before its directory sync.
	// Make that selection durable before removing the initial recovery image.
	if err := s.parIO.sync(s.parentDirFd); err != nil {
		return err
	}
	if err := os.Remove(path); err != nil {
		return err
	}
	return s.parIO.sync(s.parentDirFd)
}

// parLocalMeta is exclusively local voting history. Two independently checked
// copies are made durable before the ordinary state save can acknowledge it.
// Interrupted updates select the highest checked sequence; a peer is never a
// source for these records. Existing saved state requires a valid local copy;
// only an empty state store may initialize a new pair.
type parLocalMeta struct {
	Sequence uint64
	Cluster  string
	Node     string
	Term     int64
	Vote     string
	VoteName string
	Voted    bool
	VoteTerm int64
}

func parWriteAtomic(path string, data []byte) (err error) {
	return (parUndoIO{}).writeAtomic(path, data)
}

func (disk parUndoIO) writeAtomic(path string, data []byte) (err error) {
	tmp := path + ".par-tmp-" + cryRand15B()
	defer os.Remove(tmp)
	fd, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	n, writeErr := fd.Write(data)
	if writeErr == nil && n != len(data) {
		writeErr = io.ErrShortWrite
	}
	err = errors.Join(writeErr, disk.sync(fd), fd.Close())
	if err != nil {
		return err
	}
	if err = disk.rename(tmp, path); err != nil {
		return err
	}
	return disk.syncDir(filepath.Dir(path))
}

func parLoadLocalMeta(path string) (*parLocalMeta, error) {
	m, _, err := parLoadLocalMetaDetail(path)
	return m, err
}

func parLoadLocalMetaDetail(path string) (*parLocalMeta, bool, error) {
	var best *parLocalMeta
	present := false
	valid := 0
	for _, suffix := range []string{".par-meta0", ".par-meta1"} {
		fd, err := os.Open(path + suffix)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, false, err
		}
		present = true
		data, err := io.ReadAll(io.LimitReader(fd, 8193))
		err = errors.Join(err, fd.Close())
		if err != nil {
			return nil, false, err
		}
		var envelope struct{ Body, Hash string }
		if len(data) > 8192 || json.Unmarshal(data, &envelope) != nil {
			continue
		}
		h := blake3.New(64, nil)
		h.Write([]byte(envelope.Body))
		if envelope.Hash != blake3ToString33B(h) {
			continue
		}
		var candidate parLocalMeta
		if json.Unmarshal([]byte(envelope.Body), &candidate) != nil || candidate.Sequence == 0 || candidate.Term < 0 ||
			candidate.VoteTerm < 0 || candidate.VoteTerm > candidate.Term || (candidate.Voted && candidate.VoteTerm != candidate.Term) {
			continue
		}
		if best != nil && candidate.Sequence == best.Sequence && candidate != *best {
			return nil, false, ErrPARConflict
		}
		if best != nil && candidate != *best {
			valid = 0 // Interrupted publication; bring both copies to the selected sequence.
		}
		valid++
		if best == nil || candidate.Sequence > best.Sequence {
			copy := candidate
			best = &copy
		}
	}
	if best == nil && present {
		return nil, false, ErrPAREvidence
	}
	return best, best != nil && valid < 2, nil
}

func (m *parLocalMeta) merge(state *RaftState) error {
	if m == nil {
		return nil
	}
	if state == nil {
		return ErrPAREvidence
	}
	if m.Cluster != state.ClusterID || (m.Node != "" && state.PeerName != "" && m.Node != state.PeerName) {
		return ErrPARIdentity
	}
	if m.Term < state.CurrentTerm {
		return nil
	}
	if m.Term == state.CurrentTerm && m.Vote != "" && state.VotedFor != "" && m.Vote != state.VotedFor {
		return ErrPARConflict
	}
	if m.Term > state.CurrentTerm || m.Voted || m.Vote != "" {
		state.CurrentTerm, state.VotedFor, state.VotedForName = m.Term, m.Vote, m.VoteName
		state.HaveVoted, state.HaveVotedTerm = m.Voted, m.VoteTerm
	}
	return nil
}

func (s *raftStatePersistor) parSaveLocalMeta(state *RaftState) error {
	if !s.parMetaEnabled {
		return nil
	}
	if err := s.parMeta.merge(state); err != nil {
		return err
	}
	next := parLocalMeta{Cluster: state.ClusterID, Node: state.PeerName, Term: state.CurrentTerm,
		Vote: state.VotedFor, VoteName: state.VotedForName, Voted: state.HaveVoted, VoteTerm: state.HaveVotedTerm}
	if s.parMeta != nil {
		next.Sequence = s.parMeta.Sequence
		if next == *s.parMeta && !s.parMetaDirty {
			return nil
		}
	}
	if next.Sequence == math.MaxUint64 {
		return ErrPARBounds
	}
	next.Sequence++
	body, err := json.Marshal(next)
	if err != nil {
		return err
	}
	h := blake3.New(64, nil)
	h.Write(body)
	data, err := json.Marshal(struct{ Body, Hash string }{string(body), blake3ToString33B(h)})
	if err != nil {
		return err
	}
	if len(data) > 8192 {
		return ErrPARBounds
	}
	for _, suffix := range []string{".par-meta0", ".par-meta1"} {
		if err := s.parIO.writeAtomic(s.path+suffix, data); err != nil {
			return err
		}
	}
	s.parMeta = &next
	s.parMetaDirty = false
	return nil
}

// protocol aware recovery implementation
// https://www.usenix.org/sites/default/files/conference/protected-files/fast18_slides_alagappan.pdf

//go:generate greenpack

// maximum size, including space for the 8 byte checksum.
const maxParRecord = 400

// ParRecord is the "persist record"
// in protocol aware recovery; the entry identifier.
// It is written to the separate .parlog sidecar.
// An 8 byte crc64.ECMA checksum follows
// the msgpack bytes on disk.
type ParRecord struct {
	Offset    int64  `zid:"0"`
	Len0      int64  `zid:"1"`
	Len2      int64  `zid:"2"`
	Index     int64  `zid:"3"`
	Term      int64  `zid:"4"`
	Epoch     int64  `zid:"5"`
	TicketID  string `zid:"6"` // 28 bytes
	ClusterID string `zid:"7"`
	RLEblake3 string `zid:"8"` // 56 bytes
}

type parLog struct {
	nodisk bool

	path        string
	fd          parFile
	parentDirFd *os.File

	n       int64 // logical records, including a staged batch
	durable int64 // records published only after the WAL sync

	e []*ParRecord
}

// PAR sidecar operations and recovery reads use this seam. Tests wrap real
// local files without replacing the rest of the storage stack.
type parFile interface {
	ReadAt([]byte, int64) (int, error)
	WriteAt([]byte, int64) (int, error)
	Stat() (os.FileInfo, error)
	Truncate(int64) error
	Sync() error
	Close() error
}

// Fixed error classes let recovery decisions avoid string matching and per-call
// error allocation. Location and identity are carried separately in parFault.
var (
	ErrPARBounds       = errors.New("PAR: invalid record boundary")
	ErrPARFrame        = errors.New("PAR: invalid frame")
	ErrPARChecksum     = errors.New("PAR: checksum mismatch")
	ErrPARIdentity     = errors.New("PAR: entry identity mismatch")
	ErrPAREvidence     = errors.New("PAR: insufficient trustworthy evidence")
	ErrPARStale        = errors.New("PAR: stale recovery round")
	ErrPARConflict     = errors.New("PAR: conflicting recovery evidence")
	ErrPARPrefixQuorum = errors.New("PAR: current voter quorum has not acknowledged the protected prefix")
)

type parFault struct {
	Path   string
	Slot   int64
	Offset int64
	Cause  error
}

func (e *parFault) Error() string {
	return fmt.Sprintf("%s: path=%q slot=%d offset=%d", e.Cause, e.Path, e.Slot, e.Offset)
}
func (e *parFault) Unwrap() error { return e.Cause }

// parFrame accepts only a complete binary frame within the supplied bounds.
// Unlike nextframe it never seeks, panics, or allocates from an unchecked length.
func parFrame(b []byte) (payload []byte, size int, err error) {
	if len(b) < 2 {
		return nil, 0, ErrPARFrame
	}
	var head int
	var n uint64
	switch b[0] {
	case bin8:
		head, n = 2, uint64(b[1])
	case bin16:
		if len(b) < 3 {
			return nil, 0, ErrPARFrame
		}
		head, n = 3, uint64(binary.BigEndian.Uint16(b[1:3]))
	case bin32:
		if len(b) < 5 {
			return nil, 0, ErrPARFrame
		}
		head, n = 5, uint64(binary.BigEndian.Uint32(b[1:5]))
	default:
		return nil, 0, ErrPARFrame
	}
	if n > uint64(len(b)-head) {
		return nil, 0, ErrPARFrame
	}
	size = head + int(n)
	return b[head:size], size, nil
}

func encodeParSlot(pr *ParRecord) ([]byte, error) {
	by, err := pr.MarshalMsg(nil)
	if err != nil {
		return nil, err
	}
	frame, err := ByteSlice(by).MarshalMsg(nil)
	if err != nil {
		return nil, err
	}
	if len(frame) > maxParRecord-8 {
		return nil, ErrPARBounds
	}
	b := make([]byte, maxParRecord)
	copy(b, frame)
	binary.BigEndian.PutUint64(b[len(frame):], crc64.Checksum(frame, crc64table))
	return b, nil
}

func decodeParSlot(b []byte) (*ParRecord, error) {
	if len(b) != maxParRecord {
		return nil, ErrPARBounds
	}
	payload, n, err := parFrame(b[:maxParRecord-8])
	if err != nil {
		return nil, err
	}
	if crc64.Checksum(b[:n], crc64table) != binary.BigEndian.Uint64(b[n:n+8]) {
		return nil, ErrPARChecksum
	}
	pr := new(ParRecord)
	left, err := pr.UnmarshalMsg(payload)
	if err != nil {
		return nil, err
	}
	if len(left) != 0 {
		return nil, ErrPARFrame
	}
	// Padding is deliberately not checksummed in the existing file format.
	return pr, nil
}

func newParLog(path string, nodisk bool) (*parLog, error) {
	s := &parLog{nodisk: nodisk, path: path}
	if nodisk {
		return s, nil
	}
	fd, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	ok := false
	defer func() {
		if !ok {
			fd.Close()
		}
	}()
	if err = fd.Sync(); err != nil {
		return nil, err
	}
	fi, err := fd.Stat()
	if err != nil {
		return nil, err
	}
	if fi.Size()%maxParRecord != 0 {
		return nil, ErrPARBounds
	}
	s.fd = fd
	for i := int64(0); i < fi.Size()/maxParRecord; i++ {
		pr, err := s.loadone(i)
		if err != nil {
			return nil, &parFault{path, i, i * maxParRecord, err}
		}
		s.e = append(s.e, pr)
		s.n++
	}
	s.durable = s.n
	ok = true
	return s, nil
}

// Len0 is the framed entry length; Len2 is the separate checksum-frame length.
// Their sum is the entry's entire WAL span.
func (s *parLog) stage1(e *RaftLogEntry, offset, nw0, nw2 int64, hash string) error {
	if e == nil || e.Ticket == nil {
		return ErrPARIdentity
	}
	pr := &ParRecord{Offset: offset, Len0: nw0, Len2: nw2, Index: e.Index,
		Term: e.Term, TicketID: e.Ticket.TicketID, ClusterID: e.Ticket.ClusterID, RLEblake3: hash}
	if !s.nodisk {
		if err := parRecordBounds(pr); err != nil {
			return err
		}
		if _, err := encodeParSlot(pr); err != nil {
			return err
		}
	}
	s.e = append(s.e, pr)
	s.n++
	return nil
}

func (s *parLog) append1(e *RaftLogEntry, offset, nw0, nw2 int64, hash string) error {
	if err := s.stage1(e, offset, nw0, nw2, hash); err != nil {
		return err
	}
	if err := s.sync(); err != nil {
		s.e = s.e[:s.n-1]
		s.n--
		return err
	}
	return nil
}

func (s *parLog) at(i int64) (*ParRecord, error) {
	if i < 0 {
		return nil, ErrPARBounds
	}
	if i >= int64(len(s.e)) {
		return nil, io.EOF
	}
	return s.e[i], nil
}

func (s *parLog) truncate(keep int64) error {
	if keep < 0 || keep > s.n {
		return ErrPARBounds
	}
	if keep == s.n {
		return nil
	}
	if !s.nodisk && keep < s.durable {
		if err := s.fd.Truncate(keep * maxParRecord); err != nil {
			return err
		}
		if err := s.fd.Sync(); err != nil {
			return err
		}
	}
	s.e, s.n = s.e[:keep], keep
	if s.durable > keep {
		s.durable = keep
	}
	return nil
}

// The WAL owner must sync the data file BEFORE calling sync on staged records.
// WriteAt avoids changing the appender's file position.
func (s *parLog) sync() error {
	if s.nodisk {
		s.durable = s.n
		return nil
	}
	if s.durable == s.n {
		return nil
	}
	for i := s.durable; i < s.n; i++ {
		if i > math.MaxInt64/maxParRecord {
			return ErrPARBounds
		}
		b, err := encodeParSlot(s.e[i])
		if err != nil {
			return err
		}
		n, err := s.fd.WriteAt(b, i*maxParRecord)
		if err != nil {
			return err
		}
		if n != len(b) {
			return io.ErrShortWrite
		}
	}
	if err := s.fd.Sync(); err != nil {
		return err
	}
	s.durable = s.n
	return nil
}

func (s *parLog) close() error {
	if s.nodisk || s.fd == nil {
		return nil
	}
	err := s.sync()
	closeErr := s.fd.Close()
	s.fd = nil
	return errors.Join(err, closeErr)
}

func (s *parLog) loadone(i int64) (*ParRecord, error) {
	if i < 0 || i > math.MaxInt64/maxParRecord {
		return nil, ErrPARBounds
	}
	if s.nodisk {
		return s.at(i)
	}
	var b [maxParRecord]byte
	if _, err := s.fd.ReadAt(b[:], i*maxParRecord); err != nil {
		return nil, err
	}
	return decodeParSlot(b[:])
}

// A sparse inventory preserves verified identities even when payload bytes are
// unreadable. It is separate from raftLog: a hole is never an executable command.
type parEntryStatus uint8

const (
	parIntact parEntryStatus = iota
	parDamaged
	parUnknown
	parInterrupted
)

type parEntry struct {
	Record *ParRecord
	Entry  *RaftLogEntry
	Bytes  []byte
	Status parEntryStatus
	Err    error
}

type parInventory struct {
	Entries []parEntry
	WALSize int64
	ParSize int64
	Err     error // missing/unidentified history, not an absence certificate
}

func parHash(b, prev []byte) (single, chain string) {
	h := blake3.New(64, nil)
	h.Write(b)
	single = blake3ToString33B(h)
	h.Write(prev)
	return single, blake3ToString33B(h)
}

func parRecordBounds(pr *ParRecord) error {
	if pr == nil || pr.Offset < 0 || pr.Index <= 0 || pr.Term < 0 ||
		pr.Len0 < 2 || pr.Len2 < 2 || pr.Len0 > math.MaxInt64-pr.Len2 ||
		pr.Offset > math.MaxInt64-pr.Len0-pr.Len2 || pr.TicketID == "" || pr.RLEblake3 == "" {
		return ErrPARBounds
	}
	return nil
}

func parCheckIdentity(pr *ParRecord, e *RaftLogEntry, hash string) error {
	if e == nil || e.Ticket == nil || e.Index != pr.Index || e.Term != pr.Term ||
		e.Ticket.TicketID != pr.TicketID || e.Ticket.ClusterID != pr.ClusterID || hash != pr.RLEblake3 {
		return ErrPARIdentity
	}
	return nil
}

// parReadEntry reads only the separately checked record's range. File size is
// checked before allocating, including on a missing/truncated complete suffix.
func parReadEntry(fd *os.File, pr *ParRecord, size int64, prev []byte, checkChain bool) parEntry {
	r := parEntry{Record: pr, Status: parDamaged}
	if r.Err = parRecordBounds(pr); r.Err != nil {
		r.Status = parUnknown
		return r
	}
	span := pr.Len0 + pr.Len2
	if pr.Offset > size || span > size-pr.Offset {
		r.Err = io.ErrUnexpectedEOF
		return r
	}
	// Avoid int truncation on 32-bit hosts.
	if uint64(span) > uint64(^uint(0)>>1) {
		r.Err = ErrPARBounds
		return r
	}
	b := make([]byte, int(span))
	if _, r.Err = fd.ReadAt(b, pr.Offset); r.Err != nil {
		return r
	}
	payload, n, err := parFrame(b[:pr.Len0])
	if err != nil || int64(n) != pr.Len0 {
		r.Err = ErrPARFrame
		return r
	}
	hash, chain := parHash(payload, prev)
	if hash != pr.RLEblake3 {
		r.Err = ErrPARChecksum
		return r
	}
	e := new(RaftLogEntry)
	left, err := e.UnmarshalMsg(payload)
	if err != nil {
		r.Err = err
		return r
	}
	if len(left) != 0 {
		r.Err = ErrPARFrame
		return r
	}
	if r.Err = parCheckIdentity(pr, e, hash); r.Err != nil {
		return r
	}
	r.Entry, r.Bytes = e, payload
	e.msgbytes = payload
	checksum, n, err := parFrame(b[pr.Len0:])
	if err != nil || int64(n) != pr.Len2 {
		r.Err = ErrPARFrame
		return r
	}
	sum := new(Blake3sum)
	left, err = sum.UnmarshalMsg(checksum)
	if err != nil {
		r.Err = err
		return r
	}
	if len(left) != 0 {
		r.Err = ErrPARFrame
		return r
	}
	if sum.Sum33B != hash || (checkChain && sum.Sum33Bchain != chain) {
		r.Err = ErrPARChecksum
		return r
	}
	r.Status = parIntact
	return r
}

// scanParWAL is read-only, including on missing files. It never creates/truncates
// evidence and continues after damaged slots without trusting their boundaries.
func scanParWAL(path, cluster string) (*parInventory, error) {
	inv := new(parInventory)
	wal, err := os.Open(path)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		// Identifiers may still identify every missing payload. Preserve them
		// instead of treating an absent WAL as a newly bootstrapped node.
		inv.Err = err
	} else {
		defer wal.Close()
		fi, err := wal.Stat()
		if err != nil {
			return nil, err
		}
		inv.WALSize = fi.Size()
	}
	par, err := os.Open(path + ".parlog")
	if err != nil {
		inv.Err = err
		return inv, nil
	}
	defer par.Close()
	fi, err := par.Stat()
	if err != nil {
		return nil, err
	}
	inv.ParSize = fi.Size()
	if fi.Size()%maxParRecord != 0 {
		inv.Err = ErrPAREvidence
	}
	var previous *ParRecord
	var prevBytes []byte
	var end int64
	for i := int64(0); i < fi.Size()/maxParRecord; i++ {
		var slot [maxParRecord]byte
		_, readErr := par.ReadAt(slot[:], i*maxParRecord)
		pr, decodeErr := decodeParSlot(slot[:])
		if readErr != nil || decodeErr != nil {
			cause := errors.Join(readErr, decodeErr)
			inv.Entries = append(inv.Entries, parEntry{Status: parUnknown, Err: &parFault{path + ".parlog", i, i * maxParRecord, cause}})
			inv.Err = ErrPAREvidence
			previous, prevBytes = nil, nil
			continue
		}
		if parRecordBounds(pr) != nil || (cluster != "" && pr.ClusterID != cluster) ||
			(previous != nil && (previous.Index == math.MaxInt64 || pr.Index != previous.Index+1 || pr.Term < previous.Term || pr.Offset != end)) ||
			(i == 0 && pr.Offset != 0) {
			inv.Entries = append(inv.Entries, parEntry{Record: pr, Status: parUnknown, Err: ErrPARIdentity})
			inv.Err = ErrPAREvidence
			previous, prevBytes = nil, nil
			continue
		}
		chainPrev := prevBytes
		if i == 1 {
			// The existing WAL deliberately uses this fixed seed for its
			// second record; preserve that format when auditing old files.
			chainPrev = yggdrasill
		}
		r := parReadEntry(wal, pr, inv.WALSize, chainPrev, i <= 1 || prevBytes != nil)
		if r.Entry != nil && (r.Entry.PrevIndex != pr.Index-1 || (previous != nil && r.Entry.PrevTerm != previous.Term)) {
			r.Status, r.Err, r.Entry, r.Bytes = parDamaged, ErrPARIdentity, nil, nil
		}
		inv.Entries = append(inv.Entries, r)
		previous, prevBytes = pr, r.Bytes
		end = pr.Offset + pr.Len0 + pr.Len2
	}
	if end != inv.WALSize {
		inv.Err = ErrPAREvidence
	}
	return inv, nil
}

// parFence is echoed by every recovery reply. A caller must supply its current
// fence when consuming a reply; term/configuration/generation changes invalidate
// the complete round rather than allowing certificates from different histories.
type parFence struct {
	Leader        string
	Term          int64
	ConfigTerm    int64
	ConfigVersion int64
	Generation    uint64
	Round         string
}

type parEvidence uint8

const (
	parUnavailable parEvidence = iota
	parValid
	parKnownDamaged
	parAbsent
	parCompacted
)

type parReply struct {
	Fence  parFence
	Member string
	PeerID string
	Kind   parEvidence
	Record ParRecord
	Bytes  []byte // original serialized entry, not a re-encoded command
}

type parDecision uint8

const (
	parWait parDecision = iota
	parRepair
	parDiscard
)

type parRound struct {
	fence     parFence
	target    ParRecord
	members   map[string]string // voting member name -> current incarnation
	donors    map[string]string // voters plus configured nonvoting replicas
	protected int64             // durable commit/applied/snapshot floor
	replies   map[string]parReply
	failed    error
}

func newParRound(fence parFence, target ParRecord, members map[string]string, protected int64, extraDonors ...map[string]string) (*parRound, error) {
	if fence.Leader == "" || fence.Round == "" || fence.Term <= 0 || len(members) == 0 || parRecordBounds(&target) != nil {
		return nil, ErrPAREvidence
	}
	m := make(map[string]string, len(members))
	ids := make(map[string]bool, len(members))
	for name, id := range members {
		if name == "" || id == "" || ids[id] {
			return nil, ErrPARIdentity
		}
		m[name], ids[id] = id, true
	}
	donors := make(map[string]string, len(m))
	for name, id := range m {
		donors[name] = id
	}
	for _, extra := range extraDonors {
		for name, id := range extra {
			if old, ok := donors[name]; ok && old == id {
				continue
			}
			if name == "" || id == "" || ids[id] || donors[name] != "" {
				return nil, ErrPARIdentity
			}
			donors[name], ids[id] = id, true
		}
	}
	return &parRound{fence: fence, target: target, members: m, donors: donors, protected: protected, replies: make(map[string]parReply)}, nil
}

func parSameIdentity(a, b *ParRecord) bool {
	return a.ClusterID == b.ClusterID && a.Index == b.Index && a.Term == b.Term &&
		a.TicketID == b.TicketID && a.RLEblake3 == b.RLEblake3
}

func parValidateBytes(pr *ParRecord, b []byte) (*RaftLogEntry, error) {
	hash, _ := parHash(b, nil)
	if hash != pr.RLEblake3 {
		return nil, ErrPARChecksum
	}
	e := new(RaftLogEntry)
	left, err := e.UnmarshalMsg(b)
	if err != nil {
		return nil, err
	}
	if len(left) != 0 {
		return nil, ErrPARFrame
	}
	if err := parCheckIdentity(pr, e, hash); err != nil {
		return nil, err
	}
	if e.PrevIndex != e.Index-1 {
		return nil, ErrPARIdentity
	}
	e.msgbytes = b
	return e, nil
}

func (r *parRound) add(current parFence, reply parReply) error {
	if current != r.fence || reply.Fence != r.fence {
		return ErrPARStale
	}
	eligible := r.members
	if reply.Kind != parAbsent {
		eligible = r.donors
	}
	id, ok := eligible[reply.Member]
	if !ok || id != reply.PeerID || reply.Member == r.fence.Leader {
		return ErrPARStale
	}
	if !parSameIdentity(&r.target, &reply.Record) {
		return ErrPARIdentity
	}
	if reply.Kind > parCompacted {
		return ErrPAREvidence
	}
	if reply.Kind == parValid {
		if _, err := parValidateBytes(&r.target, reply.Bytes); err != nil {
			return err
		}
	}
	if old, ok := r.replies[reply.Member]; ok {
		// Unknown/compacted/damaged evidence may advance to checked data or
		// a positive absence assertion after that node finishes recovery.
		if old.Kind == parAbsent && reply.Kind == parValid || old.Kind == parValid && reply.Kind == parAbsent {
			r.failed = ErrPARConflict
			return r.failed
		}
		if old.Kind == parValid || old.Kind == parAbsent {
			return nil
		}
	}
	// A reply's storage is owned by its decoder. Retain an independent copy.
	reply.Bytes = append([]byte(nil), reply.Bytes...)
	r.replies[reply.Member] = reply
	return nil
}

func (r *parRound) decide(current parFence) (parDecision, []byte, error) {
	if current != r.fence {
		return parWait, nil, ErrPARStale
	}
	if r.failed != nil {
		return parWait, nil, r.failed
	}
	negative := 0
	for _, reply := range r.replies {
		if reply.Kind == parValid {
			return parRepair, append([]byte(nil), reply.Bytes...), nil
		}
		if reply.Kind == parAbsent {
			negative++
		}
	}
	// Self has damaged data, not an absence vote. The threshold is calculated
	// from all configured voters, never from the number that happened to reply.
	if negative >= len(r.members)/2+1 {
		if r.target.Index <= r.protected {
			return parWait, nil, ErrPARConflict
		}
		return parDiscard, nil, nil
	}
	return parWait, nil, nil
}

// repairParEntry restores the original byte span without destroying identifier
// evidence. Interrupted repair leaves the same independently identified hole.
// The owner must fence concurrent WAL mutation before invoking this operation.
func repairParEntry(path string, slot int64, target ParRecord, data []byte) error {
	if slot < 0 || slot > math.MaxInt64/maxParRecord || parRecordBounds(&target) != nil {
		return ErrPARBounds
	}
	if _, err := parValidateBytes(&target, data); err != nil {
		return err
	}
	par, err := os.Open(path + ".parlog")
	if err != nil {
		return err
	}
	defer par.Close()
	var b [maxParRecord]byte
	if _, err = par.ReadAt(b[:], slot*maxParRecord); err != nil {
		return err
	}
	pr, err := decodeParSlot(b[:])
	if err != nil {
		return err
	}
	if *pr != target {
		return ErrPARStale
	}
	// Creating a missing WAL is authorized only after checking the existing
	// independent identifier above. It does not create new log history.
	_, statErr := os.Stat(path)
	if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
		return statErr
	}
	wal, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer wal.Close()
	fi, err := wal.Stat()
	if err != nil {
		return err
	}
	var prev []byte
	if slot == 1 {
		prev = yggdrasill
	}
	if slot > 1 {
		if _, err = par.ReadAt(b[:], (slot-1)*maxParRecord); err != nil {
			return err
		}
		before, err := decodeParSlot(b[:])
		if err != nil {
			return err
		}
		if parRecordBounds(before) != nil || before.Index != target.Index-1 ||
			before.Offset+before.Len0+before.Len2 != target.Offset {
			return ErrPARIdentity
		}
		entry := parReadEntry(wal, before, fi.Size(), nil, false)
		if entry.Entry == nil {
			return ErrPAREvidence
		}
		prev = entry.Bytes
	}
	entry, err := parValidateBytes(&target, data)
	if err != nil {
		return err
	}
	if slot > 0 {
		if _, err = par.ReadAt(b[:], (slot-1)*maxParRecord); err != nil {
			return err
		}
		before, err := decodeParSlot(b[:])
		if err != nil {
			return err
		}
		if before.Index != target.Index-1 || entry.PrevTerm != before.Term {
			return ErrPARIdentity
		}
	}
	frame, err := ByteSlice(data).MarshalMsg(nil)
	if err != nil {
		return err
	}
	hash, chain := parHash(data, prev)
	sum, err := (&Blake3sum{Sum33B: hash, Sum33Bchain: chain}).MarshalMsg(nil)
	if err != nil {
		return err
	}
	check, err := ByteSlice(sum).MarshalMsg(nil)
	if err != nil {
		return err
	}
	if int64(len(frame)) != target.Len0 || int64(len(check)) != target.Len2 {
		return ErrPARBounds
	}
	frame = append(frame, check...)
	n, err := wal.WriteAt(frame, target.Offset)
	if err != nil {
		return err
	}
	if n != len(frame) {
		return io.ErrShortWrite
	}
	if err = wal.Sync(); err != nil {
		return err
	}
	// A previous repair attempt may have created the file and then failed its
	// directory sync. Retrying must finish that publication too.
	if err := parSyncDir(filepath.Dir(path)); err != nil {
		return err
	}
	fi, err = wal.Stat()
	if err != nil {
		return err
	}
	verified := parReadEntry(wal, &target, fi.Size(), prev, true)
	return verified.Err
}

// parReadFrameAt bounds every claimed length by the actual WAL extent before
// allocation. It is used only when identifier damage prevents a trusted span.
func parReadFrameAt(fd *os.File, offset, size int64) ([]byte, int64, error) {
	if offset < 0 || offset >= size {
		return nil, 0, ErrPARBounds
	}
	var header [5]byte
	n, err := fd.ReadAt(header[:min(int64(len(header)), size-offset)], offset)
	if err != nil || n == 0 {
		return nil, 0, ErrPARFrame
	}
	var length int64
	var prefix int64
	switch header[0] {
	case bin8:
		if n < 2 {
			return nil, 0, ErrPARFrame
		}
		prefix, length = 2, int64(header[1])
	case bin16:
		if n < 3 {
			return nil, 0, ErrPARFrame
		}
		prefix, length = 3, int64(binary.BigEndian.Uint16(header[1:3]))
	case bin32:
		if n < 5 {
			return nil, 0, ErrPARFrame
		}
		prefix, length = 5, int64(binary.BigEndian.Uint32(header[1:5]))
	default:
		return nil, 0, ErrPARFrame
	}
	if length <= 0 || prefix+length > size-offset || length > int64(math.MaxInt) {
		return nil, 0, ErrPARBounds
	}
	data := make([]byte, int(length))
	if _, err := fd.ReadAt(data, offset+prefix); err != nil {
		return nil, 0, err
	}
	return data, prefix + length, nil
}

// parIdentifierCandidate proposes the first damaged/missing identifier whose
// WAL payload has its own intact checksum. This is not election or absence
// evidence: a checked peer must confirm the exact bytes before installation.
func parIdentifierCandidate(path, cluster string) (int64, *ParRecord, error) {
	fd, err := os.Open(path)
	if err != nil {
		return 0, nil, err
	}
	defer fd.Close()
	fi, err := fd.Stat()
	if err != nil {
		return 0, nil, err
	}
	ids, err := os.Open(path + ".parlog")
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return 0, nil, err
	}
	if ids != nil {
		defer ids.Close()
	}
	var prev *RaftLogEntry
	for slot, offset := int64(0), int64(0); offset < fi.Size(); slot++ {
		data, len0, err := parReadFrameAt(fd, offset, fi.Size())
		if err != nil {
			return 0, nil, err
		}
		check, len2, err := parReadFrameAt(fd, offset+len0, fi.Size())
		if err != nil {
			return 0, nil, err
		}
		var sum Blake3sum
		if rest, err := sum.UnmarshalMsg(check); err != nil || len(rest) != 0 {
			return 0, nil, ErrPARFrame
		}
		hash, _ := parHash(data, nil)
		if hash != sum.Sum33B {
			return 0, nil, ErrPARChecksum
		}
		var entry RaftLogEntry
		if rest, err := entry.UnmarshalMsg(data); err != nil || len(rest) != 0 || entry.Ticket == nil {
			return 0, nil, ErrPARFrame
		}
		if entry.Ticket.ClusterID != cluster || entry.PrevIndex != entry.Index-1 ||
			(prev != nil && (entry.PrevIndex != prev.Index || entry.PrevTerm != prev.Term)) {
			return 0, nil, ErrPARIdentity
		}
		pr := &ParRecord{Offset: offset, Len0: len0, Len2: len2, Index: entry.Index, Term: entry.Term,
			TicketID: entry.Ticket.TicketID, ClusterID: cluster, RLEblake3: hash}
		if err := parRecordBounds(pr); err != nil {
			return 0, nil, err
		}
		var raw [maxParRecord]byte
		if ids == nil {
			return slot, pr, nil
		}
		_, readErr := ids.ReadAt(raw[:], slot*maxParRecord)
		old, decodeErr := decodeParSlot(raw[:])
		if readErr != nil || decodeErr != nil {
			return slot, pr, nil
		}
		if !parSameIdentity(old, pr) || old.Offset != offset || old.Len0 != len0 || old.Len2 != len2 {
			return 0, nil, ErrPARConflict
		}
		prev = &entry
		offset += len0 + len2
	}
	return 0, nil, ErrPAREvidence
}

func parInstallIdentifier(path string, slot int64, target ParRecord, peerBytes []byte) (err error) {
	if slot < 0 || slot > math.MaxInt64/maxParRecord || parRecordBounds(&target) != nil {
		return ErrPARBounds
	}
	if _, err := parValidateBytes(&target, peerBytes); err != nil {
		return err
	}
	wal, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, wal.Close()) }()
	fi, err := wal.Stat()
	if err != nil {
		return err
	}
	local := parReadEntry(wal, &target, fi.Size(), nil, false)
	if local.Status != parIntact {
		return ErrPAREvidence
	}
	// Recheck the identifier at installation time. Never replace a different
	// intact identity merely because a peer holds another legitimate suffix.
	ids, err := os.OpenFile(path+".parlog", os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, ids.Close()) }()
	fi, err = ids.Stat()
	if err != nil {
		return err
	}
	if slot*maxParRecord > fi.Size() {
		return ErrPARBounds
	}
	var raw [maxParRecord]byte
	if _, err := ids.ReadAt(raw[:], slot*maxParRecord); err == nil {
		if old, decodeErr := decodeParSlot(raw[:]); decodeErr == nil {
			if *old == target {
				// Complete publication even if an earlier attempt wrote this slot
				// successfully but failed before syncing its new directory entry.
				return errors.Join(ids.Sync(), parSyncDir(filepath.Dir(path)))
			}
			return ErrPARConflict
		}
	}
	encoded, err := encodeParSlot(&target)
	if err != nil {
		return err
	}
	n, err := ids.WriteAt(encoded, slot*maxParRecord)
	if err != nil {
		return err
	}
	if n != len(encoded) {
		return io.ErrShortWrite
	}
	if err := ids.Sync(); err != nil {
		return err
	}
	return parSyncDir(filepath.Dir(path))
}

var ErrPARRecovering = errors.New("PAR: log recovery in progress")

// parNode is owned exclusively by TubeNode.Start. No recovery goroutine reads
// mutable Raft state or changes the WAL behind the event loop's back.
type parNode struct {
	active        bool
	unknown       bool
	metaFault     bool
	rebuildID     bool
	pendingLeader bool
	round         *parRound
	chunks        map[string]*parChunks
	slot          int
	tick          <-chan time.Time
	generation    uint64
	err           error
	nextSnapshot  time.Time
	applying      bool
	completions   []parCompletion
}

type parCompletion struct {
	ticket  *Ticket
	leader  bool
	respond bool
}

// Disk-backed application batches may finish tickets only after saving the
// resulting application/session state and LastApplied. Keep both local Done
// notifications and remote replies behind that boundary without per-entry syncs.
func (s *TubeNode) parQueueCompletion(ticket *Ticket, leader, respond bool) bool {
	if s.par == nil || !s.par.applying {
		return false
	}
	s.par.completions = append(s.par.completions, parCompletion{ticket: ticket, leader: leader, respond: respond})
	return true
}

func (s *TubeNode) parCompleteApply() {
	if s.par == nil || !s.par.applying {
		return
	}
	s.par.applying = false
	pending := s.par.completions
	s.par.completions = nil
	for _, completion := range pending {
		if completion.respond {
			s.respondToClientTicketApplied(completion.ticket)
		} else {
			s.FinishTicket(completion.ticket, completion.leader)
		}
	}
}

type parWire struct {
	Version int
	Cluster string
	Fence   parFence
	Record  ParRecord
	Member  string
	PeerID  string
	Kind    parEvidence
	Bytes   []byte
	Offset  int64
	Total   int64
}

type parChunks struct {
	next  int64
	total int64
	data  []byte
}

func parInventoryUnknown(inv *parInventory) bool {
	var end int64
	if inv.ParSize%maxParRecord != 0 {
		return true
	}
	for _, entry := range inv.Entries {
		if entry.Record == nil || entry.Status == parUnknown {
			return true
		}
		end = entry.Record.Offset + entry.Record.Len0 + entry.Record.Len2
	}
	return end < inv.WALSize || (len(inv.Entries) == 0 && inv.Err != nil)
}

// openPARWAL is the node-startup entry point. The traditional WAL opener remains
// a strict diagnostic/test API; it is deliberately not made to swallow damage.
func (cfg *TubeConfig) openPARWAL(path string) (*raftWriteAheadLog, error) {
	if cfg.NoDisk {
		return cfg.newRaftWriteAheadLogMemoryOnlyTestingOnly()
	}
	if err := parRecoverUndo(path); err != nil {
		return nil, err
	}
	_, walErr := os.Stat(path)
	_, parErr := os.Stat(path + ".parlog")
	_, stateErr := os.Stat(filepath.Join(filepath.Dir(path), "persistor.raftstate.tube.msgp"))
	if errors.Is(walErr, os.ErrNotExist) && errors.Is(parErr, os.ErrNotExist) && errors.Is(stateErr, os.ErrNotExist) {
		return cfg.newRaftWriteAheadLog(path, false)
	}
	inv, err := scanParWAL(path, cfg.ClusterID)
	if err != nil {
		return nil, err
	}
	intact := inv.Err == nil
	for _, entry := range inv.Entries {
		intact = intact && entry.Status == parIntact
	}
	if intact {
		return cfg.newRaftWriteAheadLog(path, false)
	}
	// A valid payload plus its independent identifier is enough to restore a
	// damaged checksum frame locally; no peer's mutable in-memory data is used.
	for slot, entry := range inv.Entries {
		if entry.Status == parDamaged && entry.Entry != nil {
			_ = repairParEntry(path, int64(slot), *entry.Record, entry.Bytes)
		}
	}
	inv, err = scanParWAL(path, cfg.ClusterID)
	if err != nil {
		return nil, err
	}
	intact = inv.Err == nil
	for _, entry := range inv.Entries {
		intact = intact && entry.Status == parIntact
	}
	if intact {
		return cfg.newRaftWriteAheadLog(path, false)
	}
	wal := &raftWriteAheadLog{path: path, name: cfg.MyName, parInv: inv, logIndex: newTermsRLE(), checkEach: blake3.New(64, nil),
		noLogCompaction: cfg.NoLogCompaction, isTest: cfg.isTest, testNum: cfg.testNum, compactionThresholdBytes: cfg.CompactionThresholdBytes}
	if wal.compactionThresholdBytes == 0 {
		wal.compactionThresholdBytes = 6 << 20
	}
	// Open existing evidence without creating a missing identifier file.
	fd, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	wal.fd = fd
	if fd != nil {
		wal.w, wal.r = msgp.NewWriter(fd), msgp.NewReader(fd)
	}
	p := &parLog{path: path + ".parlog", n: int64(len(inv.Entries)), durable: int64(len(inv.Entries))}
	pfd, err := os.OpenFile(p.path, os.O_RDWR, 0)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		if fd != nil {
			fd.Close()
		}
		return nil, err
	}
	if pfd != nil {
		p.fd = pfd
	}
	for _, entry := range inv.Entries {
		p.e = append(p.e, entry.Record)
	}
	wal.parlog = p
	parent, err := os.Open(filepath.Dir(path))
	if err != nil {
		if fd != nil {
			fd.Close()
		}
		p.close()
		return nil, err
	}
	wal.parentDirFd = parent
	prefix := true
	for i, entry := range inv.Entries {
		pr := entry.Record
		if pr == nil || entry.Status == parUnknown {
			break
		}
		if i == 0 {
			wal.logIndex.BaseC = pr.Index - 1
			wal.logIndex.Endi = pr.Index - 1
			if entry.Entry != nil {
				wal.logIndex.CompactTerm = entry.Entry.PrevTerm
			}
		}
		wal.logIndex.AddTerm(pr.Term)
		wal.lli, wal.llt = pr.Index, pr.Term
		prefix = prefix && entry.Status == parIntact
		if prefix {
			wal.raftLog = append(wal.raftLog, entry.Entry)
			wal.begseekpos = append(wal.begseekpos, pr.Offset)
		}
	}
	return wal, nil
}

func (s *TubeNode) parBlocked() bool { return s.par != nil && s.par.active }

func (s *TubeNode) parCampaignBlocked() bool {
	return s.par != nil && (s.par.unknown || (s.state.KVstore != nil && s.state.KVstore.parFault != nil))
}

func (s *TubeNode) parProtected() int64 {
	floor := max(s.state.CommitIndex, s.state.LastApplied, s.state.CompactionDiscardedLast.Index)
	if s.state.MC != nil {
		floor = max(floor, s.state.MC.CommitIndex)
	}
	return floor
}

// A no-op committed before a voter-set change does not by itself establish P1
// for every later set. Reuse replication acknowledgments to require the whole
// protected prefix on a quorum of the current voters before another change.
// Q1/Q2 still check configuration/term adoption separately. No extra messages
// or persistence are needed; normal replication can satisfy this condition.
func (s *TubeNode) parReconfigurationReady(mc *MemberConfig) bool {
	if s.par == nil {
		return true
	}
	if s.parBlocked() || mc == nil {
		return false
	}
	floor, count := s.parProtected(), 0
	for name := range mc.PeerNames.All() {
		if name == s.name {
			if s.lastLogIndex() >= floor {
				count++
			}
			continue
		}
		current := s.cktAllByName[name]
		if current == nil || current.ckt == nil || current.MC == nil || !current.MC.VersionEqual(mc) {
			continue
		}
		for _, peer := range s.peers {
			// MatchIndex can be raised from PeerLogLastIndex. A longer
			// divergent suffix is not evidence for this leader's prefix.
			if peer == nil || peer.PeerName != name || peer.MatchIndex < floor || peer.LargestCommonRaftIndex < floor {
				continue
			}
			// Prefix evidence belongs to the current connected incarnation and
			// current voter config. A stale per-peer replication record cannot
			// stand in for a disconnected or not-yet-adopted voter.
			if current.PeerID == "" || current.PeerID != peer.PeerID {
				continue
			}
			count++
			break // Duplicate/stale records cannot count a voter twice.
		}
	}
	return count >= mc.majority()
}

func (s *TubeNode) parAfterRestore() {
	if s.PeerServiceName != TUBE_REPLICA || s.cfg.NoDisk {
		return
	}
	s.par = &parNode{active: s.wal.parInv != nil}
	if s.wal.parInv != nil {
		s.par.unknown = parInventoryUnknown(s.wal.parInv)
	}
	anchor := s.state.CompactionDiscardedLast
	if s.wal.logIndex.BaseC > 0 && s.wal.logIndex.CompactTerm == 0 {
		if anchor.Index == s.wal.logIndex.BaseC {
			s.wal.logIndex.CompactTerm = anchor.Term
		} else {
			s.par.active, s.par.unknown = true, true
		}
	}
	if s.wal.lli == 0 && len(s.wal.raftLog) == 0 && anchor.Index > 0 && (!s.par.active || s.par.unknown) {
		// Unknown identifiers do not erase an independently saved snapshot
		// anchor. Elections remain fenced until the retained suffix is checked.
		s.wal.lli, s.wal.llt = anchor.Index, anchor.Term
		s.wal.logIndex.BaseC, s.wal.logIndex.Endi, s.wal.logIndex.CompactTerm = anchor.Index, anchor.Index, anchor.Term
	}
	if s.parProtected() > s.wal.lli {
		s.par.active, s.par.unknown, s.par.err = true, true, ErrPAREvidence
	}
	if s.state.KVstore != nil && s.state.KVstore.parFault != nil {
		// Application damage blocks campaigning/service, but does not make
		// independently checked WAL identities unfit for granting a vote.
		s.par.active, s.par.err = true, s.state.KVstore.parFault
	}
	if s.par.active {
		s.par.tick = time.After(100 * time.Millisecond)
	}
}

func (s *TubeNode) parReject(tkt *Ticket) bool {
	if !s.parBlocked() {
		return false
	}
	tkt.Err = ErrPARRecovering
	if tkt.FromID != s.PeerID {
		s.replyToForwardedTicketWithError(tkt)
	}
	// For a local membership request FinishTicket fills its inspection before
	// closing Done. Closing it in the reply helper first races that result.
	s.FinishTicket(tkt, false)
	return true
}

func (s *TubeNode) parMembers() map[string]string {
	m := make(map[string]string)
	if s.state.MC == nil {
		return m
	}
	for name, det := range s.state.MC.PeerNames.All() {
		id := det.PeerID
		if name == s.name {
			id = s.PeerID
		} else if c, ok := s.cktAllByName[name]; ok && c.ckt != nil {
			id = c.PeerID
		}
		if id == "" {
			id = "pending:" + name
		}
		m[name] = id
	}
	return m
}

func (s *TubeNode) parDonors(voters map[string]string) map[string]string {
	donors := make(map[string]string, len(voters))
	for name, id := range voters {
		donors[name] = id
	}
	for _, config := range []*MemberConfig{s.state.ShadowReplicas, s.state.Observers} {
		if config == nil || config.PeerNames == nil {
			continue
		}
		for name := range config.PeerNames.All() {
			if _, voter := voters[name]; voter {
				continue
			}
			if c, ok := s.cktAllByName[name]; ok && c.ckt != nil && c.ckt.RemoteServiceName == TUBE_REPLICA {
				donors[name] = c.PeerID
			}
		}
	}
	return donors
}

func (s *TubeNode) parFenceNow(round string) parFence {
	f := parFence{Leader: s.name, Term: s.state.CurrentTerm, Round: round, Generation: s.par.generation}
	if s.state.MC != nil {
		f.ConfigTerm, f.ConfigVersion = s.state.MC.ConfigTerm, s.state.MC.ConfigVersion
	}
	return f
}

func (s *TubeNode) parBecomeLeader() bool {
	if !s.parBlocked() {
		return false
	}
	if s.parCampaignBlocked() {
		return true
	}
	s.par.pendingLeader = true
	s.resetPreVoteState(false, true)
	s.role = LEADER
	s.leaderID, s.leaderName, s.leaderURL, s.leaderTerm = s.PeerID, s.name, s.URL, s.state.CurrentTerm
	s.leaderElectedTm = time.Now()
	s.leaderWasUpdated()
	s.resetElectionTimeout("PAR leader recovery")
	s.par.round = nil
	s.par.tick = time.After(time.Millisecond)
	return true
}

func (s *TubeNode) parRecoveryStep() {
	p := s.par
	if p == nil {
		return
	}
	if !p.active {
		p.tick = nil
		return
	}
	p.tick = time.After(200 * time.Millisecond)
	if p.metaFault || s.state.MC == nil {
		return
	}
	if s.state.KVstore != nil && s.state.KVstore.parFault != nil {
		s.parRequestSnapshot()
		return
	}
	if s.wal.parInv == nil {
		// An internally intact WAL/sidecar prefix can still end below the
		// saved commitment boundary. Startup gates that loss without creating
		// a sparse inventory; only a verified snapshot can restore the gap.
		s.parRequestSnapshot()
		return
	}
	inv := s.wal.parInv
	slot := -1
	var target *ParRecord
	rebuild := false
	if p.unknown {
		candidateSlot, candidate, err := parIdentifierCandidate(s.wal.path, s.ClusterID)
		if err == nil {
			slot, target, rebuild = int(candidateSlot), candidate, true
		} else {
			p.err = err
		}
	}
	if slot < 0 {
		for i, e := range inv.Entries {
			if e.Status == parDamaged && e.Record != nil {
				slot, target = i, e.Record
				break
			}
		}
	}
	if slot < 0 {
		if p.unknown {
			s.parRequestSnapshot()
		} else {
			s.parFinish()
		}
		return
	}
	members := s.parMembers()
	donors := s.parDonors(members)
	reset := p.round == nil || p.slot != slot || p.rebuildID != rebuild
	if !reset {
		reset = p.round.fence != s.parFenceNow(p.round.fence.Round) || len(members) != len(p.round.members) || len(donors) != len(p.round.donors)
		for name, id := range members {
			reset = reset || p.round.members[name] != id
		}
		for name, id := range donors {
			reset = reset || p.round.donors[name] != id
		}
	}
	if reset {
		fence := s.parFenceNow(rpc.NewCallID("par"))
		round, err := newParRound(fence, *target, members, s.parProtected(), donors)
		if err != nil {
			p.err = err
			return
		}
		p.round, p.slot = round, slot
		p.rebuildID = rebuild
		p.chunks = nil
	}
	for name := range donors {
		if name == s.name {
			continue
		}
		if c, ok := s.cktAllByName[name]; ok && c.ckt != nil {
			msg := parWire{Version: 1, Cluster: s.ClusterID, Fence: p.round.fence, Record: p.round.target, Member: s.name, PeerID: s.PeerID}
			s.parSend(c.ckt, PARQueryMsg, msg)
		}
	}
	decision, data, err := p.round.decide(s.parFenceNow(p.round.fence.Round))
	if err != nil {
		p.err = err
		return
	}
	if decision == parRepair {
		var err error
		if rebuild {
			err = parInstallIdentifier(s.wal.path, int64(slot), p.round.target, data)
		} else {
			err = repairParEntry(s.wal.path, int64(slot), p.round.target, data)
		}
		if err != nil {
			p.err = err
			return
		}
		updated, err := scanParWAL(s.wal.path, s.ClusterID)
		if err != nil {
			p.err = err
			return
		}
		s.wal.parInv = updated
		p.unknown = parInventoryUnknown(updated)
		last := s.state.CompactionDiscardedLast.Index
		for _, entry := range updated.Entries {
			if entry.Record != nil {
				last = max(last, entry.Record.Index)
			}
		}
		p.unknown = p.unknown || last < s.parProtected()
		p.round = nil
		p.tick = time.After(time.Millisecond)
	}
	if decision == parDiscard && !rebuild && s.role == LEADER {
		if err := s.parDiscardSuffix(); err != nil {
			p.err = err
		}
	}
}

// parDiscardSuffix installs only a current election/configuration certificate.
// Under Tube's safe logless reconfiguration rules, P1 preserves the committed
// prefix when voter sets change. MC.CommitIndex carries that known barrier into
// recovery. A majority of OTHER voters in the exact current configuration and
// term must affirm absence; missing/damaged identifiers cannot cast that vote.
// Configuration/incarnation changes invalidate the certificate rather than
// combining old and new quorums. Followers never discard on such a certificate.
func (s *TubeNode) parDiscardSuffix() error {
	p := s.par
	if p == nil || !p.active || p.unknown || p.round == nil || p.rebuildID || s.role != LEADER ||
		s.leaderID != s.PeerID || s.leaderTerm != s.state.CurrentTerm {
		return ErrPARStale
	}
	if _, voter := p.round.members[s.name]; !voter {
		return ErrPARStale
	}
	current := s.parFenceNow(p.round.fence.Round)
	members := s.parMembers()
	if len(members) != len(p.round.members) {
		return ErrPARStale
	}
	for name, id := range members {
		if p.round.members[name] != id {
			return ErrPARStale
		}
	}
	decision, _, err := p.round.decide(current)
	if err != nil {
		return err
	}
	if decision != parDiscard || p.round.target.Index <= s.parProtected() {
		return ErrPARConflict
	}
	inv, err := scanParWAL(s.wal.path, s.ClusterID)
	if err != nil {
		return err
	}
	if parInventoryUnknown(inv) || p.slot < 0 || p.slot >= len(inv.Entries) ||
		inv.Entries[p.slot].Record == nil || *inv.Entries[p.slot].Record != p.round.target {
		return ErrPAREvidence
	}
	for _, entry := range inv.Entries[:p.slot] {
		if entry.Status != parIntact {
			return ErrPAREvidence
		}
	}
	undo, err := parBeginUndo(s.wal.path)
	if err != nil {
		return err
	}
	// A failed publication keeps its durable before-image and service gate.
	// Startup restores it before opening either member of the pair.
	for i, path := range []string{s.wal.path, s.wal.path + ".parlog"} {
		fd, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
		if err != nil {
			p.unknown = true
			return err
		}
		length := p.round.target.Offset
		if i == 1 {
			length = int64(p.slot) * maxParRecord
		}
		err = errors.Join(fd.Truncate(length), fd.Sync(), fd.Close())
		if err != nil {
			p.unknown = true
			return err
		}
	}
	if err := undo.finish(); err != nil {
		p.unknown = true
		return err
	}
	p.round, p.chunks = nil, nil
	p.generation++
	s.wal.parInv, err = scanParWAL(s.wal.path, s.ClusterID)
	if err != nil {
		p.unknown = true
		return err
	}
	p.tick = time.After(time.Millisecond)
	return nil
}

func (s *TubeNode) parSend(ckt *rpc.Circuit, op int, msg parWire) {
	if msg.Kind == parValid && len(msg.Bytes) > 0 {
		const chunkSize = rpc.UserMaxPayload / 2
		data := msg.Bytes
		msg.Total = int64(len(data))
		for offset := 0; offset < len(data); offset += chunkSize {
			part := msg
			part.Offset = int64(offset)
			part.Bytes = data[offset:min(offset+chunkSize, len(data))]
			s.parSendPart(ckt, op, part)
		}
		return
	}
	s.parSendPart(ckt, op, msg)
}

func (s *TubeNode) parSendPart(ckt *rpc.Circuit, op int, msg parWire) {
	b, err := json.Marshal(msg)
	if err != nil || len(b) > rpc.UserMaxPayload {
		return
	}
	frag := s.newFrag()
	frag.FragOp = op
	frag.FragSubject = "PAR recovery v1"
	frag.Payload = b
	_ = s.SendOneWay(ckt, frag, -1, 0)
}

func (s *TubeNode) parHandle(frag *rpc.Fragment, ckt *rpc.Circuit) {
	if s.par == nil || ckt == nil || len(frag.Payload) > rpc.UserMaxPayload {
		return
	}
	var msg parWire
	if err := json.Unmarshal(frag.Payload, &msg); err != nil {
		return
	}
	if msg.Version != 1 || msg.Cluster != s.ClusterID || msg.Member != ckt.RemotePeerName || msg.PeerID != ckt.RemotePeerID {
		return
	}
	if s.state.MC == nil {
		return
	}
	_, voter := s.state.MC.PeerNames.Get2(msg.Member)
	if !voter {
		configured := false
		for _, config := range []*MemberConfig{s.state.ShadowReplicas, s.state.Observers} {
			if config != nil && config.PeerNames != nil {
				_, known := config.PeerNames.Get2(msg.Member)
				configured = configured || known
			}
		}
		if !configured || ckt.RemoteServiceName != TUBE_REPLICA {
			return
		}
	}
	if c, ok := s.cktall[ckt.RemotePeerID]; ok {
		// The fence describes the requester's configuration, not necessarily
		// the donor's. PAR traffic proves liveness, never remote membership
		// adoption or replication progress for logless reconfiguration.
		s.seenIndirect(c, time.Now())
	}
	if frag.FragOp == PARReplyMsg {
		p := s.par
		if p.round == nil {
			return
		}
		if (msg.Kind == parCompacted || msg.Kind == parAbsent) && msg.Fence == s.parFenceNow(p.round.fence.Round) &&
			parSameIdentity(&msg.Record, &p.round.target) && ckt.RemotePeerID == s.leaderID && s.role == FOLLOWER {
			// The accepted leader may have compacted this history or replaced
			// an obsolete uncommitted identity. Its checked snapshot must still
			// cover our protected floor; followers never discard on this reply.
			s.parRequestSnapshot()
		}
		if msg.Kind == parValid {
			data, complete, err := p.joinReply(s.parFenceNow(p.round.fence.Round), msg)
			if err != nil {
				p.err = err
				return
			}
			if !complete {
				return
			}
			msg.Bytes = data
		}
		reply := parReply{Fence: msg.Fence, Member: msg.Member, PeerID: msg.PeerID, Kind: msg.Kind, Record: msg.Record, Bytes: msg.Bytes}
		if err := p.round.add(s.parFenceNow(p.round.fence.Round), reply); err != nil {
			p.err = err
			return
		}
		p.tick = time.After(time.Millisecond)
		return
	}
	if msg.Fence.Leader != msg.Member || msg.Fence.Term < s.state.CurrentTerm {
		return
	}
	matchingConfig := msg.Fence.ConfigTerm == s.state.MC.ConfigTerm && msg.Fence.ConfigVersion == s.state.MC.ConfigVersion
	if msg.Fence.Term > s.state.CurrentTerm {
		s.becomeFollower(msg.Fence.Term, nil, SAVE_STATE)
	}
	if parRecordBounds(&msg.Record) != nil {
		return
	}
	reply := parWire{Version: 1, Cluster: s.ClusterID, Fence: msg.Fence, Record: msg.Record, Member: s.name, PeerID: s.PeerID, Kind: parUnavailable}
	inv, err := scanParWAL(s.wal.path, s.ClusterID)
	if err == nil {
		last := s.wal.logIndex.BaseC
		for _, entry := range inv.Entries {
			if entry.Record != nil {
				last = max(last, entry.Record.Index)
			}
		}
		canAssertAbsence := matchingConfig && !parInventoryUnknown(inv) && !s.par.unknown && last >= s.parProtected()
		if msg.Record.Index <= s.wal.logIndex.BaseC {
			reply.Kind = parCompacted
		} else {
			found := false
			for _, entry := range inv.Entries {
				if entry.Record == nil || entry.Record.Index != msg.Record.Index {
					continue
				}
				found = true
				if parSameIdentity(entry.Record, &msg.Record) {
					reply.Kind = parKnownDamaged
					if entry.Entry != nil {
						reply.Kind, reply.Bytes = parValid, entry.Bytes
					}
				} else if canAssertAbsence {
					reply.Kind = parAbsent
				}
				break
			}
			if !found && canAssertAbsence {
				reply.Kind = parAbsent
			}
		}
	}
	s.parSend(ckt, PARReplyMsg, reply)
}

func (s *TubeNode) parRequestSnapshot() {
	if s.role != FOLLOWER || s.leaderID == "" || time.Now().Before(s.par.nextSnapshot) {
		return
	}
	c, ok := s.cktall[s.leaderID]
	if !ok || c.ckt == nil {
		return
	}
	s.par.nextSnapshot = time.Now().Add(time.Second)
	request := s.newFrag()
	request.FragOp = RequestStateSnapshot
	request.FragSubject = "PAR needs verified snapshot history"
	_ = s.SendOneWay(c.ckt, request, -1, 0)
}

func (s *TubeNode) parSnapshotAllowed(snap *Snapshot) bool {
	if snap == nil || snap.State == nil {
		return false
	}
	// A delayed snapshot from the same leader/term must not revive an older
	// voter set after newer AppendEntries have already deactivated it.
	if s.state != nil && s.state.MC != nil && snap.State.MC != nil && s.state.MC.VersionGT(snap.State.MC) {
		return false
	}
	if !s.parBlocked() {
		return true
	}
	if s.par.metaFault || s.role != FOLLOWER || s.leaderID == "" {
		return false // A peer snapshot cannot repair this node's voting history.
	}
	state := snap.State
	if state.ClusterID != s.ClusterID || state.PeerID != s.leaderID || state.PeerName != s.leaderName ||
		state.CurrentTerm != s.state.CurrentTerm || state.LastApplied < s.parProtected() ||
		state.CommitIndex < state.LastApplied || state.CompactionDiscardedLast.Index != state.LastApplied ||
		state.CompactionDiscardedLast.Term != state.LastAppliedTerm || state.KVstore == nil || !state.KVstore.SnapshotValid ||
		state.KVstore.parFault != nil || state.MC == nil || s.state.MC == nil {
		return false
	}
	return state.MC.ConfigTerm == s.state.MC.ConfigTerm && state.MC.ConfigVersion == s.state.MC.ConfigVersion
}

// The enclosing snapshot journal is already durable. These handles contain no
// new writes to publish; replace even missing/corrupt sidecars without decoding
// them. A failure remains recoverable from the journal on restart.
// A checked portable before-image is sufficient to restore this node locally.
// Preserve the unreadable destination in the same undo journal used by peer
// snapshot installation. Startup rolls back any still-published journal so an
// interrupted attempt can be retried.
func (s *TubeNode) parRestorePortableApplication() error {
	kv := s.state.KVstore
	if s.cfg.NoDisk || kv == nil || kv.parFault == nil || !kv.SnapshotValid {
		return nil
	}
	undo, err := parBeginUndoWithIO(s.wal.path, s.saver.parIO, s.state)
	if err != nil {
		return err
	}
	replacement := kv.clone()
	replacement.parFS = kv.parFS
	// These rows came from the checked local state image, rather than from
	// the unavailable destination. Make that image installable in a fresh DB.
	replacement.parFault, replacement.parFaultPath = nil, ""
	defer func() {
		if s.state.KVstore != replacement {
			replacement.Close()
		}
	}()
	kv.Close() // Close the old backend before reusing any of its pathnames.
	path := s.GetKVStorePath()
	if err := os.RemoveAll(path); err != nil {
		return err
	}
	if err := parSyncDir(filepath.Dir(path)); err != nil {
		return err
	}
	if err := replacement.Attach(path, false); err != nil {
		return err
	}
	s.state.KVstore = replacement
	if _, err := s.saver.save(s.state); err != nil {
		return err
	}
	return undo.finish()
}

func (s *TubeNode) parPrepareApplicationSnapshot() error {
	if s.state.KVstore == nil || s.state.KVstore.parFault == nil {
		return nil
	}
	if s.wal.parTxn == nil {
		return ErrPAREvidence
	}
	s.state.KVstore.Close()
	// The published journal contains the opaque damaged store. A peer snapshot
	// must install into a fresh directory rather than trying to open that store.
	if err := os.RemoveAll(s.GetKVStorePath()); err != nil {
		return err
	}
	return parSyncDir(filepath.Dir(s.GetKVStorePath()))
}

func (s *raftWriteAheadLog) parPrepareSnapshotWAL() error {
	if s.parTxn == nil {
		return ErrPAREvidence
	}
	if s.fd != nil {
		if err := s.fd.Close(); err != nil {
			return err
		}
	}
	if s.parlog != nil {
		if s.parlog.fd != nil {
			if err := s.parlog.fd.Close(); err != nil {
				return err
			}
		}
		if s.parlog.parentDirFd != nil {
			if err := s.parlog.parentDirFd.Close(); err != nil {
				return err
			}
		}
	}
	fd, err := os.OpenFile(s.path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	pfd, err := os.OpenFile(s.path+".parlog", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		fd.Close()
		return err
	}
	s.fd, s.w, s.r = fd, msgp.NewWriter(fd), msgp.NewReader(fd)
	s.parlog = &parLog{path: s.path + ".parlog", fd: pfd}
	s.parInv = nil
	return nil
}

func (s *TubeNode) parFinish() {
	p := s.par
	if s.state.KVstore != nil && s.state.KVstore.parFault != nil {
		return
	}
	inv, err := scanParWAL(s.wal.path, s.ClusterID)
	if err != nil || inv.Err != nil {
		p.err = ErrPAREvidence
		return
	}
	for _, entry := range inv.Entries {
		if entry.Status != parIntact {
			return
		}
	}
	fresh, err := s.cfg.newRaftWriteAheadLog(s.wal.path, false)
	if err != nil {
		p.err = err
		return
	}
	if len(fresh.raftLog) == 0 {
		anchor := s.state.CompactionDiscardedLast
		fresh.logIndex.BaseC, fresh.logIndex.Endi, fresh.logIndex.CompactTerm = anchor.Index, anchor.Index, anchor.Term
		fresh.lli, fresh.llt = anchor.Index, anchor.Term
	}
	anchor := s.state.CompactionDiscardedLast
	if fresh.logIndex.BaseC != anchor.Index || fresh.logIndex.CompactTerm != anchor.Term {
		// Reconstructing entry identities cannot invent missing application
		// state or change the saved snapshot boundary. Obtain a snapshot first.
		p.unknown = true
		p.err = errors.Join(ErrPAREvidence, fresh.close())
		return
	}
	// The previous recovery handle has no staged writes to publish.
	old := s.wal
	s.wal = fresh
	if old.fd != nil {
		old.fd.Close()
	}
	if old.parlog != nil && old.parlog.fd != nil {
		old.parlog.fd.Close()
	}
	if old.parentDirFd != nil {
		old.parentDirFd.Close()
	}
	p.active, p.unknown, p.round = false, false, nil
	p.chunks = nil
	p.generation++
	if p.pendingLeader && s.role == LEADER {
		p.pendingLeader = false
		s.role = CANDIDATE
		s.becomeLeader()
	}
}

// A recovering follower still obeys terms/leader liveness, but never executes or
// acknowledges entries across its unresolved holes.
func (s *TubeNode) parObserveAE(ae *AppendEntries) {
	if ae.ClusterID != s.ClusterID || ae.LeaderTerm < s.state.CurrentTerm {
		return
	}
	var confirmed *MemberConfig
	if ae.MC != nil && s.state.MC != nil && ae.MC.VersionGT(s.state.MC) {
		// Recovery still adopts newer logless configurations from the leader.
		// Otherwise a replica removed/reconfigured while down can never admit
		// that leader's snapshot, whose configuration must match the local one.
		// The changed fence invalidates all evidence from the previous round.
		confirmed = ae.MC
	}
	if ae.LeaderTerm > s.state.CurrentTerm || s.role != FOLLOWER ||
		(confirmed != nil && confirmed.VersionGT(s.state.MC)) {
		s.becomeFollower(ae.LeaderTerm, confirmed, SAVE_STATE)
	}
	s.leaderID, s.leaderName, s.leaderURL, s.leaderTerm = ae.LeaderID, ae.FromPeerName, ae.LeaderURL, ae.LeaderTerm
	s.lastLegitAppendEntries = time.Now()
	s.resetElectionTimeout("PAR follower observing leader")
	s.leaderWasUpdated()
}

// joinReply bounds reassembly by the independently checked requested entry span.
// Only configured incarnations in the current round may allocate a buffer. Lost
// or reordered parts are retried by the ordinary recovery timer, not accepted as
// a complete entry or counted as an absence assertion.
func (p *parNode) joinReply(current parFence, msg parWire) ([]byte, bool, error) {
	r := p.round
	if r == nil || current != r.fence || msg.Fence != r.fence {
		return nil, false, ErrPARStale
	}
	if id, ok := r.donors[msg.Member]; !ok || id != msg.PeerID || msg.Member == r.fence.Leader {
		return nil, false, ErrPARStale
	}
	if !parSameIdentity(&r.target, &msg.Record) {
		return nil, false, ErrPARIdentity
	}
	if msg.Total <= 0 || msg.Total > r.target.Len0 || msg.Offset < 0 || msg.Offset > msg.Total || int64(len(msg.Bytes)) > msg.Total-msg.Offset || len(msg.Bytes) == 0 {
		return nil, false, ErrPARBounds
	}
	if p.chunks == nil {
		p.chunks = make(map[string]*parChunks)
	}
	if msg.Offset == 0 {
		p.chunks[msg.Member] = &parChunks{total: msg.Total}
	}
	part := p.chunks[msg.Member]
	if part == nil || part.total != msg.Total || part.next != msg.Offset {
		return nil, false, ErrPAREvidence
	}
	part.data = append(part.data, msg.Bytes...)
	part.next += int64(len(msg.Bytes))
	if part.next != part.total {
		return nil, false, nil
	}
	delete(p.chunks, msg.Member)
	return part.data, true, nil
}
