package tube

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/glycerine/blake3"
)

type raftStatePersistor struct {
	name        string
	path        string
	kvPath      string
	fd          *os.File
	parentDirFd *os.File

	// check each TubeLogEntry entry.
	checkEach *blake3.Hasher

	rpos int
	wpos int

	nodisk         bool
	parMetaEnabled bool
	parMeta        *parLocalMeta
	parMetaDirty   bool
	parInitial     bool // first state publication has no previous state to fall back to
	parIO          parUndoIO
}

func raftStatePersistorKVPath(path string, node *TubeNode) string {
	if node != nil && node.cfg.DataDir != "" {
		return node.GetKVStorePath()
	}
	return path + ".yogadb"
}

func (cfg *TubeConfig) newRaftStatePersistor_NODISK(node *TubeNode) (s *raftStatePersistor, state *RaftState, error error) {
	s = &raftStatePersistor{
		nodisk: true,
		name:   node.name,
	}
	if node == nil {
		// just a precaution. The persistor_tests will hit this
		// iff the use NoDisk, which they should not.
		panic("unsupported -- persistor_tests really need to read/write actual disk")
	} else {
		state = node.newRaftState()
	}
	return
}

func (cfg *TubeConfig) NewRaftStatePersistor(path string, node *TubeNode, readOnly bool) (s *raftStatePersistor, state *RaftState, error error) {
	return cfg.newRaftStatePersistor(path, node, readOnly, false)
}

// Only the node-startup path may keep an unavailable application behind PAR's
// service gate. The public diagnostic opener remains strict about store errors.
func (cfg *TubeConfig) newRaftStatePersistor(path string, node *TubeNode, readOnly, recoverApplication bool) (s *raftStatePersistor, state *RaftState, error error) {
	//vv("NewRaftStatePersistor(path = '%v'; nodisk=%v)", path,cfg.NoDisk)
	if cfg.NoDisk {
		return cfg.newRaftStatePersistor_NODISK(node)
	}
	// make dir if necessary
	dir := filepath.Dir(path)
	if !readOnly {
		panicOn(os.MkdirAll(dir, 0700))
	}

	var sz int64
	fi, err := os.Stat(path)
	if err == nil {
		sz = fi.Size()
	}
	fd, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, nil, err
	}
	panicOn(fd.Sync()) // per tigerbeetle/wal.go:217
	panicOn(fd.Close())
	s = &raftStatePersistor{
		path:           path,
		kvPath:         raftStatePersistorKVPath(path, node),
		checkEach:      blake3.New(64, nil),
		parMetaEnabled: node != nil,
	}
	opened := false
	defer func() {
		if !opened && s != nil {
			s.close()
			if state != nil && state.KVstore != nil {
				state.KVstore.Close()
			}
		}
	}()
	if node != nil {
		s.name = node.name
	}

	// parent directory metadata must also be synced
	// to disk for true persistence.
	dir2, err2 := getActualParentDirForFsync(path)
	panicOn(err2)
	s.parentDirFd, err = os.Open(dir2)
	panicOn(err)
	resumedInitial := false
	if s.parMetaEnabled {
		s.parMeta, s.parMetaDirty, err = parLoadLocalMetaDetail(path)
		if err == nil && sz == 0 && !readOnly {
			state, err = s.parReadInitialState()
			resumedInitial = state != nil
			if err == nil && resumedInitial {
				err = s.parSaveLocalMeta(state)
			}
		}
		if err == nil && sz > 0 && s.parMeta == nil {
			err = ErrPAREvidence
		}
		if err != nil {
			if state != nil && state.KVstore != nil {
				state.KVstore.Close()
			}
			s.close()
			return nil, nil, err
		}
	}

	if sz == 0 && !resumedInitial {
		if s.parMeta != nil {
			s.close()
			return nil, nil, ErrPAREvidence
		}
		// file started empty, fine. nothing to load.
		s.parInitial = s.parMetaEnabled
		opened = true
		return
	}
	if !resumedInitial {
		state, _, err = s.load()
		if err == io.EOF {
			//vv("breaking on EOF at j = %v", j)
			err = nil
		}
	}
	panicOn(err)
	if err = s.parMeta.merge(state); err != nil {
		s.close()
		return nil, nil, err
	}
	if s.parMetaDirty && !readOnly {
		if err = s.parSaveLocalMeta(state); err != nil {
			s.close()
			return nil, nil, err
		}
	}
	if state != nil && state.KVstore != nil {
		portable := state.KVstore.SnapshotValid
		image := KVStore{Leafz: state.KVstore.Leafz, Tables: state.KVstore.Tables,
			LeafTables: state.KVstore.LeafTables, SnapshotValid: portable}
		err = state.KVstore.Open(s.kvPath, false)
		if err == nil {
			var restored *RaftState
			restored, err = s.parRestoreApplication(state, portable)
			if err == nil {
				state = restored
			}
		}
		if err == nil && recoverApplication && !readOnly && len(state.KVstore.db.CheckIntegrity()) != 0 {
			// The checkpoint alone does not validate unrelated application
			// pages. Audit once at node startup, outside the serving loop.
			err = ErrPARChecksum
		}
		if err != nil {
			// Keep the original state on a failed checkpoint restoration too.
			// A peer can replace application rows, never local voting history.
			if !recoverApplication || readOnly || node == nil || node.PeerServiceName != TUBE_REPLICA || errors.Is(err, ErrPARConflict) {
				if state != nil && state.KVstore != nil {
					state.KVstore.Close()
				}
				s.close()
				return nil, nil, err
			}
			state.KVstore.Close()
			state.KVstore = &image
			state.KVstore.PostLoadHook() // Retain any checked portable before-image.
			state.KVstore.parFault, state.KVstore.parFaultPath = err, s.kvPath
		}
	}
	if resumedInitial {
		// Publish only after the portable initial rows/checkpoint are restored.
		// The initial image remains available if this save is interrupted too.
		_, err = s.save(state)
		panicOn(err)
	}
	if s.parMetaEnabled && !readOnly {
		panicOn(s.parRetireInitialState())
	}
	opened = true
	return
}

func (s *raftStatePersistor) close() (err error) {
	// no path files are open between saves now.
	// but the parent dir file handle is open.
	if s.parentDirFd != nil {
		err = s.parentDirFd.Close()
		s.parentDirFd = nil
	}
	return
}

func (s *raftStatePersistor) sync(needParent bool) error {
	if s.nodisk {
		return nil
	}
	err := s.fd.Sync()
	panicOn(err)

	if needParent {
		err = s.parentDirFd.Sync()
		panicOn(err)
	}

	return nil
}

func (s *raftStatePersistor) save(state *RaftState) (nw int64, err error) {

	if s == nil {
		return
	}
	active := state.KVstore != nil && state.KVstore.parTx != nil
	if active {
		state, err = parPriorApplication(state)
		panicOn(err)
	}
	state.LastSaveTimestamp = time.Now()

	if s.nodisk {
		return 0, nil
	}
	if s.parInitial {
		panicOn(s.parStageInitialState(state))
	}
	panicOn(s.parSaveLocalMeta(state))
	appFault := state.KVstore != nil && state.KVstore.parFault != nil
	if state.KVstore != nil && !appFault && !state.KVstore.isPersistent() {
		panicOn(state.KVstore.Attach(s.kvPath, false))
	}
	// LastApplied may never become durable ahead of the state machine it names.
	if state.KVstore != nil && !appFault && !active {
		panicOn(s.parSaveApplication(state))
	}
	tmppath := s.path + ".pre_rename." + cryRand15B()
	fd, err := os.Create(tmppath)
	panicOn(err)
	defer os.Remove(tmppath)
	defer fd.Close()

	b, err := state.MarshalMsg(nil)
	panicOn(err)

	by, err := ByteSlice(b).MarshalMsg(nil)
	panicOn(err)
	_, err = fd.Write(by)
	panicOn(err)
	err = s.saveBlake3sumFor(b, fd)
	panicOn(err)
	err = s.parIO.sync(fd)
	panicOn(err)
	err = fd.Close()
	panicOn(err)

	//archivePath := s.path + fmt.Sprintf(".prev.state.%v.%v", ts(), cryRand15B())

	//vv("%v preserving old state before installing new state. old: from '%v' -> '%v'", s.name, s.path, archivePath)

	// os.Rename(oldpath, newpath)
	//err = os.Rename(s.path, archivePath)
	//panicOn(err)

	err = s.parIO.rename(tmppath, s.path)
	panicOn(err)
	err = s.parIO.sync(s.parentDirFd)
	panicOn(err)
	if s.parInitial {
		panicOn(s.parRetireInitialState())
		s.parInitial = false
	}
	return
}

func (s *raftStatePersistor) load() (state *RaftState, nr int64, err error) {
	fd, err := os.OpenFile(s.path, os.O_RDWR, 0)
	if err != nil {
		return
	}
	defer func() { err = errors.Join(err, fd.Close()) }()
	if err = fd.Sync(); err != nil {
		return
	}
	// Read only actual file bytes. A damaged bin32 prefix cannot allocate its
	// claimed length or decode application rows before integrity is checked.
	data, err := io.ReadAll(fd)
	if err != nil {
		return nil, 0, err
	}
	state, err = parDecodeState(data)
	if err != nil {
		return nil, 0, err
	}
	nr = int64(len(data))
	s.rpos += len(data)
	return
}

func (s *raftStatePersistor) loadBlake3sum(fd *os.File) (bla *Blake3sum, err error) {
	//vv("load called")

	readme, err := nextframe(fd, s.path)
	if err == io.EOF {
		return nil, err
	}
	panicOn(err)

	var by ByteSlice
	_, err = by.UnmarshalMsg(readme)
	//vv("load sees err=%v from msgp.Decode; len(by)=%v", err, len(by))
	panicOn(err)
	if err != nil {
		return
	}
	s.rpos += len(by)

	bla = &Blake3sum{}
	_, err = bla.UnmarshalMsg(by)
	panicOn(err)

	return
}

// does not fsync internally.
func (s *raftStatePersistor) saveBlake3sumFor(by []byte, fd *os.File) (err error) {

	s.checkEach.Reset()
	s.checkEach.Write(by)
	h := blake3ToString33B(s.checkEach)

	//vv("persistor at len %v, checkEach h = '%v'", curpos(fd), h)

	blak := &Blake3sum{
		Sum33B: h,
	}
	b, err := blak.MarshalMsg(nil)
	panicOn(err)

	by2, err := ByteSlice(b).MarshalMsg(nil)
	panicOn(err)
	_, err = fd.Write(by2)
	panicOn(err)
	return
}
