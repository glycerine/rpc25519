package tube

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/glycerine/blake3"
)

type raftStatePersistor struct {
	name        string
	path        string
	fd          *os.File
	parentDirFd *os.File

	// check each TubeLogEntry entry.
	checkEach *blake3.Hasher

	rpos int
	wpos int

	nodisk bool
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
	fd.Close()
	s = &raftStatePersistor{
		path:      path,
		checkEach: blake3.New(64, nil),
	}
	if node != nil {
		s.name = node.name
	}

	// parent directory metadata must also be synced
	// to disk for true persistence.
	dir2, err2 := getActualParentDirForFsync(path)
	panicOn(err2)
	s.parentDirFd, err = os.Open(dir2)
	panicOn(err)

	if sz == 0 {
		// file started empty, fine. nothing to load.
		return
	}
	state, _, err = s.load()
	if err == io.EOF {
		//vv("breaking on EOF at j = %v", j)
		err = nil
	}
	panicOn(err)
	return
}

func (s *raftStatePersistor) close() (err error) {
	// no files are open between saves now.
	return
}

func (s *raftStatePersistor) sync() error {
	if s.nodisk {
		return nil
	}
	err := s.fd.Sync()
	panicOn(err)
	err = s.parentDirFd.Sync()
	panicOn(err)
	return nil
}

func (s *raftStatePersistor) save(state *RaftState) (nw int64, err error) {

	if s == nil {
		return
	}
	state.LastSaveTimestamp = time.Now()

	if s.nodisk {
		return 0, nil
	}
	tmppath := s.path + ".pre_rename." + cryRand15B()
	fd, err := os.Create(tmppath)
	panicOn(err)

	b, err := state.MarshalMsg(nil)
	panicOn(err)

	by, err := ByteSlice(b).MarshalMsg(nil)
	panicOn(err)
	_, err = fd.Write(by)
	panicOn(err)
	err = s.saveBlake3sumFor(b, fd)
	panicOn(err)
	fd.Close()

	//archivePath := s.path + fmt.Sprintf(".prev.state.%v.%v", ts(), cryRand15B())

	//vv("%v preserving old state before installing new state. old: from '%v' -> '%v'", s.name, s.path, archivePath)

	// os.Rename(oldpath, newpath)
	//err = os.Rename(s.path, archivePath)
	//panicOn(err)

	err = os.Rename(tmppath, s.path)
	panicOn(err)
	err = s.parentDirFd.Sync()
	panicOn(err)
	return
}

func (s *raftStatePersistor) load() (state *RaftState, nr int64, err error) {
	//vv("load called")
	if s.nodisk {
		// load() only called in disk-using newRaftStatePersistor()
		// so nothing special needed here.
	}

	fd, err := os.OpenFile(s.path, os.O_RDWR|os.O_CREATE, 0644)
	panicOn(err)
	defer fd.Close()
	readme, err := nextframe(fd, s.path)
	panicOn(err)

	var by ByteSlice
	_, err = by.UnmarshalMsg(readme)
	//vv("load sees err=%v from UnmarshalMsg len(by)=%v", err, len(by))
	panicOn(err)
	if err != nil {
		return
	}
	s.rpos += len(by)

	state = &RaftState{}
	_, err = state.UnmarshalMsg(by)
	panicOn(err)
	//vv("ds = '%#v'", ds)

	// read the checksum from disk
	onDisk, err := s.loadBlake3sum(fd)
	panicOn(err)

	// verify checksum (really a cryptographic hash)
	s.checkEach.Reset()
	s.checkEach.Write(by)
	h := blake3ToString33B(s.checkEach)

	if h != onDisk.Sum33B {
		panic(fmt.Sprintf("corrupt RaftState '%v' at pos '%v'. onDisk.Sum:'%v'; vs. re-computed-hash: '%v'", s.path, s.rpos, onDisk.Sum33B, h))
	}

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

func (s *raftStatePersistor) saveBlake3sumFor(by []byte, fd *os.File) (err error) {

	s.checkEach.Reset()
	s.checkEach.Write(by)
	h := blake3ToString33B(s.checkEach)

	blak := &Blake3sum{
		Sum33B: h,
	}
	b, err := blak.MarshalMsg(nil)
	panicOn(err)

	by2, err := ByteSlice(b).MarshalMsg(nil)
	panicOn(err)
	_, err = fd.Write(by2)
	panicOn(err)
	// caller is about to close
	//err = s.sync()
	//panicOn(err)
	return
}
