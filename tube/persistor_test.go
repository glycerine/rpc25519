package tube

import (
	"bytes"
	"fmt"
	"os"
	"testing"
	"time"

	rpc "github.com/glycerine/rpc25519"
	//"github.com/glycerine/rpc25519/tube/art"
)

func Test301_raftStatePersistorSaveLoad(t *testing.T) {
	path := "test301.persistor.tube.msgp"
	os.Remove(path)
	defer os.Remove(path)
	cfg := &TubeConfig{}
	saver, _, err := cfg.NewRaftStatePersistor(path, nil, false)
	panicOn(err)
	var versions []*RaftState

	for i := range int64(3) {
		// save state of varying size, so disk overlap
		// partially with each other.
		cluID := rpc.NewCallID("") // random clusterID
		state := &RaftState{
			CurrentTerm: i,
			Serial:      int64(i + 1),
			ClusterID:   cluID,
			KVstore:     newKVStore(),
		}
		versions = append(versions, state)

		tkt := &Ticket{
			Table:          "hello",
			Key:            "world",
			Val:            Val("43"),
			RaftLogEntryTm: time.Now(),
		}

		state.kvstoreWrite(tkt, &TubeNode{cfg: *cfg})

		// check read back immediately...
		v, _, err := state.KVStoreRead("hello", "world")
		panicOn(err)
		if string(v) != "43" {
			panic(fmt.Sprintf("want '43'; got '%v'", v))
		}
		//vv("good, read back v='%v'", string(v))

		// Since it overwrites each time.
		// only the last should remain at the end, Serial 3
		_, err = saver.save(state)
		panicOn(err)

		// check it immediately, file first file handle still open.
		saver2, st2, err := cfg.NewRaftStatePersistor(path, nil, false)
		panicOn(err)

		//vv("tree sz = len(st2.KVstore.m) = %v", len(st2.KVstore.m))
		//for k, v := range st2.KVstore.m {
		//	vv("st2.KVstore.m: key '%v' -> tree.Size='%v'", k, v.Tree.Size())
		//}

		v2, _, err2 := st2.KVStoreRead("hello", "world")
		panicOn(err2)
		if got, want := string(v2), "43"; got != want {
			panic(fmt.Sprintf("got = '%v'; want = '%v'", got, want))
		}

		if got, want := st2.CurrentTerm, state.CurrentTerm; got != want {
			t.Fatalf("got = %v; want = %v", got, want)
		}
		if got, want := st2.ClusterID, state.ClusterID; got != want {
			t.Fatalf("got = %v; want = %v", got, want)
		}
		if got, want := st2.Serial, state.Serial; got != want {
			t.Fatalf("got = %v; want = %v", got, want)
		}
		saver2.close()
	}
	panicOn(saver.close())

	// the last one should be the one read back...
	saver2, st2, err := cfg.NewRaftStatePersistor(path, nil, false)
	panicOn(err)
	defer saver2.close()

	// expect state, observe state2
	// last one should match, first two should not
	for i, state := range versions {
		if i == len(versions)-1 {
			if got, want := st2.CurrentTerm, state.CurrentTerm; got != want {
				t.Fatalf("got = %v; want = %v", got, want)
			}
			if got, want := st2.ClusterID, state.ClusterID; got != want {
				t.Fatalf("got = %v; want = %v", got, want)
			}
			if got, want := st2.Serial, state.Serial; got != want {
				t.Fatalf("got = %v; want = %v", got, want)
			}
		} else {
			if got, shouldDiffer := st2.CurrentTerm, state.CurrentTerm; got == shouldDiffer {
				t.Fatalf("got = %v; shouldDiffer = %v", got, shouldDiffer)
			}
			if got, shouldDiffer := st2.ClusterID, state.ClusterID; got == shouldDiffer {
				t.Fatalf("got = %v; shouldDiffer = %v", got, shouldDiffer)
			}
			if got, shouldDiffer := st2.Serial, state.Serial; got == shouldDiffer {
				t.Fatalf("got = %v; shouldDiffer = %v", got, shouldDiffer)
			}
		}
	}
}

func Test303_raftStatePersistorCorruptionNoticed(t *testing.T) {
	path := "test303.persistor.tube.msgp"
	os.Remove(path)
	defer os.Remove(path)
	cfg := &TubeConfig{}
	saver, _, err := cfg.NewRaftStatePersistor(path, nil, false)
	panicOn(err)
	for i := range int64(3) {
		state := &RaftState{
			CurrentTerm: 1,
			Serial:      int64(i + 1),
			ClusterID:   rpc.NewCallID(""), // random clusterID
		}
		_, err := saver.save(state)
		panicOn(err)
	}
	panicOn(saver.close())

	// corrupt it
	fd2, err := os.OpenFile(path, os.O_RDWR, 0644)
	panicOn(err)
	_, err = fd2.Write([]byte{0, 0, 0, 0})
	panicOn(err)
	panicOn(fd2.Close())

	func() {
		defer func() {
			r := recover()
			if r == nil {
				t.Fatalf("expected panic on corruption")
			} else {
				//vv("good, caught: '%v'", r)
				// example:
				// good, caught: 'msgp: attempted to decode type "int" with method for "bin"'
			}
		}()
		cfg.NewRaftStatePersistor(path, nil, false) // should panic and be recovered
	}()
}

func Test304_raftStatePersistorChecksumWrongNoticed(t *testing.T) {
	path := "test304.persistor.tube.msgp"
	os.Remove(path)
	defer os.Remove(path)
	cfg := &TubeConfig{}
	saver, _, err := cfg.NewRaftStatePersistor(path, nil, false)
	panicOn(err)
	for i := range int64(3) {
		state := &RaftState{
			CurrentTerm: 1,
			Serial:      int64(i + 1),
			ClusterID:   path,
		}
		_, err := saver.save(state)
		panicOn(err)
	}
	panicOn(saver.close())

	// corrupt it
	by, err := os.ReadFile(path)
	panicOn(err)
	hashPrefix := "blake3.33B-"
	pre := len(hashPrefix)
	pos := bytes.Index(by, []byte(hashPrefix))
	if pos == -1 {
		panic("file should have some DiskState + Blake3 in it")
	}
	copy(by[pos+pre:], []byte("shazam!"))

	fd2, err := os.Create(path)
	panicOn(err)
	_, err = fd2.Write(by)
	panicOn(err)
	panicOn(fd2.Close())

	func() {
		defer func() {
			r := recover()
			if r == nil {
				t.Fatalf("expected panic on corruption")
			} else {
				//vv("good, caught: '%v'", r)
				// example:
				// good, caught: 'corrupt persistor raftstate 'test304.saver' at pos '0'. onDisk.Sum:'blake3.33B-shazam!Ft_lEWT-9DvJZxI9Jc3eJLuRHS7LsoEC00lHi
				// '; vs. re-computed-hash: 'blake3.33B-bpLA52kFt_lEWT-9DvJZxI9Jc3eJLuRHS7LsoEC00lHi
			}
		}()
		cfg.NewRaftStatePersistor(path, nil, false) // should panic and be recovered.
	}()
}
