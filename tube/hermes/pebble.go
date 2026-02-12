package hermes

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/glycerine/rpc25519/tube"
)

func BackupPeebleDatabase(db *pebble.DB, backupRoot string) error {
	// 1. Define where the checkpoint will live locally
	checkpointDir := filepath.Join(backupRoot, "snapshot_"+time.Now().Format(rfc3339NanoNumericTZ0pad))

	// 2. Create the Checkpoint (The Magic Step)
	// This is near-instant. 'db' continues to accept writes safely.
	if err := db.Checkpoint(checkpointDir); err != nil {
		return fmt.Errorf("failed to create checkpoint: %w", err)
	}

	// 3. At this point, 'checkpointDir' is a valid Pebble database.
	// You can tar it up, stream it to S3, etc.
	// Example: uploadToS3(checkpointDir)

	// 4. (Optional) Cleanup the local hard links after upload
	if false {
		os.RemoveAll(checkpointDir)
	}

	return nil
}

func (s *HermesNode) dbDir() string {
	dataDir, err := tube.GetServerDataDir()
	panicOn(err)
	app := "hermes_pebble"
	if s.testName != "" {
		app += "_test"
		app = filepath.Join(app, s.testName)
	}
	return filepath.Join(dataDir, app, s.name)
}

func (s *HermesNode) openDB() {
	if s.memOnly {
		s.storeMap = make(map[Key]*KeyMeta)
		return
	}
	path := s.dbDir()
	db, err := pebble.Open(path, &pebble.Options{})
	panicOn(err)
	s.storeDB = db
	s.storePath = path
}

var pebbleWriteAndFsync = &pebble.WriteOptions{Sync: true}

func (s *HermesNode) writeDB(keym *KeyMeta) {
	if s.memOnly {
		s.storeMap[keym.Key] = keym
		return
	}
	// Write with Durability (Sync = true)

	by, err2 := keym.MarshalMsg(nil)
	panicOn(err2)

	// docs: "Set sets the value for the given key. It overwrites
	// any previous value for that key; a DB is not a multi-map.
	// It is safe to modify the contents of the arguments after Set returns."
	err := s.storeDB.Set([]byte(keym.Key), by, pebbleWriteAndFsync)
	panicOn(err)
}

func (s *HermesNode) readDB(key Key) (keym *KeyMeta, ok bool) {
	if s.memOnly {
		keym, ok = s.storeMap[key]
		return
	}

	// docs: "Get gets the value for the given key. It
	// returns ErrNotFound if the DB does not contain the key.
	// The caller should not modify the contents of the
	// returned slice, but it is safe to modify the contents
	// of the argument after Get returns. The returned slice
	// will remain valid until the returned Closer is closed.
	// On success, the caller MUST call closer.Close() or a
	// memory leak will occur."
	by, closer, err2 := s.storeDB.Get([]byte(key)) // ([]byte, io.Closer, error)
	if err2 == pebble.ErrNotFound {
		return nil, false
	}
	panicOn(err2)

	defer closer.Close()
	keym = &KeyMeta{}
	_, err := keym.UnmarshalMsg(by)
	panicOn(err)
	ok = true
	return
}

func (s *HermesNode) closeDB() (err error) {
	if s.memOnly {
		s.storeMap = nil
		return
	}
	err = s.storeDB.Close()
	panicOn(err)
	return
}

/*
Advanced: Incremental Backups: [note: use jsync to automatically transfer just the diffs.]

If your database is 10TB, you don't want to copy 10TB every day.
Pebble allows for incremental backups, but you have to build the logic yourself:

List Files: Get the list of SSTables in your new Checkpoint.

Compare: Compare this list against the list of SSTables you already have in S3 (by filename/ID).

Upload Delta: Only upload the new SSTables. Since SSTables are immutable, a file with the same name is guaranteed to have the exact same content. You never need to "diff" the inside of a file.
*/
