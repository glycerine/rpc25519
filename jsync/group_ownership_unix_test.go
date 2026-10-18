//go:build unix

package jsync

import (
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	rpc "github.com/glycerine/rpc25519"
)

// Run as root. The source account database assigns a different GID to a
// group that exists on the receiver. Only the source lookup is simulated;
// the transfer, receiver, filesystem ownership, and destination group are real.
func TestRootTransferPreservesGroupNameAcrossDifferentGIDs(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("requires root to assign and restore file groups")
	}
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}

	destinationGroup, err := user.LookupGroupId(strconv.Itoa(os.Getegid()))
	must(err)
	destinationGID, err := strconv.Atoi(destinationGroup.Gid)
	must(err)
	sourceGID := 60001
	if sourceGID == destinationGID {
		sourceGID++
	}
	records, err := localAccounts()
	must(err)
	sourceGroups := make(map[int64]string, len(records.groups)+1)
	for id, name := range records.groups {
		sourceGroups[id] = name
	}
	sourceGroups[int64(sourceGID)] = destinationGroup.Name
	// Exceed a single account-table batch. The relevant ID may occur in any
	// batch, but the complete table must arrive before file metadata is used.
	for i := range 700 {
		sourceGroups[int64(100000+i)] = fmt.Sprintf("unused-group-%d", i)
	}
	sourceRecords := newAccountDB(records.users, sourceGroups)

	base := t.TempDir()
	// Force new destinations to inherit the source numeric GID, so simply
	// omitting group restoration cannot accidentally satisfy the assertion.
	must(os.Chown(base, -1, sourceGID))
	must(os.Chmod(base, 0700|os.ModeSetgid))
	source := filepath.Join(base, "source")
	destination := filepath.Join(base, "destination")
	must(os.MkdirAll(filepath.Join(source, "nested"), 0700))
	must(os.WriteFile(filepath.Join(source, "nested", "data"), []byte("group name preservation\n"), 0640))
	must(os.WriteFile(filepath.Join(source, "empty"), nil, 0640))
	must(os.Symlink("nested/data", filepath.Join(source, "link")))
	paths := []string{"", "nested", "nested/data", "empty", "link"}
	for _, path := range paths {
		must(os.Lchown(filepath.Join(source, path), 0, sourceGID))
	}
	cfg := rpc.NewConfig()
	cfg.TCPonly_no_TLS = true
	cfg.ServerAddr = "127.0.0.1:0"
	srv := rpc.NewServer("group_metadata", cfg)
	addr, err := srv.Start()
	must(err)
	defer srv.Close()
	server, err := NewJsyncServer(srv)
	must(err)
	defer server.Close()
	cfg.ClientDialToHostPort = addr.String()
	cli, err := rpc.NewClient("group_metadata", cfg)
	must(err)
	must(cli.Start())
	defer cli.Close()
	client, err := NewJsyncClient(cli)
	must(err)
	defer client.Close()
	var sourceLoads atomic.Int32
	client.rsyncd.sourceAccounts = func() (*accountDB, error) { sourceLoads.Add(1); return sourceRecords, nil }
	server.rsyncd.sourceAccounts = func() (*accountDB, error) { return sourceRecords, nil }

	checkGroup := func(path string) {
		t.Helper()
		fi, err := os.Lstat(path)
		must(err)
		got := fi.Sys().(*syscall.Stat_t).Gid
		if got != uint32(destinationGID) {
			t.Errorf("%s: destination GID = %d; want %d for group %q (source GID %d)", path, got, destinationGID, destinationGroup.Name, sourceGID)
		}
	}

	// Directory listings and standalone file summaries use different metadata
	// paths. Both must resolve the source group name on the root receiver.
	_, err = client.PushFromTo(source+string(os.PathSeparator), destination+string(os.PathSeparator))
	must(err)
	for _, path := range paths {
		checkGroup(filepath.Join(destination, path))
	}
	if n := sourceLoads.Load(); n != 1 {
		t.Fatalf("directory transfer loaded source account maps %d times; want once for all file circuits", n)
	}
	// Metadata-only copies must resolve groups even when the bytes match.
	must(os.Chown(filepath.Join(destination, "nested/data"), -1, sourceGID))
	_, err = client.PushFromTo(source+string(os.PathSeparator), destination+string(os.PathSeparator))
	must(err)
	checkGroup(filepath.Join(destination, "nested/data"))
	pulled := filepath.Join(base, "pulled")
	_, err = client.PullToFrom(pulled+string(os.PathSeparator), source+string(os.PathSeparator))
	must(err)
	for _, path := range paths {
		checkGroup(filepath.Join(pulled, path))
	}

	single := filepath.Join(base, "single")
	_, err = client.PushFromTo(filepath.Join(source, "nested", "data"), single)
	must(err)
	checkGroup(single)
	for pass := range 2 {
		must(os.Chown(single, -1, sourceGID))
		if pass == 1 {
			must(os.Chtimes(single, time.Time{}, time.Unix(1234567890, 0)))
		}
		_, err = client.PushFromTo(filepath.Join(source, "nested/data"), single)
		must(err)
		checkGroup(single)
		must(os.Chown(single, -1, sourceGID))
		if pass == 1 {
			must(os.Chtimes(single, time.Time{}, time.Unix(1234567890, 0)))
		}
		_, err = client.PullToFrom(single, filepath.Join(source, "nested/data"))
		must(err)
		checkGroup(single)
	}
}

func TestOwnershipMapsGroupNamesAcrossHosts(t *testing.T) {
	source := newAccountDB(nil, map[int64]string{20: "staff"})
	destination := newAccountDB(nil, map[int64]string{1001: "staff"})
	if gid, known := destination.destinationGID(source.groups[20]); gid != 1001 || !known {
		t.Fatalf("destination GID = %d (%v), want 1001", gid, known)
	}
	for _, name := range []string{"", "missing-group", source.groups[12345]} {
		if gid, known := destination.destinationGID(name); gid != 0 || known {
			t.Fatalf("unknown group mapped to %d (%v)", gid, known)
		}
	}
}

func TestRootMissingDestinationGroupClearsSetgid(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("requires a root receiver")
	}
	path := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(path, []byte("test"), 0600); err != nil {
		t.Fatal(err)
	}
	root, err := user.LookupId("0")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"", "jsync-no-such-group-" + strconv.Itoa(os.Getpid())} {
		if err := os.Chown(path, 0, 1); err != nil {
			t.Fatal(err)
		}
		// A known root owner keeps its legitimate setuid bit; group fallback
		// must clear setgid without losing the remaining permission bits.
		mode := os.FileMode(0751) | os.ModeSetgid | os.ModeSetuid
		accounts := &OwnershipAccounts{Users: map[int64]string{0: root.Username}, Groups: map[int64]string{1: name}}
		if err := restoreMetadata(path, uint32(mode), true, 0, 1, accounts); err != nil {
			t.Fatal(err)
		}
		fi, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if gid := fi.Sys().(*syscall.Stat_t).Gid; gid != 0 {
			t.Fatalf("unknown group %q: GID %d, want 0", name, gid)
		}
		if fi.Mode()&os.ModeSetgid != 0 {
			t.Fatalf("unknown group %q retained setgid", name)
		}
		if fi.Mode()&(os.ModePerm|os.ModeSetuid) != mode&^os.ModeSetgid {
			t.Fatalf("unexpected permissions: %v", fi.Mode())
		}
	}
}
