//go:build unix

package jsync

import (
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	rpc "github.com/glycerine/rpc25519"
)

// This exercises the same service used by cmd/jsrv and cmd/jcp. Run with
// sudo go test ./jsync -run TestRootPreservesOwnershipAndPermissions -count=1.
func TestRootPreservesOwnershipAndPermissions(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("requires root to create and preserve another user's ownership")
	}
	testCopyMetadata(t, true)
}

func TestPreservesPermissions(t *testing.T) { testCopyMetadata(t, false) }

func testCopyMetadata(t *testing.T, ownership bool) {
	t.Helper()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}

	// Choose a real non-root source account, since ownership is mapped by name.
	uid, gid := 1, 1
	unknownUID := 65533
	if ownership {
		u, err := user.LookupId("1")
		must(err)
		uid, err = strconv.Atoi(u.Uid)
		must(err)
		gid, err = strconv.Atoi(u.Gid)
		must(err)
	}
	base := t.TempDir()
	src, dst := filepath.Join(base, "src"), filepath.Join(base, "dst")
	must(os.MkdirAll(filepath.Join(src, "nested", "leaf"), 0700))
	must(os.Mkdir(filepath.Join(src, "emptydir"), 0700))
	must(os.WriteFile(filepath.Join(src, "nested", "data"), []byte("ownership regression\n"), 0600))
	must(os.WriteFile(filepath.Join(src, "empty"), nil, 0600))
	must(os.Symlink("nested/data", filepath.Join(src, "link")))
	must(os.Symlink("missing", filepath.Join(src, "dangling")))
	modes := map[string]os.FileMode{
		"": 0751, "nested": 0750, "nested/leaf": 0711, "emptydir": 0750,
		"nested/data": 0754, "empty": 0640,
	}
	// Go represents the special permission bits separately from the Unix bits.
	modes["nested"] = 0750 | os.ModeSetgid
	modes["emptydir"] |= os.ModeSticky
	if ownership {
		modes["nested/data"] |= os.ModeSetuid | os.ModeSetgid
		must(os.WriteFile(filepath.Join(src, "noaccess"), []byte("mode zero"), 0600))
		modes["noaccess"] = 0
		must(os.WriteFile(filepath.Join(src, "rootowned"), []byte("root IDs are valid metadata"), 0600))
		modes["rootowned"] = 0755 | os.ModeSetuid
		for {
			if _, err := user.LookupId(strconv.Itoa(unknownUID)); err != nil {
				break
			}
			unknownUID++
		}
		must(os.WriteFile(filepath.Join(src, "unknownowner"), []byte("unknown source account"), 0600))
		modes["unknownowner"] = 0755 | os.ModeSetuid
	}
	for path, mode := range modes {
		p := filepath.Join(src, path)
		if ownership && path != "rootowned" {
			ownerUID := uid
			if path == "unknownowner" {
				ownerUID = unknownUID
			}
			must(os.Chown(p, ownerUID, gid))
		}
		must(os.Chmod(p, mode))
	}
	if ownership {
		must(os.Lchown(filepath.Join(src, "link"), uid, gid))
		must(os.Lchown(filepath.Join(src, "dangling"), uid, gid))
	}
	cfg := rpc.NewConfig()
	cfg.TCPonly_no_TLS = true
	cfg.ServerAddr = "127.0.0.1:0"
	srv := rpc.NewServer("metadata", cfg)
	addr, err := srv.Start()
	must(err)
	defer srv.Close()
	server, err := NewJsyncServer(srv)
	must(err)
	defer server.Close()
	cfg.ClientDialToHostPort = addr.String()
	cli, err := rpc.NewClient("metadata", cfg)
	must(err)
	must(cli.Start())
	defer cli.Close()
	client, err := NewJsyncClient(cli)
	must(err)
	defer client.Close()
	check := func(dest string) {
		t.Helper()
		paths := []string{"", "nested", "nested/leaf", "emptydir", "nested/data", "empty", "link", "dangling"}
		if ownership {
			paths = append(paths, "noaccess", "rootowned", "unknownowner")
		}
		for _, path := range paths {
			a, err := os.Lstat(filepath.Join(src, path))
			must(err)
			b, err := os.Lstat(filepath.Join(dest, path))
			must(err)
			if a.Mode().IsRegular() {
				x, err := os.ReadFile(filepath.Join(src, path))
				must(err)
				y, err := os.ReadFile(filepath.Join(dest, path))
				must(err)
				if string(x) != string(y) {
					t.Errorf("%s: content mismatch", path)
				}
			}
			mask := os.ModePerm | os.ModeSetuid | os.ModeSetgid | os.ModeSticky
			expectedMode := a.Mode() & mask
			if path == "unknownowner" {
				expectedMode &^= os.ModeSetuid
			}
			if expectedMode != b.Mode()&mask {
				t.Errorf("%s: permissions got %v, want %v", path, b.Mode()&mask, expectedMode)
			}
			if ownership {
				x, y := a.Sys().(*syscall.Stat_t), b.Sys().(*syscall.Stat_t)
				expectedUID := x.Uid
				if path == "unknownowner" {
					expectedUID = 0
				}
				if expectedUID != y.Uid || x.Gid != y.Gid {
					t.Errorf("%s: owner/group got %d:%d, want %d:%d", path, y.Uid, y.Gid, expectedUID, x.Gid)
				}
			}
		}
	}
	_, err = client.PushFromTo(src+string(os.PathSeparator), dst+string(os.PathSeparator))
	must(err)
	check(dst)
	// Correct metadata even when size and mtime already match.
	must(os.Chmod(filepath.Join(dst, "nested/data"), 0600))
	if ownership {
		must(os.Chown(filepath.Join(dst, "nested/data"), 0, 0))
		must(os.Chown(filepath.Join(dst, "rootowned"), uid, gid))
	}
	_, err = client.PushFromTo(src+string(os.PathSeparator), dst+string(os.PathSeparator))
	must(err)
	check(dst)
	// Replacing existing contents must also restore ownership and special bits.
	must(os.WriteFile(filepath.Join(src, "nested/data"), []byte("changed ownership regression contents\n"), 0600))
	must(os.Chmod(filepath.Join(src, "nested/data"), modes["nested/data"]))
	_, err = client.PushFromTo(src+string(os.PathSeparator), dst+string(os.PathSeparator))
	must(err)
	check(dst)
	pulled := filepath.Join(base, "pulled")
	_, err = client.PullToFrom(pulled+string(os.PathSeparator), src+string(os.PathSeparator))
	must(err)
	check(pulled)

	// Standalone file copies exercise the metadata and hash-match shortcuts too.
	singlePaths := []string{"nested/data", "empty"}
	if ownership {
		singlePaths = append(singlePaths, "unknownowner", "rootowned")
	}
	for _, path := range singlePaths {
		source := filepath.Join(src, path)
		dest := filepath.Join(base, filepath.Base(path)+".single")
		checkSingle := func() {
			t.Helper()
			a, err := os.Stat(source)
			must(err)
			b, err := os.Stat(dest)
			must(err)
			mask := os.ModePerm | os.ModeSetuid | os.ModeSetgid
			expectedMode := a.Mode() & mask
			if path == "unknownowner" {
				expectedMode &^= os.ModeSetuid
			}
			if expectedMode != b.Mode()&mask {
				t.Errorf("single %s: mode got %v, want %v", path, b.Mode()&mask, expectedMode)
			}
			if ownership {
				x, y := a.Sys().(*syscall.Stat_t), b.Sys().(*syscall.Stat_t)
				expectedUID := x.Uid
				if path == "unknownowner" {
					expectedUID = 0
				}
				if expectedUID != y.Uid || x.Gid != y.Gid {
					t.Errorf("single %s: owner/group got %d:%d, want %d:%d", path, y.Uid, y.Gid, expectedUID, x.Gid)
				}
			}
		}
		for pass := range 3 {
			if pass > 0 {
				must(os.Chmod(dest, 0600))
				if ownership {
					must(os.Chown(dest, 0, 0))
				}
				if pass == 2 { // Identical bytes but mismatching mtime.
					must(os.Chtimes(dest, time.Time{}, time.Unix(1234567890, 0)))
				}
			}
			_, err = client.PushFromTo(source, dest)
			must(err)
			checkSingle()
			// Pull to an existing destination to exercise both matching-data paths.
			must(os.Chmod(dest, 0600))
			if ownership {
				must(os.Chown(dest, 0, 0))
			}
			if pass == 2 {
				must(os.Chtimes(dest, time.Time{}, time.Unix(1234567890, 0)))
			}
			_, err = client.PullToFrom(dest, source)
			must(err)

			checkSingle()
		}
	}
}

func TestNonRootReceiverSkipsOwnership(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("requires an unprivileged receiver")
	}
	path := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(path, []byte("test"), 0600); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	// An unprivileged receiver must not even attempt to become root.
	if err := restoreMetadata(path, 0751, true, 0, 0, nil); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	a, b := before.Sys().(*syscall.Stat_t), after.Sys().(*syscall.Stat_t)
	if a.Uid != b.Uid || a.Gid != b.Gid {
		t.Fatal("unprivileged receiver changed ownership")
	}
	if after.Mode().Perm() != 0751 {
		t.Fatalf("permissions got %v, want 0751", after.Mode())
	}
}

func TestOwnershipMapsUsernamesAcrossHosts(t *testing.T) {
	source := newAccountDB(map[int64]string{501: "jaten"}, nil)
	destination := newAccountDB(map[int64]string{1001: "jaten"}, nil)
	if uid, known := destination.destinationUID(source.users[501]); uid != 1001 || !known {
		t.Fatalf("destination UID = %d (%v), want 1001", uid, known)
	}
	for _, name := range []string{"", "missing-user", source.users[12345]} {
		if uid, known := destination.destinationUID(name); uid != 0 || known {
			t.Fatalf("unknown user mapped to %d (%v)", uid, known)
		}
	}
}

func TestRootMissingDestinationUserFallsBackToRoot(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("requires a root receiver")
	}
	path := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(path, []byte("test"), 0600); err != nil {
		t.Fatal(err)
	}
	// Empty source names and unknown destination names must both use root,
	// even if the source numeric UID happens to be an existing local account.
	for _, name := range []string{"", "jsync-no-such-user-" + strconv.Itoa(os.Getpid())} {
		if err := os.Chown(path, 1, 1); err != nil {
			t.Fatal(err)
		}
		if err := restoreMetadata(path, uint32(0751|os.ModeSetuid), true, 1, 1, &OwnershipAccounts{Users: map[int64]string{1: name}}); err != nil {
			t.Fatal(err)
		}
		fi, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if uid := fi.Sys().(*syscall.Stat_t).Uid; uid != 0 {
			t.Fatalf("unknown username %q: UID %d, want root", name, uid)
		}
		if fi.Mode()&os.ModeSetuid != 0 {
			t.Fatalf("unknown username %q: fallback retained setuid", name)
		}
		if fi.Mode().Perm() != 0751 {
			t.Fatalf("fallback changed ordinary permissions: %v", fi.Mode())
		}
	}
}

func TestNumericOwnershipMetadataDoesNotAllocate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	var conveyed bool
	var uid, gid uint32
	allocations := testing.AllocsPerRun(1000, func() { conveyed, uid, gid = fileOwnership(fi) })
	if allocations != 0 {
		t.Fatalf("numeric metadata allocated %v objects per file", allocations)
	}
	st := fi.Sys().(*syscall.Stat_t)
	if !conveyed || uid != st.Uid || gid != st.Gid {
		t.Fatalf("numeric metadata mismatch: %v %d:%d", conveyed, uid, gid)
	}
}
