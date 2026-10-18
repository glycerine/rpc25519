package jsync

import (
	"bytes"
	"reflect"
	"strings"
	"testing"

	"github.com/glycerine/greenpack/msgp"
)

func TestOwnershipAccountFilesAndWireRoundTrip(t *testing.T) {
	users, err := parseAccountMap(strings.NewReader(`# public passwd records
root:*:0:0:root:/root:/bin/sh
jaten:*:501:20:Jaten:/home/jaten:/bin/sh
nobody:*:-2:-2:nobody:/:/bin/false
invalid:*:not-a-number:0:x:/:/bin/false
`), 7)
	if err != nil {
		t.Fatal(err)
	}
	groups, err := parseAccountMap(strings.NewReader(`# public group records
wheel:*:0:root
staff:*:20:jaten
nogroup:*:-1:
nobody:*:-2:
invalid
`), 4)
	if err != nil {
		t.Fatal(err)
	}
	wantUsers := map[int64]string{0: "root", 501: "jaten", 4294967294: "nobody"}
	wantGroups := map[int64]string{0: "wheel", 20: "staff", 4294967294: "nobody"}
	if !reflect.DeepEqual(users, wantUsers) || !reflect.DeepEqual(groups, wantGroups) {
		t.Fatalf("parsed accounts: users %v, groups %v", users, groups)
	}
	if _, ok := groups[4294967295]; ok {
		t.Fatal("chown's -1 sentinel was included")
	}
	want := &OwnershipAccounts{Users: users, Groups: groups, Last: true}
	data, err := want.MarshalMsg(nil)
	if err != nil {
		t.Fatal(err)
	}
	var got OwnershipAccounts
	rest, err := got.UnmarshalMsg(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(rest) != 0 || !reflect.DeepEqual(want, &got) {
		t.Fatalf("account map wire round trip: got %#v", got)
	}
	var stream bytes.Buffer
	if err := msgp.Encode(&stream, want); err != nil {
		t.Fatal(err)
	}
	var decoded OwnershipAccounts
	if err := msgp.Decode(&stream, &decoded); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(want, &decoded) {
		t.Fatalf("streamed account map round trip: got %#v", decoded)
	}
}

func TestOwnershipFieldsOmittedForUnprivilegedTaker(t *testing.T) {
	s := NewSyncService(nil)
	s.sourceAccounts = func() (*accountDB, error) {
		t.Fatal("unprivileged taker caused account files to be loaded")
		return nil, nil
	}
	tr, err := s.collectOwnership(false)
	if err != nil {
		t.Fatal(err)
	}
	if tr.accounts != nil {
		t.Fatal("unprivileged transfer has account maps")
	}
	base := &File{Path: "data", FileMode: 0640}
	expected, err := base.MarshalMsg(nil)
	if err != nil {
		t.Fatal(err)
	}
	file := &File{Path: "data", FileMode: 0640, OwnershipConveyed: true, UID: 501, GID: 20}
	s.limitFileOwnership(file, 12345)
	actual, err := file.MarshalMsg(nil)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(expected, actual) {
		t.Fatalf("unprivileged ownership consumes wire bytes: %x vs %x", actual, expected)
	}
	for _, name := range []string{"OwnershipConveyed", "UID_zid", "GID_zid"} {
		if bytes.Contains(actual, []byte(name)) {
			t.Fatalf("zero ownership field %q present on wire", name)
		}
	}
	// Standalone summaries and requests obey the same omission rule.
	precis := &FilePrecis{OwnershipConveyed: true, UID: 501, GID: 20}
	s.limitPrecisOwnership(precis, 12345)
	data, err := precis.MarshalMsg(nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"OwnershipConveyed", "UID_zid", "GID_zid"} {
		if bytes.Contains(data, []byte(name)) {
			t.Fatalf("summary ownership field %q present for unprivileged taker", name)
		}
	}
	req := &RequestToSyncPath{OwnershipConveyed: true, UID: 501, GID: 20}
	s.limitRequestOwnership(req)
	data, err = req.MarshalMsg(nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"OwnershipConveyed", "UID_zid", "GID_zid"} {
		if bytes.Contains(data, []byte(name)) {
			t.Fatalf("request ownership field %q present for unprivileged taker", name)
		}
	}

	// UID/GID zero must still represent real root ownership when conveyed.
	root := &File{Path: "data", OwnershipConveyed: true}
	data, err = root.MarshalMsg(nil)
	if err != nil {
		t.Fatal(err)
	}
	var decoded File
	if _, err = decoded.UnmarshalMsg(data); err != nil {
		t.Fatal(err)
	}
	if !decoded.OwnershipConveyed || decoded.UID != 0 || decoded.GID != 0 {
		t.Fatalf("zero-ID ownership lost: %#v", decoded)
	}
}
