package jsync

import (
	"bufio"
	"io"
	"strconv"
	"strings"
	"sync"
)

// Immutable process-wide snapshots of the two public account files. Reading
// these once avoids NSS/directory-service requests and per-file name lookups.
var localAccounts = sync.OnceValues(loadLocalAccountDB)

type accountDB struct {
	users  map[int64]string
	groups map[int64]string
	uids   map[string]uint32
	gids   map[string]uint32
}

func newAccountDB(users, groups map[int64]string) *accountDB {
	db := &accountDB{users: users, groups: groups, uids: make(map[string]uint32), gids: make(map[string]uint32)}
	for id, name := range users {
		if name != "" {
			db.uids[name] = uint32(id)
		}
	}
	for id, name := range groups {
		if name != "" {
			db.gids[name] = uint32(id)
		}
	}
	return db
}

func (db *accountDB) destinationUID(name string) (int, bool) {
	id, ok := db.uids[name]
	return int(id), ok
}

func (db *accountDB) destinationGID(name string) (int, bool) {
	id, ok := db.gids[name]
	return int(id), ok
}

// passwd and group both put the numeric ID in column 3. Signed IDs occur in
// BSD account files; -1 is excluded because chown interprets it as no change.
func parseAccountMap(r io.Reader, fields int) (map[int64]string, error) {
	names := make(map[int64]string)
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 4096), 1<<20)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		cols := strings.Split(line, ":")
		if len(cols) < fields || cols[0] == "" {
			continue
		}
		n, err := strconv.ParseInt(cols[2], 10, 64)
		if err != nil || n < -2147483648 || n >= 4294967295 || n == -1 {
			continue
		}
		id := int64(uint32(n))
		if _, ok := names[id]; !ok {
			names[id] = cols[0]
		}
	}
	return names, scanner.Err()
}
