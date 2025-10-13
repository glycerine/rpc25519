package tube

import (
	bolt "github.com/glycerine/bbolt"
)

func openBolt(dbPath string) (db *bolt.DB, err error) {
	o := bolt.DefaultOptions
	o.FreelistType = bolt.FreelistArrayType
	//o.freelistType = bolt.FreelistMapType

	db, err = bolt.Open(dbPath, 0600, o)
	return
}
