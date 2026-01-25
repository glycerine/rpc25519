package tube

import (
	bolt "github.com/glycerine/bbolt"
)

// interesting experiment but might be slower.

const sessBucket = "sessBucket"

type sessionsDB struct {
	db *bolt.DB
}

func openSessDB(dbPath string) (sdb *sessionsDB, err0 error) {
	o := bolt.DefaultOptions
	o.FreelistType = bolt.FreelistArrayType
	//o.freelistType = bolt.FreelistMapType

	db, err := bolt.Open(dbPath, 0600, o)
	if err != nil {
		return nil, err
	}
	sdb = &sessionsDB{
		db: db,
	}

	err0 = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(sessBucket))
		return err
	})
	panicOn(err0)
	return
}

func (s *sessionsDB) Close() error {
	return s.db.Close()
}

func (s *sessionsDB) saveSTE(ste *SessionTableEntry) (err0 error) {

	bts, err := ste.MarshalMsg(nil)
	panicOn(err)

	err0 = s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(sessBucket))
		if b == nil {
			panicf("why did sessBucket not get created at startup?")
		}

		if err := b.Put([]byte(ste.SessionID), bts); err != nil {
			return err
		}

		return nil
	})
	panicOn(err0)
	return
}

func (s *sessionsDB) loadSTE(sessionID string) (ste *SessionTableEntry, err0 error) {

	err0 = s.db.View(func(tx *bolt.Tx) (err error) {
		b := tx.Bucket([]byte(sessBucket))
		if b == nil {
			panicf("why did sessBucket not get created at startup?")
		}

		v := b.Get([]byte(sessionID))
		if len(v) == 0 {
			return ErrKeyNotFound
		}
		ste = &SessionTableEntry{}
		_, err = ste.UnmarshalMsg(v)
		return
	})
	panicOn(err0)
	return
}

func (s *sessionsDB) deleteSTE(sessionID string) (err0 error) {

	err0 = s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(sessBucket))
		if b == nil {
			panicf("why did sessBucket not get created at startup?")
		}
		return b.Delete([]byte(sessionID))
	})
	panicOn(err0)
	return
}

func (s *sessionsDB) batchWriteSessTable(sessTable map[string]*SessionTableEntry) (err0 error) {

	err0 = s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(sessBucket))
		if b == nil {
			panicf("why did sessBucket not get created at startup?")
		}

		for id, ste := range sessTable {
			bts, err := ste.MarshalMsg(nil)
			panicOn(err)

			err = b.Put([]byte(id), bts)
			if err != nil {
				return err
			}
		}
		return nil
	})
	panicOn(err0)
	return
}

func (s *sessionsDB) loadSessTable() (sessTable map[string]*SessionTableEntry, err0 error) {

	err0 = s.db.View(func(tx *bolt.Tx) (err error) {
		b := tx.Bucket([]byte(sessBucket))
		if b == nil {
			panicf("why did sessBucket not get created at startup?")
		}

		sessTable = make(map[string]*SessionTableEntry)
		c := b.Cursor()

		for k, v := c.First(); k != nil; k, v = c.Next() {

			ste := &SessionTableEntry{}
			_, err = ste.UnmarshalMsg(v)
			panicOn(err)
			sessTable[string(k)] = ste
		}
		return
	})
	panicOn(err0)
	return
}
