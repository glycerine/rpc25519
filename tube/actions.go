package tube

import (
	//"bytes"
	//"errors"
	"fmt"
	"iter"
	//"net"
	//"sort"
	//"strings"

	//"io"
	//"os"
	"context"
	//"net/url"
	//"math"
	//"sort"
	//"sync"
	//cryrand "crypto/rand"
	//"path/filepath"
	//"sync"
	//"sync/atomic"
	"time"

	//rb "github.com/glycerine/rbtree"
	//"github.com/glycerine/greenpack/msgp"
	//"github.com/glycerine/blake3"
	//"github.com/glycerine/idem"
	"github.com/glycerine/rpc25519/tube/art"
)

// The 9 fsm actions here, on both TubeNode and Session:
// Read, Write, CAS,
// ReadKeyRange, DeleteKey, ShowKeys,
// MakeTable, DeleteTable, RenameTable.

// Write a new value under key, or update key's existing
// value to val if key already exists.
//
// Setting waitForDur = 0 means the Write will
// wait indefinitely for the write to complete, and
// is a reasonable default. This provides strong consistency
// (linearizability) from all live replicas.
//
// Key versioning is used to make Write's action
// linearizable over a global total
// order of writes and reads to all live replicas. Any node can
// issue a Write, and any node can issue a Read, and
// both are linearizable with respect to each other.
// This provides the strongest and most intuitive
// consistency. It also gives us composability --
// a synonym for ease of use.
func (s *TubeNode) Write(ctx context.Context, table, key Key, val Val, waitForDur time.Duration, sess *Session) (tkt *Ticket, err error) {

	desc := fmt.Sprintf("write: key(%v) = val(%v)", key, showExternalCluster(val))
	tkt = s.NewTicket(desc, table, key, val, s.PeerID, s.name, WRITE, waitForDur, ctx)
	if sess != nil {
		tkt.SessionID = sess.SessionID
		tkt.SessionSerial = sess.SessionSerial
		tkt.MinSessSerialWaiting = sess.MinSessSerialWaiting
		tkt.SessionLastKnownIndex = sess.LastKnownIndex
		//vv("Write set tkt.SessionSerial = %v", tkt.SessionSerial)
	}

	select {
	case s.writeReqCh <- tkt:
		// proceed to wait below for txt.done
	case <-ctx.Done():
		err = ctx.Err()
		return
	case <-s.Halt.ReqStop.Chan:
		err = ErrTimeOut
		return
	}

	select {
	case <-tkt.Done.Chan:
		err = tkt.Err
		if sess != nil && tkt.AsOfLogIndex > sess.LastKnownIndex {
			sess.LastKnownIndex = tkt.AsOfLogIndex
		}
		if sess != nil && err == nil {
			if tkt.SessionSerial > sess.MinSessSerialWaiting {
				sess.MinSessSerialWaiting = tkt.SessionSerial
			}
		}
		//vv("Write set err = '%v'", err)
		return
	case <-ctx.Done():
		err = ctx.Err()
		return
	case <-s.Halt.ReqStop.Chan:
		err = ErrShutDown
		return
	}
}

// Compare and Swap
func (s *TubeNode) CAS(ctx context.Context, table, key Key, oldval, newval Val, waitForDur time.Duration, sess *Session) (tkt *Ticket, err error) {

	desc := fmt.Sprintf("cas(table(%v), key(%v), if oldval(%v) -> newval(%v)", table, key, string(oldval), string(newval))

	tkt = s.NewTicket(desc, table, key, newval, s.PeerID, s.name, CAS, waitForDur, ctx)
	tkt.OldVal = oldval

	if sess != nil {
		tkt.SessionID = sess.SessionID
		tkt.SessionSerial = sess.SessionSerial
		tkt.MinSessSerialWaiting = sess.MinSessSerialWaiting
		tkt.SessionLastKnownIndex = sess.LastKnownIndex
		//vv("Write set tkt.SessionSerial = %v", tkt.SessionSerial)
	}

	select {
	case s.writeReqCh <- tkt:
		// proceed to wait below for txt.done
	case <-ctx.Done():
		err = ctx.Err()
		return
	case <-s.Halt.ReqStop.Chan:
		err = ErrTimeOut
		return
	}

	select {
	case <-tkt.Done.Chan:
		err = tkt.Err
		if sess != nil && tkt.AsOfLogIndex > sess.LastKnownIndex {
			sess.LastKnownIndex = tkt.AsOfLogIndex
		}
		if sess != nil && err == nil {
			if tkt.SessionSerial > sess.MinSessSerialWaiting {
				sess.MinSessSerialWaiting = tkt.SessionSerial
			}
		}
		//vv("CAS set err = '%v'", err)
		return
	case <-ctx.Done():
		err = ctx.Err()
		return
	case <-s.Halt.ReqStop.Chan:
		err = ErrShutDown
		return
	}
}

// Read a key's value. Setting waitForDur = 0
// means the Read will wait indefinitely for a valid key.
//
// For production use, you may want a non-zero waitForDur
// timeout, if you are able to (and/or don't want to) wait
// for some tail events like the replica recovery
// protocol to finish.
//
// A waitForDur of -1 means try locally, but return
// ErrNotFound quickly if there is nothing here.
//
// The default waitForDur of 0 avoids
// races with writers, and still provides the fastest
// possible local read if the key is valid (not
// being written at the moment).
//
// Read only ever returns a linearizable, replicated value
// when the returned error is nil. Non-nil errors can
// include ErrTimeOut, ErrShutDown, and ErrNotFound, in
// which case val will be undefined but typically nil.
func (s *TubeNode) Read(ctx context.Context, table, key Key, waitForDur time.Duration, sess *Session) (tkt *Ticket, err error) {

	desc := fmt.Sprintf("read key(%v)", key)
	tkt = s.NewTicket(desc, table, key, nil, s.PeerID, s.name, READ, waitForDur, ctx)
	if sess != nil {
		tkt.SessionID = sess.SessionID
		tkt.SessionSerial = sess.SessionSerial
		tkt.MinSessSerialWaiting = sess.MinSessSerialWaiting
		tkt.SessionLastKnownIndex = sess.LastKnownIndex
	}
	select {
	case s.readReqCh <- tkt:
		// proceed to wait below for txt.Done
	case <-ctx.Done():
		err = ctx.Err()
		return
	case <-s.Halt.ReqStop.Chan:
		tkt = nil
		err = ErrShutDown // ErrTimeOut
		return
	}

	select {
	case <-tkt.Done.Chan: // Read() waits for completion
		err = tkt.Err
		if sess != nil && tkt.AsOfLogIndex > sess.LastKnownIndex {
			sess.LastKnownIndex = tkt.AsOfLogIndex
		}
		if sess != nil && err == nil {
			if tkt.SessionSerial > sess.MinSessSerialWaiting {
				sess.MinSessSerialWaiting = tkt.SessionSerial
			}
		}
		return
	case <-ctx.Done():
		err = ctx.Err()
		return
	case <-s.Halt.ReqStop.Chan:
		tkt = nil
		err = ErrShutDown
		return
	}
}

func (s *TubeNode) ReadKeyRange(ctx context.Context, table, key, keyEndx Key, descend bool, waitForDur time.Duration, sess *Session) (tkt *Ticket, err error) {

	desc := fmt.Sprintf("read key range [%v, %v) from table '%v' (descend:%v)", key, keyEndx, table, descend)
	tkt = s.NewTicket(desc, table, key, nil, s.PeerID, s.name, READ_KEYRANGE, waitForDur, ctx)
	tkt.KeyEndx = keyEndx
	tkt.ScanDescend = descend
	if sess != nil {
		tkt.SessionID = sess.SessionID
		tkt.SessionSerial = sess.SessionSerial
		tkt.MinSessSerialWaiting = sess.MinSessSerialWaiting
		tkt.SessionLastKnownIndex = sess.LastKnownIndex
	}
	select {
	case s.readReqCh <- tkt:
		// proceed to wait below for txt.Done
	case <-ctx.Done():
		err = ctx.Err()
		return
	case <-s.Halt.ReqStop.Chan:
		tkt = nil
		err = ErrShutDown // ErrTimeOut
		return
	}

	select {
	case <-tkt.Done.Chan: // Read() waits for completion
		err = tkt.Err
		if sess != nil && tkt.AsOfLogIndex > sess.LastKnownIndex {
			sess.LastKnownIndex = tkt.AsOfLogIndex
		}
		if sess != nil && err == nil {
			if tkt.SessionSerial > sess.MinSessSerialWaiting {
				sess.MinSessSerialWaiting = tkt.SessionSerial
			}
		}
		return
	case <-ctx.Done():
		err = ctx.Err()
		return
	case <-s.Halt.ReqStop.Chan:
		tkt = nil
		err = ErrShutDown
		return
	}
}

func (s *TubeNode) DeleteKey(ctx context.Context, table, key Key, waitForDur time.Duration, sess *Session) (tkt *Ticket, err error) {

	desc := fmt.Sprintf("delete key(%v)", key)
	tkt = s.NewTicket(desc, table, key, nil, s.PeerID, s.name, DELETE_KEY, waitForDur, ctx)
	if sess != nil {
		tkt.SessionID = sess.SessionID
		tkt.SessionSerial = sess.SessionSerial
		tkt.MinSessSerialWaiting = sess.MinSessSerialWaiting
		tkt.SessionLastKnownIndex = sess.LastKnownIndex
	}
	select {
	case s.deleteKeyReqCh <- tkt:
		// proceed to wait below for txt.Done
	case <-ctx.Done():
		err = ctx.Err()
		return
	case <-s.Halt.ReqStop.Chan:
		tkt = nil
		err = ErrShutDown // ErrTimeOut
		return
	}

	select {
	case <-tkt.Done.Chan: // DeleteKey() waits for completion
		err = tkt.Err
		if sess != nil && tkt.AsOfLogIndex > sess.LastKnownIndex {
			sess.LastKnownIndex = tkt.AsOfLogIndex
		}
		if sess != nil && err == nil {
			if tkt.SessionSerial > sess.MinSessSerialWaiting {
				sess.MinSessSerialWaiting = tkt.SessionSerial
			}
		}
		return
	case <-ctx.Done():
		err = ctx.Err()
		return
	case <-s.Halt.ReqStop.Chan:
		tkt = nil
		err = ErrShutDown
		return
	}
}

// empty table means list all the tables
func (s *TubeNode) ShowKeys(ctx context.Context, table Key, waitForDur time.Duration, sess *Session) (tkt *Ticket, err error) {

	desc := fmt.Sprintf("show (%v)", table)
	tkt = s.NewTicket(desc, table, "", nil, s.PeerID, s.name, SHOW_KEYS, waitForDur, ctx)
	if sess != nil {
		tkt.SessionID = sess.SessionID
		tkt.SessionSerial = sess.SessionSerial
		tkt.MinSessSerialWaiting = sess.MinSessSerialWaiting
		tkt.SessionLastKnownIndex = sess.LastKnownIndex
	}
	select {
	// SHOW_KEYS, READ_KEYRANGE, and READ use readReqCh.
	case s.readReqCh <- tkt:
		// proceed to wait below for txt.Done
	case <-ctx.Done():
		err = ctx.Err()
		return
	case <-s.Halt.ReqStop.Chan:
		tkt = nil
		err = ErrShutDown // ErrTimeOut
		return
	}

	select {
	case <-tkt.Done.Chan: // ShowKeys() waits for completion
		err = tkt.Err
		if sess != nil && tkt.AsOfLogIndex > sess.LastKnownIndex {
			sess.LastKnownIndex = tkt.AsOfLogIndex
		}
		if sess != nil && err == nil {
			if tkt.SessionSerial > sess.MinSessSerialWaiting {
				sess.MinSessSerialWaiting = tkt.SessionSerial
			}
		}
		return
	case <-ctx.Done():
		err = ctx.Err()
		return
	case <-s.Halt.ReqStop.Chan:
		tkt = nil
		err = ErrShutDown
		return
	}
}

func (s *TubeNode) DeleteTable(ctx context.Context, table Key, waitForDur time.Duration, sess *Session) (tkt *Ticket, err error) {

	desc := fmt.Sprintf("delete table '%v'", table)
	tkt = s.NewTicket(desc, table, "", nil, s.PeerID, s.name, DELETE_TABLE, waitForDur, ctx)

	if sess != nil {
		tkt.SessionID = sess.SessionID
		tkt.SessionSerial = sess.SessionSerial
		tkt.MinSessSerialWaiting = sess.MinSessSerialWaiting
		tkt.SessionLastKnownIndex = sess.LastKnownIndex
	}
	select {
	case s.writeReqCh <- tkt:
		// proceed to wait below for txt.Done
	case <-ctx.Done():
		err = ctx.Err()
		return
	case <-s.Halt.ReqStop.Chan:
		tkt = nil
		err = ErrShutDown // ErrTimeOut
		return
	}

	select {
	case <-tkt.Done.Chan: // DeleteTable waits for completion
		err = tkt.Err
		if sess != nil && tkt.AsOfLogIndex > sess.LastKnownIndex {
			sess.LastKnownIndex = tkt.AsOfLogIndex
		}
		if sess != nil && err == nil {
			if tkt.SessionSerial > sess.MinSessSerialWaiting {
				sess.MinSessSerialWaiting = tkt.SessionSerial
			}
		}
		return
	case <-ctx.Done():
		err = ctx.Err()
		return
	case <-s.Halt.ReqStop.Chan:
		tkt = nil
		err = ErrShutDown
		return
	}
}

func (s *RaftState) kvstoreWrite(tktTable, tktKey Key, tktVal []byte) {
	if s.KVstore == nil {
		s.KVstore = newKVStore()
	}
	if s.KVstore.m == nil {
		s.KVstore.m = make(map[Key]*ArtTable)
	}
	table, ok := s.KVstore.m[tktTable]
	if !ok {
		table = newArtTable()
		s.KVstore.m[tktTable] = table
	}
	table.Tree.Insert(art.Key(tktKey), art.ByteSliceValue([]byte(append([]byte{}, tktVal...))))
	//vv("%v wrote key '%v' ; KVstore now len=%v", s.name, tktKey, s.KVstore.Len())
}

func (s *RaftState) kvstoreRangeScan(tktTable, tktKey, tktKeyEndx Key, descend bool) (results *art.Tree, err error) {
	table, ok := s.KVstore.m[tktTable]
	if !ok {
		return nil, ErrKeyNotFound
	}
	//vv("%v kvstoreRangeScan table='%v', key='%v', keyEndx='%v'; descend=%v", s.name, tktTable, tktKey, tktKeyEndx, descend)
	results = art.NewArtTree()
	if descend {
		// note this is correct, the endx comes first in art.Descend.
		for key, lf := range art.Descend(table.Tree, art.Key(tktKeyEndx), art.Key(tktKey)) {
			//vv("Descend sees key '%v' -> lf.Value: '%v'", string(key), string(lf.Value))
			// make copies.
			key2 := append([]byte{}, key...)
			val2 := append([]byte{}, lf.Value...)
			results.Insert(key2, val2)
		}
	} else {
		for key, lf := range art.Ascend(table.Tree, art.Key(tktKey), art.Key(tktKeyEndx)) {
			//vv("Ascend sees key '%v' -> lf.Value: '%v'", string(key), string(lf.Value))
			// make copies.
			key2 := append([]byte{}, key...)
			val2 := append([]byte{}, lf.Value...)
			results.Insert(key2, val2)
		}
	}
	return
}

func (s *RaftState) KVStoreRead(tktTable, tktKey Key) ([]byte, error) {
	table, ok := s.KVstore.m[tktTable]
	if !ok {
		return nil, ErrKeyNotFound
	}
	val, idx, ok := table.Tree.FindExact(art.Key(tktKey))
	_ = idx
	if ok {
		return val, nil
	}
	return nil, ErrKeyNotFound
}

func (s *KVStore) Len() int {
	return len(s.m)
}
func (s *KVStore) All() iter.Seq2[Key, *ArtTable] {
	return func(yield func(Key, *ArtTable) bool) {
		for k, v := range s.m {
			if !yield(k, v) {
				return
			}
		}
	}
}

func (s *ArtTable) Len() int {
	return s.Tree.Size()
}
func (s *ArtTable) All() iter.Seq2[Key, Val] {
	return func(yield func(Key, Val) bool) {
		iter := s.Tree.Iter(nil, nil)
		for iter.Next() {
			if !yield(Key(iter.Key()), Val(iter.Value())) {
				return
			}
		}
	}
}

func (s *TubeNode) doDeleteKey(tkt *Ticket) {
	vv("%v DELETE_KEY called on %v:%v", s.me(), tkt.Table, tkt.Key)
	if s.state.KVstore != nil {
		table, ok := s.state.KVstore.m[tkt.Table]
		if ok {
			table.Tree.Remove(art.Key(tkt.Key))
			const purgeEmptyTables = false // purge empty tables immediately?
			if purgeEmptyTables {
				if table.Tree.Size() == 0 {
					delete(s.state.KVstore.m, tkt.Table)
				}
			}
		}
	}
}

func (s *ArtTable) String() (r string) {
	iter := s.Tree.Iter(nil, nil)
	for iter.Next() {
		r += fmt.Sprintf("%v : %v\n", Key(iter.Key()), string(iter.Value()))
	}
	return
}

func (s *TubeNode) doMakeTable(tkt *Ticket) {
	if tkt.Table == "" {
		return // noop
	}
	_, ok := s.state.KVstore.m[tkt.Table]
	if ok {
		return // already there
	}
	s.state.KVstore.m[tkt.Table] = newArtTable()
}

func (s *TubeNode) doDeleteTable(tkt *Ticket) {
	vv("%v doDeleteTable called on %v", s.me(), tkt.Table)
	if len(s.state.KVstore.m) == 0 || tkt.Table == "" {
		return // noop
	}
	_, ok := s.state.KVstore.m[tkt.Table]
	if !ok {
		return // does not exist; noop.
	}
	delete(s.state.KVstore.m, tkt.Table)
}

func (s *TubeNode) doRenameTable(tkt *Ticket) {
	vv("%v doRenameTable called on %v:%v", s.me(), tkt.Table, tkt.NewTableName)

	if len(s.state.KVstore.m) == 0 {
		tkt.Err = ErrKeyNotFound
		return
	}
	if tkt.Table == "" {
		tkt.Err = fmt.Errorf("error in rename table: no existing table name supplied.")
		return
	}
	if tkt.NewTableName == "" {
		tkt.Err = fmt.Errorf("error in rename table: no new table name supplied.")
		return
	}
	if tkt.NewTableName == tkt.Table {
		return // no-op, already done.
	}
	_, ok := s.state.KVstore.m[tkt.NewTableName]
	if ok {
		tkt.Err = fmt.Errorf("error in rename table: target new table '%v' already exists.", tkt.NewTableName)
		return
	}
	tab, ok := s.state.KVstore.m[tkt.Table]
	if !ok {
		tkt.Err = fmt.Errorf("error in rename table: existing table '%v' not found.", tkt.Table)
		return
	}
	s.state.KVstore.m[tkt.NewTableName] = tab
	delete(s.state.KVstore.m, tkt.Table)
}

func (s *TubeNode) RenameTable(ctx context.Context, table, newTableName Key, waitForDur time.Duration, sess *Session) (tkt *Ticket, err error) {
	desc := fmt.Sprintf("rename table '%v' -> '%v'", table, newTableName)
	tkt = s.NewTicket(desc, table, "", nil, s.PeerID, s.name, RENAME_TABLE, waitForDur, ctx)
	tkt.NewTableName = newTableName
	if sess != nil {
		tkt.SessionID = sess.SessionID
		tkt.SessionSerial = sess.SessionSerial
		tkt.MinSessSerialWaiting = sess.MinSessSerialWaiting
		tkt.SessionLastKnownIndex = sess.LastKnownIndex
	}
	select {
	case s.writeReqCh <- tkt:
		// proceed to wait below for txt.Done
	case <-ctx.Done():
		err = ctx.Err()
		return
	case <-s.Halt.ReqStop.Chan:
		tkt = nil
		err = ErrShutDown // ErrTimeOut
		return
	}

	select {
	case <-tkt.Done.Chan: // RenameTable waits for completion
		err = tkt.Err
		if sess != nil && tkt.AsOfLogIndex > sess.LastKnownIndex {
			sess.LastKnownIndex = tkt.AsOfLogIndex
		}
		if sess != nil && err == nil {
			if tkt.SessionSerial > sess.MinSessSerialWaiting {
				sess.MinSessSerialWaiting = tkt.SessionSerial
			}
		}
		return
	case <-ctx.Done():
		err = ctx.Err()
		return
	case <-s.Halt.ReqStop.Chan:
		tkt = nil
		err = ErrShutDown
		return
	}
}

func (s *TubeNode) MakeTable(ctx context.Context, table Key, waitForDur time.Duration, sess *Session) (tkt *Ticket, err error) {

	desc := fmt.Sprintf("make table '%v'", table)
	tkt = s.NewTicket(desc, table, "", nil, s.PeerID, s.name, MAKE_TABLE, waitForDur, ctx)
	if sess != nil {
		tkt.SessionID = sess.SessionID
		tkt.SessionSerial = sess.SessionSerial
		tkt.MinSessSerialWaiting = sess.MinSessSerialWaiting
		tkt.SessionLastKnownIndex = sess.LastKnownIndex
	}
	select {
	case s.writeReqCh <- tkt:
		// proceed to wait below for txt.Done
	case <-ctx.Done():
		err = ctx.Err()
		return
	case <-s.Halt.ReqStop.Chan:
		tkt = nil
		err = ErrShutDown // ErrTimeOut
		return
	}

	select {
	case <-tkt.Done.Chan: // MakeTable waits for completion
		err = tkt.Err
		if sess != nil && tkt.AsOfLogIndex > sess.LastKnownIndex {
			sess.LastKnownIndex = tkt.AsOfLogIndex
		}
		if sess != nil && err == nil {
			if tkt.SessionSerial > sess.MinSessSerialWaiting {
				sess.MinSessSerialWaiting = tkt.SessionSerial
			}
		}
		return
	case <-ctx.Done():
		err = ctx.Err()
		return
	case <-s.Halt.ReqStop.Chan:
		tkt = nil
		err = ErrShutDown
		return
	}
}

func (s *TubeNode) doShowKeys(tkt *Ticket) {
	if s.state.KVstore == nil {
		tkt.Err = ErrKeyNotFound
		return
	}
	if len(s.state.KVstore.m) == 0 {
		tkt.Err = ErrKeyNotFound
		return
	}
	if tkt.Table == "" {
		// request to list tables
		i := 0
		var str string
		for tab := range s.state.KVstore.m {
			if i > 0 {
				str += "\n"
			}
			str += string(tab)
			i++
		}
		tkt.Val = []byte(str)
		return
	}
	table, ok := s.state.KVstore.m[tkt.Table]
	if !ok {
		tkt.Err = ErrKeyNotFound
		return
	}
	i := 0
	var str string
	for k := range table.All() {
		if i > 0 {
			str += "\n"
		}
		str += string(k)
		i++
	}
	tkt.Val = []byte(str)

}

// if ctx is nill we will use s.ctx
func (s *Session) CAS(ctx context.Context, table, key Key, oldVal, newVal Val, waitForDur time.Duration) (tkt *Ticket, err error) {
	if s.cli == nil {
		return nil, fmt.Errorf("error in Session.Write: cli is nil, Session.Errs='%v'", s.Errs)
	}
	s.SessionSerial++
	if ctx == nil {
		ctx = s.ctx
	}
	return s.cli.CAS(ctx, table, key, oldVal, newVal, waitForDur, s)
}

// if ctx is nill we will use s.ctx
func (s *Session) Write(ctx context.Context, table, key Key, val Val, waitForDur time.Duration) (tkt *Ticket, err error) {
	if s.cli == nil {
		return nil, fmt.Errorf("error in Session.Write: cli is nil, Session.Errs='%v'", s.Errs)
	}
	s.SessionSerial++
	if ctx == nil {
		ctx = s.ctx
	}
	vv("submitting write with s.SessionSerial = %v", s.SessionSerial)
	return s.cli.Write(ctx, table, key, val, waitForDur, s)
}

func (s *Session) RenameTable(ctx context.Context, table, newTableName Key, waitForDur time.Duration) (tkt *Ticket, err error) {
	if s.cli == nil {
		return nil, fmt.Errorf("error in Session.RenameTable: cli is nil, Session.Errs='%v'", s.Errs)
	}
	s.SessionSerial++
	if ctx == nil {
		ctx = s.ctx
	}
	return s.cli.RenameTable(ctx, table, newTableName, waitForDur, s)
}

func (s *Session) MakeTable(ctx context.Context, table Key, waitForDur time.Duration) (tkt *Ticket, err error) {
	if s.cli == nil {
		return nil, fmt.Errorf("error in Session.MakeTable: cli is nil, Session.Errs='%v'", s.Errs)
	}
	s.SessionSerial++
	if ctx == nil {
		ctx = s.ctx
	}
	return s.cli.MakeTable(ctx, table, waitForDur, s)
}

func (s *Session) DeleteTable(ctx context.Context, table Key, waitForDur time.Duration) (tkt *Ticket, err error) {
	if s.cli == nil {
		return nil, fmt.Errorf("error in Session.DeleteTable: cli is nil, Session.Errs='%v'", s.Errs)
	}
	s.SessionSerial++
	if ctx == nil {
		ctx = s.ctx
	}
	return s.cli.DeleteTable(ctx, table, waitForDur, s)
}

// if ctx is nill we will use s.ctx
func (s *Session) Read(ctx context.Context, table, key Key, waitForDur time.Duration) (tkt *Ticket, err error) {
	if s.cli == nil {
		return nil, fmt.Errorf("error in Session.Read: cli is nil, Session.Errs='%v'", s.Errs)
	}
	s.SessionSerial++
	if ctx == nil {
		ctx = s.ctx
	}
	return s.cli.Read(ctx, table, key, waitForDur, s)
}

// if ctx is nill we will use s.ctx
func (s *Session) DeleteKey(ctx context.Context, table, key Key, waitForDur time.Duration) (tkt *Ticket, err error) {
	if s.cli == nil {
		return nil, fmt.Errorf("error in Session.DeleteKey: cli is nil, Session.Errs='%v'", s.Errs)
	}
	s.SessionSerial++
	if ctx == nil {
		ctx = s.ctx
	}
	return s.cli.DeleteKey(ctx, table, key, waitForDur, s)
}

// empty table means list all the tables.
// if ctx is nill we will use s.ctx
func (s *Session) ShowKeys(ctx context.Context, table Key, waitForDur time.Duration) (tkt *Ticket, err error) {
	if s.cli == nil {
		return nil, fmt.Errorf("error in Session.ShowKeys: cli is nil, Session.Errs='%v'", s.Errs)
	}
	s.SessionSerial++
	if ctx == nil {
		ctx = s.ctx
	}
	return s.cli.ShowKeys(ctx, table, waitForDur, s)
}

func (s *Session) ReadKeyRange(ctx context.Context, table, key, keyEndx Key, descend bool, waitForDur time.Duration) (tkt *Ticket, err error) {
	if s.cli == nil {
		return nil, fmt.Errorf("error in Session.ReadKeyRange: cli is nil, Session.Errs='%v'", s.Errs)
	}
	s.SessionSerial++
	if ctx == nil {
		ctx = s.ctx
	}
	return s.cli.ReadKeyRange(ctx, table, key, keyEndx, descend, waitForDur, s)
}
