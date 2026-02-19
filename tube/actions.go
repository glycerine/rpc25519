package tube

import (
	//"bytes"
	//"errors"
	"fmt"
	"iter"
	//"net"
	//"sort"
	"strings"

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

var ErrNeedNewSession = fmt.Errorf("need new session")

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
func (s *TubeNode) Write(ctx context.Context, table, key Key, val Val, waitForDur time.Duration, sess *Session, vtype string, leaseDur time.Duration, leaseAutoDel bool) (tkt *Ticket, err error) {

	if leaseDur != 0 {
		// sanity check
		if leaseDur < 0 || leaseDur > time.Minute*15 {
			return nil, fmt.Errorf("leaseDur out of bounds, must be in [0, 15 minutes]: '%v'", leaseDur)
		}
		if s.name == "" {
			return nil, fmt.Errorf("must have s.name for Leasor to take a lease")
		}
	}

	desc := fmt.Sprintf("write: key(%v) = val(%v)", key, showExternalCluster(val))
	if vtype != "" {
		desc += fmt.Sprintf("; vtype='%v'", vtype)
	}
	if leaseDur > 0 {
		desc += fmt.Sprintf("; leaseDur='%v' requested for Leasor '%v' (autoDel: %v)", leaseDur, s.name, leaseAutoDel)
	}
	tkt = s.NewTicket(desc, table, key, val, s.PeerID, s.name, WRITE, waitForDur, ctx)
	tkt.Vtype = vtype
	if leaseDur > 0 {
		tkt.LeaseRequestDur = leaseDur
		// let leader set this using tkt.RaftLogEntryTm.Add(tkt.LeaseRequestDur)
		//tkt.LeaseUntilTm // set at tube.go:5282 in replicateTicket().
		tkt.Leasor = s.name
		tkt.LeaseAutoDel = leaseAutoDel
	}
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

// CAS is also known as Compare and Swap.
//
// There are three types of CAS available. They
// are checked in this order, and if one
// is specified the others are ignored. First we check for
// oldVersionCAS, then oldLeaseEpochCAS, the simple oldval based CAS.
//
// A zero in oldVersionCAS or a zero in oldLeaseEpochCAS
// means that they are inactive. In inactive oldval is
// passed with len(oldval) == 0.
//
// For example, if oldLeaseEpochCAS is specified, the oldval
// will never be examined during the compare-and-swap; and
// the oldVersionCAS must be nil for oldLeaseEpochCAS to be
// checked at all.
//
// If all three are not specified (oldVersion=0, and oldLeaseEpochCAS=0,
// and oldval=nil), then the CAS is equivalent to a Write.
// The leasing operations are still applied as usual for a Write.
func (s *TubeNode) CAS(ctx context.Context, table, key Key, oldval, newval Val, waitForDur time.Duration, sess *Session, newVtype string, leaseDur time.Duration, leaseAutoDel bool, oldVersionCAS, oldLeaseEpochCAS int64) (tkt *Ticket, err error) {

	if ctx.Err() != nil {
		panicf("%v: TubeNode.CAS sees already cancelled ctx, bailing early", s.name) // not seen
		err = ErrNeedNewSession
		return
	}
	if sess != nil && sess.ctx.Err() != nil {
		panicf("%v: TubeNode.CAS sees already cancelled sess.ctx, bailing early", s.name) // seen a ton
		err = ErrNeedNewSession
		return // was firing _every_ time!
	}
	//vv("%v past top two CAS session checks", s.name) // not seen.

	if oldVersionCAS > 0 && oldLeaseEpochCAS > 0 {
		return nil, fmt.Errorf("error in call to CAS: cannot have both oldVersionCAS and oldLeaseEpochCAS set.")
	}
	if len(oldval) > 0 {
		if oldVersionCAS > 0 || oldLeaseEpochCAS > 0 {
			return nil, fmt.Errorf("error in call to CAS: cannot oldval AND (either oldVersionCAS(%v) and oldLeaseEpochCAS(%v)) set: only one of the CAS will be checked.", oldVersionCAS, oldLeaseEpochCAS)
		}
	}

	if leaseDur != 0 {
		// sanity check
		if leaseDur < 0 || leaseDur > time.Minute*15 {
			return nil, fmt.Errorf("leaseDur out of bounds, must be in [0, 15 minutes]: '%v'", leaseDur)
		}
		if s.name == "" {
			return nil, fmt.Errorf("must have s.name for Leasor to take a lease")
		}
	}

	desc := fmt.Sprintf("cas(table(%v), key(%v) write newval '%v'", table, key, string(newval))
	if len(oldval) > 0 {
		//desc += fmt.Sprintf("; if oldval(len %v) present", len(oldval))
		desc += fmt.Sprintf("; if oldval('%v') present", string(oldval))
	}
	if oldLeaseEpochCAS > 0 {
		desc += fmt.Sprintf("; if oldLeaseEpochCAS == '%v'", oldLeaseEpochCAS)
		//vv("%v oldLeaseEpochCAS = %v", s.me(), oldLeaseEpochCAS)
	}
	if oldVersionCAS > 0 {
		desc += fmt.Sprintf("; if oldVersionCAS == '%v'", oldVersionCAS)
	}
	if leaseDur > 0 {
		desc += fmt.Sprintf("; leaseDur='%v' requested for Leasor '%v'", leaseDur, s.name)
	}
	tkt = s.NewTicket(desc, table, key, newval, s.PeerID, s.name, CAS, waitForDur, ctx)
	tkt.OldVal = oldval
	tkt.Vtype = newVtype
	tkt.OldVersionCAS = oldVersionCAS
	tkt.OldLeaseEpochCAS = oldLeaseEpochCAS
	tkt.LeaseAutoDel = leaseAutoDel
	if leaseDur > 0 {
		tkt.LeaseRequestDur = leaseDur
		tkt.Leasor = s.name
	}

	var sessDone <-chan struct{}
	if sess != nil {
		tkt.SessionID = sess.SessionID
		tkt.SessionSerial = sess.SessionSerial
		tkt.MinSessSerialWaiting = sess.MinSessSerialWaiting
		tkt.SessionLastKnownIndex = sess.LastKnownIndex
		//vv("Write set tkt.SessionSerial = %v", tkt.SessionSerial)
		sessDone = sess.ctx.Done()
	}

	select {
	case s.writeReqCh <- tkt:
		// proceed to wait below for txt.done
		//vv("%v CAS submitted on writeReqCh", s.name) // not seen!
	case <-ctx.Done():
		//vv("case <-ctx.Done() 1") // not seen.
		err = ctx.Err()
		return
	case <-sessDone:
		err = sess.ctx.Err()
		//vv("case <-sessDone 1, err='%v'", err) // seen lots. err='context canceled'
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
		//vv("case <-ctx.Done() 2") // not seen.
		err = ctx.Err()
		return
	case <-sessDone:
		err = sess.ctx.Err()
		//vv("case <-sessDone 2, err = '%v'", err) // seen lots. err='context canceled'
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

	// TODO: we can get stuck here if our tcp
	// connection dropped (e.g. laptop slept and we get
	// here but have not yet realized it), so we
	// need to figure out how to bail early/retry
	// if our request was lost/the network is down...
	// Of course we have the ctx.Done already;
	// which can timeout for clients who want a limited wait.
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

// called on leader locally to see if lease write would win or just read.
// When we return false we also fill in the current leaf lease info.
func (s *TubeNode) kvstoreWouldWriteLease(tkt *Ticket) (wouldWrite bool) {
	return s.kvstoreWrite(tkt, true, false)
}

// dry => dry run => just return wouldWrite without any action
// except to overwrite Val on wouldWrite=false with the current value;
// thus a failed write becomes a read of the current value. This
// is essential czar/member election mechanism that avoids
// several additional network roundtrips.
//
// testingImmut => if true && dry, do not re-write Val on ticket
// when returning false (for testing). No effect if !dry.
func (s *TubeNode) kvstoreWrite(tkt *Ticket, dry, testingImmut bool) (wouldWrite bool) {

	st := s.state
	if dry {
		if st == nil {
			return true
		}
	} else {
		// wet:
		testingImmut = false // no effect unless dry too.
		if tkt.RaftLogEntryTm.IsZero() {
			panic("tkt.RaftLogEntryTm should not be zero")
		}
	}
	clockDriftBound := s.cfg.ClockDriftBound
	tktTable := tkt.Table
	tktKey := tkt.Key
	tktVal := tkt.Val

	var surelyNoPrior bool
	if st.KVstore == nil {
		if dry {
			return true
		}
		st.KVstore = newKVStore()
		surelyNoPrior = true
	}
	if st.KVstore.m == nil {
		if dry {
			return true
		}
		st.KVstore.m = make(map[Key]*ArtTable)
		surelyNoPrior = true
	}
	table, ok := st.KVstore.m[tktTable]
	if !ok {
		if dry {
			return true
		}
		surelyNoPrior = true
		table = newArtTable()
		st.KVstore.m[tktTable] = table
	}

	key := art.Key(tktKey)
	var leaf *art.Leaf
	var found bool
	if !surelyNoPrior {
		// is there a prior lease that must be respected?
		leaf, _, found = table.Tree.Find(art.Exact, key)
	}

	if !found {
		if dry {
			return true
		}
		leaf = art.NewLeaf(key, append([]byte{}, tktVal...), tkt.Vtype)
		leaf.Leasor = tkt.Leasor
		leaf.LeasorPeerID = tkt.FromID
		leaf.LeaseEpochT0 = tkt.RaftLogEntryTm

		leaf.LeaseUntilTm = tkt.LeaseUntilTm
		leaf.WriteRaftLogIndex = tkt.LogIndex
		leaf.LeaseEpoch = 1
		leaf.Version = 1
		leaf.AutoDelete = tkt.LeaseAutoDel

		tkt.LeaseEpoch = leaf.LeaseEpoch
		tkt.LeaseWriteRaftLogIndex = leaf.WriteRaftLogIndex
		tkt.LeaseEpochT0 = leaf.LeaseEpochT0

		table.Tree.InsertLeaf(leaf)

		//vv("%v wrote key '%v' (no prior key; leasor='%v' until '%v'); KVstore now len=%v; leaf.LeaseEpochT0='%v'", st.name, tktKey, leaf.Leasor, leaf.LeaseUntilTm, st.KVstore.Len(), nice(leaf.LeaseEpochT0))
		return true
	}
	// key already present, so if leased and not leased by Leasor,
	// we must reject the write.

	if !dry && tkt.LogIndex <= leaf.WriteRaftLogIndex {
		// might not be doing much, but here we insist
		// that the sequence of writes follows a RaftIndex
		// that is strictly monotonically increasing.
		tkt.Err = fmt.Errorf(rejectedWritePrefix+" because rejecting tkt with LogIndex <= leaf.WriteRaftLogIndex. TicketID:'%v'; LogIndex='%v'; leaf.WriteRaftLogIndex='%v' (desc: '%v')", tkt.TicketID, tkt.LogIndex, leaf.WriteRaftLogIndex, tkt.Desc)
		if !testingImmut {
			s.writeFailedSetCurrentVal(tkt, leaf)
		}
		return false
	}

	if leaf.Leasor == "" || leaf.LeaseUntilTm.IsZero() {
		// no current leasor, just put the write through.
		if dry {
			return true
		}
		leaf.Value = append([]byte{}, tktVal...)
		leaf.Vtype = tkt.Vtype
		leaf.Leasor = tkt.Leasor
		leaf.LeasorPeerID = tkt.FromID
		leaf.LeaseEpochT0 = tkt.RaftLogEntryTm

		leaf.LeaseUntilTm = tkt.LeaseUntilTm
		leaf.WriteRaftLogIndex = tkt.LogIndex
		leaf.LeaseEpoch++
		leaf.Version++
		leaf.AutoDelete = tkt.LeaseAutoDel

		tkt.LeaseEpoch = leaf.LeaseEpoch
		tkt.LeaseWriteRaftLogIndex = leaf.WriteRaftLogIndex

		//vv("%v wrote key '%v' (no current lease); KVstore now len=%v", st.name, tktKey, st.KVstore.Len())
		return true
	}
	// INVAR: leaf.Leasor != "" && leaf.LeaseUntilTm > 0
	// prior key and prior lease on key is present.

	// lease end points must be strictly monotically increasing

	now := tkt.RaftLogEntryTm
	if dry {
		now = time.Now() // stand in for tkt.RaftLogEntryTm on dry run.
	}
	if tkt.LeaseRequestDur == 0 { // was UntilTm.IsZero() {
		// not extending lease, going to non-leased. skip down.
		//vv("dry tkt.LeaseRequestDur == 0 skip down ")
	} else {
		leaseUntilTm := tkt.LeaseUntilTm
		if dry {
			// faked based on now since tkt.LeaseUntilTm will not be set.
			leaseUntilTm = now.Add(tkt.LeaseRequestDur)
		}
		if lte(leaseUntilTm, leaf.LeaseUntilTm) {
			tkt.Err = fmt.Errorf(rejectedWritePrefix+" because non-increasing LeaseUntilTm rejected. table='%v'; key='%v'; current leasor='%v' (LeasorPeerID: '%v'); leaf.LeaseUntilTm='%v' >= tkt.LeaseUntilTm='%v'; rejecting attempted tkt.Leasor='%v' at now/tkt.RaftLogEntryTm='%v');", tktTable, tktKey, leaf.Leasor, leaf.LeasorPeerID, leaf.LeaseUntilTm.Format(rfc3339NanoNumericTZ0pad), leaseUntilTm.Format(rfc3339NanoNumericTZ0pad), tkt.Leasor, now.Format(rfc3339NanoNumericTZ0pad))
			if !testingImmut {
				s.writeFailedSetCurrentVal(tkt, leaf)
			}
			return false
		}

		if leaf.Leasor == tkt.Leasor &&
			leaf.LeasorPeerID == tkt.FromID { // avoid 2 peers with same name confusion.
			if dry {
				return true
			}
			// current leasor extending lease, allow it (expired or not)
			// already set: leaf.Leasor = tkt.Leasor
			leaf.LeasorPeerID = tkt.FromID // allows rebooted leasor to extend, do we want this?
			// leave alone!: leaf.LeaseEpochT0

			leaf.Value = append([]byte{}, tktVal...)
			leaf.Vtype = tkt.Vtype
			leaf.LeaseUntilTm = tkt.LeaseUntilTm
			leaf.WriteRaftLogIndex = tkt.LogIndex
			// leave this the same! no epoch change! leaf.LeaseEpoch
			leaf.Version++
			leaf.AutoDelete = tkt.LeaseAutoDel

			tkt.LeaseEpoch = leaf.LeaseEpoch
			tkt.LeaseWriteRaftLogIndex = leaf.WriteRaftLogIndex
			//vv("%v wrote key '%v' extending current lease for '%v'; KVstore now len=%v", st.name, tktKey, tkt.Leasor, st.KVstore.Len())
			return true
		}
	}
	// has prior lease expired?
	// careful: the leaf.LeaseUntilTm could be from the
	// previous leader's clock, and tkt.RaftLogEntryTm could
	// be from current leader's clock, so also
	// include clock drift bound.
	okAfter := leaf.LeaseUntilTm.Add(clockDriftBound)
	expiredDur := tkt.RaftLogEntryTm.Sub(okAfter)
	if expiredDur > 0 { // tkt.RaftLogEntryTm.After(okAfter) {
		// prior lease expired, allow write.
		if dry {
			return true
		}
		// overwritten value (old czar) can be useful to expire them quickly.
		// (Performance optimization; not correctness critical since any
		// key range scan between expiry and now might have wiped an expired
		// leaf anyway).
		tkt.PrevLeaseVal = leaf.Value
		tkt.PrevLeaseVtype = leaf.Vtype
		leaf.LeaseRenewalElap = expiredDur

		leaf.Value = append([]byte{}, tktVal...)
		leaf.Vtype = tkt.Vtype
		leaf.Leasor = tkt.Leasor
		leaf.LeasorPeerID = tkt.FromID
		leaf.LeaseEpochT0 = tkt.RaftLogEntryTm

		leaf.LeaseUntilTm = tkt.LeaseUntilTm
		leaf.WriteRaftLogIndex = tkt.LogIndex
		leaf.LeaseEpoch++
		leaf.Version++
		leaf.AutoDelete = tkt.LeaseAutoDel

		tkt.LeaseEpoch = leaf.LeaseEpoch
		tkt.LeaseWriteRaftLogIndex = leaf.WriteRaftLogIndex
		//vv("%v wrote key '%v' updating to new leasor; KVstore now len=%v", st.name, tktKey, st.KVstore.Len())
		return true
	}
	// key is already leased by a different leasor, and lease has not expired: reject.
	tkt.Err = fmt.Errorf(rejectedWritePrefix+" to leased key. table='%v'; key='%v'; current leasor='%v'; leasedUntilTm='%v'; LeaseEpoch='%v'; rejecting attempted tkt.Leasor='%v' at tkt.RaftLogEntryTm='%v' (left on lease: '%v'); ClockDriftBound='%v'", tktTable, tktKey, leaf.Leasor, leaf.LeaseUntilTm.Format(rfc3339NanoNumericTZ0pad), leaf.LeaseEpoch, tkt.Leasor, tkt.RaftLogEntryTm.Format(rfc3339NanoNumericTZ0pad), leaf.LeaseUntilTm.Sub(tkt.RaftLogEntryTm), clockDriftBound)

	// to allow simple client czar election (not the
	// raft leader election but an application level
	// election among clients), on rejection of a lease
	// we also reply with the Val/Leasor info so the contendor
	// knows who got here first (and thus is 'elected').
	if !testingImmut {
		s.writeFailedSetCurrentVal(tkt, leaf)
	}

	//vv("%v reject write to already leased key '%v' (held by '%v', rejecting '%v'); KVstore now len=%v", st.name, tktKey, leaf.Leasor, tkt.Leasor, st.KVstore.Len())
	return false
}

func (s *TubeNode) writeFailedSetCurrentVal(tkt *Ticket, leaf *art.Leaf) {

	// a copy would be safer, but consumes a whole
	// lot of memory when we are just going to
	// send it over the wire anway... just be sure
	// to treat tkt.Val as READ ONLY(!)
	tkt.Val = leaf.Value

	//tkt.Val = append([]byte{}, leaf.Value...)
	tkt.Vtype = leaf.Vtype
	tkt.Leasor = leaf.Leasor
	tkt.LeasorPeerID = leaf.LeasorPeerID
	tkt.LeaseEpochT0 = leaf.LeaseEpochT0

	// include the clockDriftBound so they can just wait until.
	tkt.LeaseUntilTm = leaf.LeaseUntilTm.Add(s.cfg.ClockDriftBound)
	tkt.LeaseEpoch = leaf.LeaseEpoch
	tkt.LeaseWriteRaftLogIndex = leaf.WriteRaftLogIndex
	tkt.VersionRead = leaf.Version
	tkt.LeaseAutoDel = leaf.AutoDelete
}

func (s *RaftState) kvstoreRangeScan(tkt *Ticket, tktTable, tktKey, tktKeyEndx Key, descend bool) (results *art.Tree, err error) {
	table, ok := s.KVstore.m[tktTable]
	if !ok {
		return nil, ErrKeyNotFound
	}

	deadzone := s.ensureDeadzone()
	now := time.Now()

	//vv("%v kvstoreRangeScan table='%v', key='%v', keyEndx='%v'; descend=%v", s.name, tktTable, tktKey, tktKeyEndx, descend)
	results = art.NewArtTree()
	results.SkipLocking = true
	if descend {
		// note this is correct, the endx comes first in art.Descend.
		for key, lf := range art.Descend(table.Tree, art.Key(tktKeyEndx), art.Key(tktKey)) {
			_ = key
			// implement AutoDelete
			if lf.AutoDelete && tktTable != "dead" &&
				lf.Leasor != "" &&
				lf.LeaseUntilTm.Before(now) {

				deadzone.Tree.InsertLeaf(lf)
				table.Tree.Remove(art.Key(key))

				//vv("Descend did auto-delete of table '%v'/key '%v'", tktTable, tktKey)
				continue
			}
			//vv("Descend sees key '%v' -> lf.Value: '%v'", string(key), string(lf.Value))
			// make copies.
			//key2 := append([]byte{}, key...)
			//val2 := append([]byte{}, lf.Value...)
			//results.Insert(key2, val2, lf.Vtype)
			lf2 := lf.Clone()
			results.InsertLeaf(lf2)
		}
	} else {
		for key, lf := range art.Ascend(table.Tree, art.Key(tktKey), art.Key(tktKeyEndx)) {
			_ = key
			// implement AutoDelete
			if lf.AutoDelete && tktTable != "dead" &&
				lf.Leasor != "" &&
				lf.LeaseUntilTm.Before(now) {

				deadzone.Tree.InsertLeaf(lf)
				table.Tree.Remove(art.Key(key))

				continue
			}
			//vv("Ascend sees key '%v' -> lf.Value: '%v'", string(key), string(lf.Value))
			// make copies.
			//key2 := append([]byte{}, key...)
			//val2 := append([]byte{}, lf.Value...)
			//results.Insert(key2, val2, lf.Vtype)
			lf2 := lf.Clone()
			results.InsertLeaf(lf2)
		}
	}
	return
}

func (s *RaftState) ensureDeadzone() (deadzone *ArtTable) {
	deadzone, ok := s.KVstore.m["dead"]
	if !ok {
		deadzone = newArtTable()
		s.KVstore.m["dead"] = deadzone
	}
	return deadzone
}

func (s *RaftState) KVStoreRead(tkt *Ticket, tktTable, tktKey Key) ([]byte, string, error) {
	table, ok := s.KVstore.m[tktTable]
	if !ok {
		return nil, "", ErrKeyNotFound
	}
	lf, _, ok := table.Tree.Find(art.Exact, art.Key(tktKey))
	if ok {
		// implement AutoDelete
		if lf.AutoDelete && tktTable != "dead" &&
			lf.Leasor != "" &&
			lf.LeaseUntilTm.Before(time.Now()) {

			deadzone := s.ensureDeadzone()
			deadzone.Tree.InsertLeaf(lf)
			table.Tree.Remove(art.Key(tktKey))

			return nil, "", ErrKeyNotFound
		}

		return lf.Value, lf.Vtype, nil
	}
	return nil, "", ErrKeyNotFound
}

func (s *RaftState) KVStoreReadLeaf(tkt *Ticket, tktTable, tktKey Key) (*art.Leaf, error) {
	table, ok := s.KVstore.m[tktTable]
	if !ok {
		return nil, ErrKeyNotFound
	}
	lf, _, ok := table.Tree.Find(art.Exact, art.Key(tktKey))
	if ok {
		// implement AutoDelete
		if lf.AutoDelete && tktTable != "dead" &&
			lf.Leasor != "" &&
			lf.LeaseUntilTm.Before(time.Now()) {

			deadzone := s.ensureDeadzone()
			deadzone.Tree.InsertLeaf(lf)
			table.Tree.Remove(art.Key(tktKey))

			return nil, ErrKeyNotFound
		}
		return lf, nil
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
func (s *ArtTable) All() iter.Seq2[Key, *art.Leaf] {
	return func(yield func(Key, *art.Leaf) bool) {
		iter := s.Tree.Iter(nil, nil)
		for iter.Next() {
			if !yield(Key(iter.Key()), iter.Leaf()) {
				return
			}
		}
	}
}

func (s *TubeNode) doDeleteKey(tkt *Ticket) {
	//vv("%v DELETE_KEY called on %v:%v", s.me(), tkt.Table, tkt.Key)
	if s.state.KVstore != nil {
		table, ok := s.state.KVstore.m[tkt.Table]
		var doDelete bool
		if ok {
			// is key leased? cannot delete until lease is up,
			// unless the requestor is also the Leasor.
			var leaf *art.Leaf
			leaf, tkt.Err = s.state.KVStoreReadLeaf(tkt, tkt.Table, tkt.Key)
			if leaf != nil {
				if leaf.Leasor == "" || leaf.LeaseUntilTm.IsZero() {
					// no current leasor, just put the delete through.
					doDelete = true
				} else {
					// INVAR: leaf.Leasor != "" && leaf.LeaseUntilTm > 0
					if leaf.Leasor == tkt.Leasor {
						doDelete = true // allow current leasor to give up lease.
					} else {
						if tkt.RaftLogEntryTm.After(
							leaf.LeaseUntilTm.Add(s.cfg.ClockDriftBound)) {
							// lease has expired, allow delete.
							doDelete = true
						} else {
							tkt.Err = fmt.Errorf("prior lease on key is not expired. table='%v'; key='%v'; Leasor='%v'; LeaseUntilTm='%v'", tkt.Table, tkt.Key, leaf.Leasor, nice(leaf.LeaseUntilTm))
						}
					}
				}
			}

			if doDelete {
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
	//vv("%v doDeleteTable called on %v", s.me(), tkt.Table)
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
	//vv("%v doRenameTable called on %v:%v", s.me(), tkt.Table, tkt.NewTableName)

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
	results := art.NewArtTree()
	results.SkipLocking = true
	tkt.KeyValRangeScan = results

	if tkt.Table == "" {
		// request to list tables
		for tableName := range s.state.KVstore.m {
			results.Insert(art.Key(tableName), nil, "")
		}
		return
	}
	tktTable := tkt.Table
	table, ok := s.state.KVstore.m[tktTable]
	if !ok {
		tkt.Err = ErrKeyNotFound
		return
	}
	deadzone := s.state.ensureDeadzone()
	now := time.Now()

	for k, lf := range art.Ascend(table.Tree, nil, nil) {
		// implement AutoDelete
		if lf.AutoDelete && tktTable != "dead" &&
			lf.Leasor != "" &&
			lf.LeaseUntilTm.Before(now) {

			deadzone.Tree.InsertLeaf(lf)
			table.Tree.Remove(art.Key(k))

			continue
		}

		results.Insert(art.Key(k), nil, "")
	}
}

// CAS is also known as Compare and Swap.
//
// There are three types of CAS available. They
// are checked in this order, and if one
// is specified the others are ignored. First we check for
// oldVersionCAS, then oldLeaseEpochCAS, the simple oldval based CAS.
//
// A zero in oldVersionCAS or a zero in oldLeaseEpochCAS
// means that they are inactive. In inactive oldval is
// passed with len(oldval) == 0.
//
// For example, if oldLeaseEpochCAS is specified, the oldval
// will never be examined during the compare-and-swap; and
// the oldVersionCAS must be nil for oldLeaseEpochCAS to be
// checked at all.
//
// If all three are not specified (oldVersion=0, and oldLeaseEpochCAS=0,
// and oldval=nil), then the CAS is equivalent to a Write.
// The leasing operations are still applied as usual for a Write.
//
// If ctx is nill we will use s.ctx instead.
func (s *Session) CAS(ctx context.Context, table, key Key, oldVal, newVal Val, waitForDur time.Duration, newVtype string, leaseDur time.Duration, leaseAutoDel bool, oldVersionCAS, oldLeaseEpochCAS int64) (tkt *Ticket, err error) {
	if s.cli == nil {
		return nil, fmt.Errorf("error in Session.Write: cli is nil, Session.Errs='%v'", s.Errs)
	}
	s.SessionSerial++
	if ctx == nil {
		ctx = s.ctx

		if s.ctx.Err() != nil {
			vv("%v: Session.CAS sees already cancelled Session.ctx, bailing early", s.cli.name) // not seen.
			err = ErrNeedNewSession
			return
		}
	} else {
		if ctx.Err() != nil {
			vv("%v: Session.CAS sees already cancelled ctx, bailing early", s.cli.name) // not seen.
			err = ErrNeedNewSession
			return
		}
	}
	return s.cli.CAS(ctx, table, key, oldVal, newVal, waitForDur, s, newVtype, leaseDur, leaseAutoDel, oldVersionCAS, oldLeaseEpochCAS)
}

// if ctx is nill we will use s.ctx
func (s *Session) Write(ctx context.Context, table, key Key, val Val, waitForDur time.Duration, vtype string, leaseDur time.Duration, leaseAutoDel bool) (tkt *Ticket, err error) {
	if s.cli == nil {
		return nil, fmt.Errorf("error in Session.Write: cli is nil, Session.Errs='%v'", s.Errs)
	}
	s.SessionSerial++
	if ctx == nil {
		ctx = s.ctx
	}
	//vv("submitting write with s.SessionSerial = %v", s.SessionSerial)
	return s.cli.Write(ctx, table, key, val, waitForDur, s, vtype, leaseDur, leaseAutoDel)
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

func (s *Session) ReadPrefixRange(ctx context.Context, table, prefix Key, descend bool, waitForDur time.Duration) (tkt *Ticket, err error) {
	if s.cli == nil {
		return nil, fmt.Errorf("error in Session.ReadPrefixRange: cli is nil, Session.Errs='%v'", s.Errs)
	}
	s.SessionSerial++
	if ctx == nil {
		ctx = s.ctx
	}
	return s.cli.ReadPrefixRange(ctx, table, prefix, descend, waitForDur, s)
}

func (s *TubeNode) ReadPrefixRange(ctx context.Context, table, prefix Key, descend bool, waitForDur time.Duration, sess *Session) (tkt *Ticket, err error) {

	desc := fmt.Sprintf("read prefix('%v') range from table '%v' (descend:%v)", prefix, table, descend)
	tkt = s.NewTicket(desc, table, prefix, nil, s.PeerID, s.name, READ_PREFIX_RANGE, waitForDur, ctx)
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

	// TODO: we can get stuck here if our tcp
	// connection dropped (e.g. laptop slept and we get
	// here but have not yet realized it), so we
	// need to figure out how to bail early/retry
	// if our request was lost/the network is down...
	// Of course we have the ctx.Done already;
	// which can timeout for clients who want a limited wait.
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

func (s *RaftState) kvstorePrefixScan(tkt *Ticket, tktTable, tktPrefix Key, descend bool) (results *art.Tree, err error) {
	table, ok := s.KVstore.m[tktTable]
	if !ok {
		return nil, ErrKeyNotFound
	}
	//vv("%v kvstorePrefixScan table='%v', key='%v'; descend=%v", s.name, tktTable, tktPrefix, descend)
	results = art.NewArtTree()
	results.SkipLocking = true

	deadzone := s.ensureDeadzone()
	now := time.Now()

	if descend {
		// the endx comes first in art.Descend.
		for key, lf := range art.Descend(table.Tree, nil, art.Key(tktPrefix)) {
			if !strings.HasPrefix(string(key), string(tktPrefix)) {
				return
			}
			// implement AutoDelete
			if lf.AutoDelete && tktTable != "dead" &&
				lf.Leasor != "" &&
				lf.LeaseUntilTm.Before(now) {

				deadzone.Tree.InsertLeaf(lf)
				table.Tree.Remove(art.Key(key))

				continue
			}
			//vv("Descend sees key '%v' -> lf.Value: '%v'", string(key), string(lf.Value))
			lf2 := lf.Clone()
			results.InsertLeaf(lf2)
		}
	} else {
		for key, lf := range art.Ascend(table.Tree, art.Key(tktPrefix), nil) {
			if !strings.HasPrefix(string(key), string(tktPrefix)) {
				return
			}
			// implement AutoDelete
			if lf.AutoDelete && tktTable != "dead" &&
				lf.Leasor != "" &&
				lf.LeaseUntilTm.Before(now) {

				deadzone.Tree.InsertLeaf(lf)
				table.Tree.Remove(art.Key(key))

				continue
			}
			//vv("Ascend sees key '%v' -> lf.Value: '%v'", string(key), string(lf.Value))
			lf2 := lf.Clone()
			results.InsertLeaf(lf2)
		}
	}
	return
}

/* old, think we can delete
// return nil error if okay, else lease is still in force.
func (cfg *TubeConfig) okayToWritePossiblyLeasedKey(leaf *art.Leaf, tkt *Ticket) error {
	if leaf.Leasor == "" || leaf.LeaseUntilTm.IsZero() {
		// no current leasor
		return nil
	}
	// INVAR: leaf.Leasor != "" && leaf.LeaseUntilTm > 0
	if leaf.Leasor == tkt.Leasor {
		// allow current leasor to give up lease/write new val.
		return nil
	}
	if tkt.RaftLogEntryTm.After(
		leaf.LeaseUntilTm.Add(cfg.ClockDriftBound)) {
		// lease has expired, allow delete.
		return nil
	}
	return fmt.Errorf("prior lease on key is not expired. table='%v'; key='%v'; Leasor='%v'; LeaseUntilTm='%v'", tkt.Table, tkt.Key, leaf.Leasor, nice(leaf.LeaseUntilTm))
}
*/
