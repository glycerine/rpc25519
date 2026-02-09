package hermes

import (
	"fmt"
	//"sort"
	"bytes"
	"encoding/binary"
	"time"

	//"github.com/glycerine/idem"
	"github.com/glycerine/blake3"
	rpc "github.com/glycerine/rpc25519"
)

var _ = bytes.Compare
var _ = blake3.New
var _ = binary.LittleEndian

// add invarient that cli... does
// eventually get their writes or reads acknowledged?
// b/c might timeout in a loop repeatedly if somebody
// else was the last writer/reader and so their
// request got overwritten.

//go:generate greenpack

// protocol table
// https://github.com/ease-lab/Hermes/blob/master/tla/protocol-actions.png

type Key string // for now... []byte later.
type Val []byte

// timestamp
type TS struct {
	Version int64  `zid:"0"`
	CoordID string `zid:"1"`
}

func (ts *TS) String() string {
	return fmt.Sprintf(`TS{Version: %v, CoordID: %v}`,
		ts.Version, rpc.AliasDecode(ts.CoordID))
}

func (a *TS) Compare(b *TS) int {
	if a.Version < b.Version {
		return -1
	}
	if a.Version > b.Version {
		return 1
	}
	// INVAR: a.Version == b.Version
	if a.CoordID == b.CoordID {
		return 0
	}
	// INVAR: a.CoordID != b.CoordID

	// option: comment this out and comment in
	// the fairness variant below; slightlyl slower
	// but statistically fairer in terms of
	// who wins the version tie-breaking after each
	// version number increment.
	if a.CoordID < b.CoordID {
		return -1
	}
	// Per the paper discussion on fairness, we implement a
	// fairness concerned variant: a keyed crypto hash
	// of the CoordID using the version as the key.
	// At each version, the winner will be different,
	// but is still locally computable and globally consistent.
	//
	// var key [32]byte
	// binary.LittleEndian.PutUint64(key[:], uint64(a.Version))
	//
	// h := blake3.New(64, key[:])
	// h.Write([]byte(a.CoordID))
	// aHash := h.Sum(nil)
	// h.Reset()
	// h.Write([]byte(b.CoordID))
	// bHash := h.Sum(nil)
	// if bytes.Compare(aHash, bHash) <= 0 {
	// 	return -1
	// }
	return 1
}

type INV struct {
	FromID   string `zid:"0"` // methinks should correspond to TicketID
	Key      Key    `zid:"1"`
	EpochID  int64  `zid:"2"`
	TS       TS     `zid:"3"`
	IsRMW    bool   `zid:"4"`
	Val      Val    `zid:"5"`
	TicketID string `zid:"6"` // methinks should correspond to FromID
}

type ACK struct {
	FromID   string `zid:"0"`
	Key      Key    `zid:"1"`
	EpochID  int64  `zid:"2"`
	TS       TS     `zid:"3"`
	TicketID string `zid:"4"`
	Val      Val    `zid:"5"` // for debugging only, TODO remove.
}

type VALIDATE struct {
	FromID   string `zid:"0"`
	Key      Key    `zid:"1"`
	EpochID  int64  `zid:"2"`
	TS       TS     `zid:"3"`
	TicketID string `zid:"4"`
	Val      Val    `zid:"5"` // for debugging only, TODO remove.
}

func (i *INV) String() string {
	return fmt.Sprintf(`INV{
      FromID: %v,
         Key: %v,
     EpochID: %v,
          TS: %v,
       IsRMW: %v,
         Val: %v,
    TicketID: %v,
}`, rpc.AliasDecode(i.FromID), string(i.Key), i.EpochID,
		i.TS.String(), i.IsRMW, string(i.Val), i.TicketID)
}

func (a *ACK) String() string {
	return fmt.Sprintf(`ACK{
      FromID: %v,
         Key: %v,
     EpochID: %v,
          TS: %v,
         Val: %v, 
    TicketID: %v,
}`, rpc.AliasDecode(a.FromID), string(a.Key), a.EpochID,
		a.TS.String(), string(a.Val), a.TicketID) // TODO remove Val
}

func (v *VALIDATE) String() string {
	return fmt.Sprintf(`VALIDATE{
      FromID: %v,
         Key: %v,
     EpochID: %v,
          TS: %v,
         Val: %v,
    TicketID: %v,
}`, rpc.AliasDecode(v.FromID), string(v.Key), v.EpochID,
		v.TS.String(), string(v.Val), v.TicketID) // TODO remove Val
}

const (
	INVmsg      int = 1
	VALIDATEmsg int = 2
	ACKmsg      int = 3
)

func init() {
	rpc.FragOpRegister(INVmsg, "INVmsg")
	rpc.FragOpRegister(VALIDATEmsg, "VALIDATEmsg")
	rpc.FragOpRegister(ACKmsg, "ACKmsg")
}

// also resets mlt on the pendingUpdate.
func (s *HermesNode) actionAbRecordPending(tkt *HermesTicket) {
	if s == nil {
		panic("why is s our *HermesNode nil?")
	}
	if tkt == nil {
		panic("why is tkt nil?")
	}
	vv("%v top of actionAbRecordPending(tkt='%v')", s.me, tkt)
	if tkt == nil || tkt.keym == nil {
		if !tkt.PseudoTicketForAckAccum {
			panicf("tkt.keym was nil where? tkt='%v'", tkt)
		}
	}
	vv("%v top of actionAbRecordPending: (keym='%v', op='%v', val='%v', fromID='%v')", s.me, tkt.keym, tkt.Op, string(tkt.Val), rpc.AliasDecode(tkt.FromID))

	// buffer Op
	messageLossTimeout := time.Now().Add(s.cfg.MessageLossTimeout)

	// we might have an existing entry we should just be
	// updating, rather than adding to the timeoutPQ.
	item, ok := s.tkt2item[tkt.TicketID]
	if ok {
		if item.tkt != tkt {
			panic("internal logic error, item.value must equal tkt")
		}
		if item.tkt.PseudoTicketForAckAccum {
			// do these count?? is what we are having problem with.
		}
		vv("%v actionAbRecordPending is updating deadline in pq, not "+
			"adding anew, for tkt='%v'", s.me, tkt)
		item.tkt.messageLossTimeout = messageLossTimeout
		// will delete and re-add item from timeoutPQ.
		err := s.timeoutPQ.update(item, tkt, messageLossTimeout) // panic: error on pqTime.update(): item not found!
		if err != nil {
			vv("queried s.tkt2item[tkt.TicketID='%v'] -> item.priority='%v' but then not item not found in s.timeoutPQ, err = '%v'", tkt.TicketID, item.priority, err)
			panic("how to fix?")
		} else {
			vv("%v actionAbRecordPending after updating deadline, tkt='%v'",
				s.me, item.tkt)
			return
		}
	}

	tkt.messageLossTimeout = messageLossTimeout
	item = s.timeoutPQ.add(messageLossTimeout, tkt)

	// _only_ write to tkt2item at the moment. key2item and timeoutPQ are also set
	// in this execution path.
	s.tkt2item[tkt.TicketID] = item
	prior, ok := s.key2items[tkt.Key]
	if !ok {
		prior = &pqitems{}
		s.key2items[tkt.Key] = prior
	}
	prior.slc = append(prior.slc, item) // and item can be used to update/delete from pq
	s.buffered = append(s.buffered, item)
}

// must be called _before_ actionI() b/c actionI() will overwrite
// the lastWriterID (and thus not let us save any messages/return early).
func (s *HermesNode) releaseDiscardedWriter(keym *keyMeta) {
	// optimization: release any prior waiting lastWriter...
	// "The Transient state indicates a coordinator with a
	// pending write that got invalidated. While not required,
	// the Transient state is useful for tracking when
	// the coordinator's original write completes,
	// hence allowing the coordinator
	// to notify the client of the write's completion." (p205-6 footnote 7)

	// "[O1] Eliminating unnecessary validations When the
	// coordinator of a write gathers all of its ACKs but discovers a
	// concurrent write to the same key with a higher timestamp
	// (i.e., was in the transient sInvalidWR state), it does not need to broadcast VAL
	// messages (Coordinator_VAL), thus saving valuable network bandwidth.

	// Me: since the transition into InvalidWR blows away
	// the old TS with a newer one, the acks would just be
	// dropped and the VAL would never be sent for a write.
	// But how about an interrupted replay (recovery action?)
	// Hmm...

	if keym.lastWriterID == "" {
		return
	}
	pqitems, ok := s.key2items[keym.key]
	if !ok {
		return
	}
	for _, it := range pqitems.slc {
		tkt2 := it.tkt
		// we only release blocked writers because blocked
		// readers can still complete when the new value is fully ACKed.
		if (tkt2.Op == WRITE || tkt2.Op == RMW) && tkt2.FromID == keym.lastWriterID {
			if 0 == keym.TS.Compare(&tkt2.TS) {

				// great, we have our useless write in progress
				// that we can discard.

				s.deleteTicket(tkt2.TicketID, true)
				vv("%v actionI optimization to release "+
					"discarded writer is about to tkt.Done.Close() on tkt='%v'",
					s.me, tkt2.String())
				tkt2.Err = ErrOverWrit
				tkt2.Done.Close()
				// in-flight ACKs will just be discarded, since
				// the TS is about to advance to the new one from
				// the over-writing INV.
			}
		}
	}
}

func (s *HermesNode) actionI(inv *INV, keym *keyMeta) {
	if inv.FromID == s.PeerID {
		panic("should never happen, sending an inv to ourselves")
	}
	// "...the follower transitions the key
	// to the Invalid state and updates the key’s local timestamp
	// (both its version and cid) and value."

	// updateTS & last writer ID"
	keym.TS = inv.TS
	keym.val = inv.Val
	keym.lastWriterID = inv.FromID

	if useBcastAckOptimization {
		if s.cfg.ReplicationDegree <= 2 {
			// we know other guy is trying write, so with
			// only two nodes, we are done.
			vv("%v actionI: setting sValid; keym='%v'", s.me, keym) // seen 2x, good.
			// still have to apply the inv value.

			keym.state = sValid
			s.unblockReadsFor(keym)
		} else {
			tkt, ok := s.getTicket(inv.TicketID)
			if !ok {
				// with O3 optimization broadcasting ACKS, we will often get an ack for a
				// ticket that is not our own (or it might come before the INV due to
				// network re-ordering).
				// Don't freak, just make a new Ticket to
				// accumulate acks on, so we can go faster (one net hop instead of two
				// to a valid read).
				tkt = s.NewHermesTicket(WRITE, keym.key, nil, inv.FromID, 0)
				tkt.TicketID = inv.TicketID // match the sender
				tkt.PseudoTicketForAckAccum = true
				tkt.Val = inv.Val
				tkt.TS = inv.TS
				tkt.keym = keym
				//tkt.ackVector[ack.FromID] = true
				// Q: what if the ack arrives before the inv? due to network message reordering.
				// Q: do we want this in the PQ? probably...arg. this is pretty complicated,
				// trying to "fake" it with this pseudo ticket.
				s.actionAbRecordPending(tkt)
			} else {
				if tkt.PseudoTicketForAckAccum {
					// if ACK came before INV, the pseudo ticket will need these,
					// since the ACK will not have Val when we remove the debugging convenience.
					tkt.Val = inv.Val
					tkt.TS = inv.TS
					tkt.keym = keym
				}
			}
			tkt.ackVector[inv.FromID] = true
		}
	}
}

// only called by write requests.
func (s *HermesNode) actionW(tkt *HermesTicket, key Key, keym *keyMeta, val Val, fromID string, isRMW bool) {

	// "Coord_TS: When a coordinator issues an update, the version of
	// the logical timestamp is incremented by one if the update
	// is an RMW and by two if it is a write."
	if isRMW {
		keym.TS.Version++
	}

	// TS++
	keym.TS.Version++
	keym.TS.CoordID = s.PeerID // we are the coordinator
	// update last writer ID
	keym.lastWriterID = fromID
	tkt.TS = keym.TS // must update version on tkt too, as it caches/communicates TS.

	// reset ack bitmap
	tkt.ackVector = make(map[string]bool)
}

// actionBR "buffer the read" is only called when the coordinator has failed.
// Calls actionAbRecordPending(tkt) so it resets mlt on the pendingUpdate
// as well as clearing the ackVector.
func (s *HermesNode) actionBR(tkt *HermesTicket) {
	// "buffer the read, same as actionW, but without incrementing TS"

	s.actionAbRecordPending(tkt)

	// reset ack bitmap
	tkt.ackVector = make(map[string]bool)
}

func (s *HermesNode) anyWritesInProgressFor(key Key) (writers, readers []*HermesTicket) {
	items, ok := s.key2items[key]
	if !ok {
		return // false, nil
	}
	for _, it := range items.slc {
		tkt := it.tkt
		if tkt.Op == WRITE || tkt.Op == RMW {
			writers = append(writers, tkt)
		} else {
			readers = append(readers, tkt)
		}
	}
	return
}

// PRE: ack.TS.Compare(&keym.TS) == 0,
// so we know this ack is for the most recent
// write
func (s *HermesNode) actionLA(ack *ACK, keym *keyMeta) (isLast, isWrite bool, tkt *HermesTicket) {
	vv("%v top of actionLA, ack='%v'; keym='%v'", s.me, ack, keym)
	// set bit in ack bitmap

	// find the pending update
	item, ok := s.tkt2item[ack.TicketID]
	if !ok {
		if useBcastAckOptimization {
		} else {
			panic(fmt.Sprintf("what here? we are in actionLA(): why is not ack.TicketID='%v' found in our tkt2item index, at node '%v'; the ACK='%v'; tkt2item='%#v'", ack.TicketID, s.me, ack, s.tkt2item))
		}
	} else {
		tkt = item.tkt
	}
	tkt.ackVector[ack.FromID] = true

	if useBcastAckOptimization {
		// the initial INV counts as the ACK from the coordinator.
		isLast = len(tkt.ackVector)+1 >= s.cfg.ReplicationDegree-1
	} else {
		isLast = len(tkt.ackVector) == s.cfg.ReplicationDegree-1
	}
	isWrite = tkt.Op == WRITE || tkt.Op == RMW
	vv("%v actionLA() returning isLast = %v; isWrite = '%v'; len(tkt.ackVector)='%v'; ReplicationDegree-1='%v'", s.me, isLast, isWrite, len(tkt.ackVector), s.cfg.ReplicationDegree-1)
	return
}

// resume read: keym.key is valid (or not if recoverying)
func (s *HermesNode) actionRR(keym *keyMeta, ticketID string) {
	tkt := s.deleteTicket(ticketID, false)
	if tkt != nil {
		vv("%v actionRR about to tkt.Done.Close() on tkt='%v'", s.me, tkt.String())
		// we can finally apply the value.
		tkt.Val = keym.val
		tkt.TS = keym.TS
		tkt.Done.Close()

		if s.nextWakeTicket == tkt {
			s.nextWakeTicket = nil
			s.nextWakeCh = nil
			s.nextWake = time.Time{}
			s.setWakeup()
		}
	}
	// any other reads for this key need resuming?
	s.unblockReadsFor(keym)
}

type HermesKeyState int

const (
	// stable
	sValid   HermesKeyState = 1
	sInvalid HermesKeyState = 2
	sWrite   HermesKeyState = 3
	sReplay  HermesKeyState = 4

	// unstable/transient state:
	sInvalidWR HermesKeyState = 5
)

func stateString(state HermesKeyState) string {
	switch state {
	case sValid:
		return "sValid"
	case sInvalid:
		return "sInvalid"
	case sWrite:
		return "sWrite"
	case sReplay:
		return "sReplay"
	case sInvalidWR:
		return "sInvalidWR"
	}
	panic(fmt.Sprintf("unknown state %v", int(state)))
}

// per key meta data
type keyMeta struct {
	key          Key
	TS           TS   `zid:"0"`
	IsRMW        bool `zid:"1"` // accommodates update-replays.
	state        HermesKeyState
	lastWriterID string
	val          Val
}

func (k *keyMeta) String() (s string) {
	return fmt.Sprintf(`keyMeta{
	key         : %v,
	TS          : %v,
	IsRMW       : %v,
	state       : %v,
	lastWriterID: %v,
	val         : %v,
}
`, string(k.key), k.TS.String(), k.IsRMW,
		stateString(k.state), rpc.AliasDecode(k.lastWriterID), string(k.val))
}

type pqitems struct {
	slc []*pqTimeItem
}

var ErrKeyNotFound = fmt.Errorf("error key not found")
var ErrShutDown = fmt.Errorf("error shutting down")
var ErrTimeOut = fmt.Errorf("error timeout")
var ErrOverWrit = fmt.Errorf("error write was over-written by a higher TS write")
var ErrAbortRMW = fmt.Errorf("error RMW was aborted by a higher TS write")

func (s *HermesNode) bcastInval(key Key, inv *INV) {
	vv("%v top of bcastInval: inv: '%v'", s.me, inv)

	frag := s.myPeer.NewFragment()
	bts, err := inv.MarshalMsg(nil)
	panicOn(err)
	frag.Payload = bts
	frag.FragOp = INVmsg
	j := 0
	for id, ckt := range s.ckt {
		_ = id
		_, err = ckt.SendOneWay(frag, 0, 0)
		_ = err // don't panic on halting
		//panicOn(err)
		if err == nil {
			vv("%v sent INV (j=%v of %v) to id='%v'; INV='%v", s.me, j, len(s.ckt), rpc.AliasDecode(id), inv)
		}
		j++
	}
}

/* modified ack() instead.
func (s *HermesNode) bcastAck(ack *ACK) {
	vv("%v top of bcastAck(ack='%v')", s.me, ack)
	frag := s.myPeer.U.NewFragment()
	bts, err := ack.MarshalMsg(nil)
	panicOn(err)
	frag.Payload = bts
	frag.FragOp = ACKmsg
	for id, ckt := range s.ckt {
		_ = id
		err = ckt.SendOneWay(frag, -1)
		_ = err // don't panic on halting.
		if err != nil {
			alwaysPrintf("non nil error '%v' on bcast/sending ACKmsg", err)
		}
	}
}
*/

func (s *HermesNode) bcastValid(valid *VALIDATE) {
	vv("%v top of bcastValid(valid='%v')", s.me, valid)
	frag := s.myPeer.NewFragment()
	bts, err := valid.MarshalMsg(nil)
	panicOn(err)
	frag.Payload = bts
	frag.FragOp = VALIDATEmsg
	for id, ckt := range s.ckt {
		_ = id
		_, err = ckt.SendOneWay(frag, -1, 0)
		_ = err // don't panic on halting.
		if err != nil {
			alwaysPrintf("non nil error '%v' on sending VALIDATEmsg", err)
		}
	}
}

// "A read request is serviced on an operational replica
// (i.e., one with an RM lease) by returning the local value of
// the requested key if it is in the Valid state.
// If the key is in any other state, the request is stalled."
func (s *HermesNode) readReq(tkt *HermesTicket) (val Val, err error) {
	key := tkt.Key
	readFromID := tkt.FromID
	waitForValid := tkt.waitForValid

	vv("%v readReq(key='%v', readFromID='%v', waitForValid='%v')", s.me, string(key), rpc.AliasDecode(readFromID), waitForValid)
	var ok bool
	var keym *keyMeta
	defer func() {
		vv("%v readReq returning(key='%v', readFromID='%v'): val='%v'; err='%v'; keym='%v'", s.me, string(key), rpc.AliasDecode(readFromID), string(val), err, keym)

		vv("if we aren't finished, get a wake up call to check for follower failure.")
		if !tkt.Done.IsClosed() {
			s.setWakeup()
		}
	}()

	keym, ok = s.store[key]
	if !ok {
		// are we a single node?
		if s.cfg.ReplicationDegree == 1 {
			// certainly.
			tkt.Err = ErrKeyNotFound
			tkt.Done.Close()
			return nil, tkt.Err
		}
		// Q: how to have the client tell us they want to wait for it?
		// A: that is what tkt.waitForValid is for
		if tkt.waitForValid < 0 {
			// they did not want to wait
			tkt.Err = ErrKeyNotFound
			tkt.Done.Close()
			return nil, tkt.Err
		}

		// first read of unknown key. make its keym
		vv("first read of unknown key '%v'", string(key))
		keym = &keyMeta{
			key: key,
			//val: tkt.Val, // this is a read, not a write.
			state: sInvalid,
			TS: TS{
				//Version: vers, // leave at 0.
				CoordID: s.PeerID,
			},
			lastWriterID: tkt.FromID,
		}
		s.store[key] = keym
	}
	tkt.keym = keym
	tkt.TS = keym.TS

	switch keym.state {
	case sValid:
		// complete read request
		vv("%v sees sValid: complete read request (key='%v', readFromID='%v')", s.me, string(key), rpc.AliasDecode(readFromID))
		//s.completeRead(keym, tkt)
		s.unblockReadsFor(keym)
		tkt.Val = keym.val
		tkt.TS = keym.TS
		tkt.Done.Close()

		return keym.val, nil
	case sInvalid:
		vv("%v sees sInvalid: (key='%v', readFromID='%v')", s.me, string(key), rpc.AliasDecode(readFromID))

		now := time.Now()
		coordFailed := !tkt.messageLossTimeout.IsZero() && now.After(tkt.messageLossTimeout)

		vv("%v sees coordFailed = '%v'", s.me, coordFailed)
		if coordFailed {
			// buffer the read, "same as actionW, but without incrementing TS"
			s.actionBR(tkt)
			keym.state = sReplay
			invalidation := &INV{
				TicketID: tkt.TicketID,
				Key:      key,
				Val:      keym.val,
				TS:       keym.TS,
				EpochID:  s.EpochID,
				FromID:   s.PeerID, // should we be using keym.TS.CoordID ? tkt.FromID ?
			}
			s.bcastInval(key, invalidation)

		} else {
			s.actionAbRecordPending(tkt) // buffer the read
		}
	case sInvalidWR, sWrite, sReplay:
		s.actionAbRecordPending(tkt) // buffer the read
	}
	return
}

/*
[Section 3.1, Overview] "From the perspective of the coordinator,
once all ACKs are received, it is safe to respond to a client
because at this point, the write is guaranteed to be visible
to all live replicas, and any future read cannot return the
old value (i.e., the write is committed – Figure 2b)."
*/
// So we want to pause until we get all the acks back.
func (s *HermesNode) writeReq(tkt *HermesTicket) {
	vv("top of writeReq. key='%v'; val='%v'; writeFromID='%v'", string(tkt.Key), string(tkt.Val), rpc.AliasDecode(tkt.FromID))
	// we (s.PeerID) are the coordinator for this write!
	//
	// "A coordinator node issues a write to a key only if it is in
	//the Valid state; otherwise the write is stalled. To issue and
	//complete a write, the coordinator node:
	//• Coord_TS: Updates the key’s local timestamp by incrementing its
	//version and appending its node id as the cid, and assigns
	//this new timestamp to the write.
	//• Coord_INV: Promptly broadcasts an INV message consisting of
	//the key, the new timestamp (TS) and the value to all replicas and transitions the key to the Write state, whilst applying the new value locally.
	//• Coord_ACK: Once the coordinator receives ACKs from all the live
	//replicas, the write is completed by transitioning the key to
	//the Valid state (Invalid state if the key was in Trans state7).
	//
	// 7 The transitional state sInvalidWR indicates a coordinator
	// with a pending write that got
	// invalidated. While not required, the transitional state
	// is useful for tracking when
	// the coordinator’s original write completes, hence allowing the coordinator
	// to notify the client of the write’s completion."

	key := tkt.Key
	val := tkt.Val
	keym, ok := s.store[key]
	if !ok {
		vv("no update, just a new key/value pair")
		keym = &keyMeta{
			key: key,
			val: tkt.Val, // apply write locally
			//state: sWrite,
			state: sInvalid,
			TS: TS{
				//Version: vers, // leave at 0.
				CoordID: s.PeerID, // we are the coordinator for this write
			},
			lastWriterID: tkt.FromID,
		}
		s.store[key] = keym
		tkt.TS = keym.TS

		// are we a single node?
		if s.cfg.ReplicationDegree == 1 {
			vv("single node writing, set to valid immeditely key='%v', val='%v'", key, string(tkt.Val))
			keym.state = sValid
			tkt.Done.Close()
			return
		}
	}
	vv("%v writeReq starting state keym = '%v'", s.me, keym)
	tkt.keym = keym
	tkt.TS = keym.TS

	// check for message loss after messageLossTimeout.
	// the s.actionAbRecordPending(tkt) below will set tkt.messageLossTimeout
	defer s.setWakeup()

	// we are in writeReq
	switch keym.state {
	case sValid, sInvalid:
		// "writes in invalid state are also supported as an optimization" -- Hermes.tla
		// must be tkt.FromID here, so we can tell early on
		// if their write is finished by being overwritten
		// by another; so tkt.FromID will be stored in lastWriterID.
		s.actionW(tkt, key, keym, val, tkt.FromID, tkt.Op == RMW)
		keym.val = val // apply write locally

		// only write or rmw puts us into the sWrite state,
		// and seeing this state also means that we are on the Coordinator
		// for this write, not the follower.
		keym.state = sWrite
		invalidation := &INV{
			TicketID: tkt.TicketID,
			FromID:   s.PeerID,
			Key:      key,
			Val:      val,
			TS:       keym.TS,
			EpochID:  s.EpochID,
			IsRMW:    false,
		}

		// "Note that the coordinator waits for ACKs only from the live
		// replicas as indicated in the membership variable. If a
		// follower fails after an INV has been sent, the coordinator waits
		// for the ACK from the failed node until the membership is
		// reliably updated (after the node is detected as failed and the
		// membership lease expires – §2.4). Once the coordinator is
		// not missing any more ACKs, it can safely continue the write
		// [jea: by sending the VALIDATE message]."

		// jea: I added this based on 3.4
		// "To detect the loss of an INV or
		// ACK for a particular write, the coordinator of the write resets
		// the request's messageLossTimeout once it broadcasts INV messages. If the mlt
		// of a key is exceeded before its write completion, then the
		// coordinator suspects a potential message loss and resets the
		// request's mlt before retransmitting the write’s INV broadcast."

		// so it seems to me we must queue our tkt.
		s.actionAbRecordPending(tkt)

		s.bcastInval(key, invalidation)

	case sInvalidWR, sWrite, sReplay:
		s.actionAbRecordPending(tkt)
	}
	// end of writeReq
}

/*
// "Read-Modify-Writes (RMWs) in Hermes may abort (§3.6)."
// Membership-based protocols are supported by a reliable
membership (RM) [54], typically based on Vertical Paxos [67].
Vertical Paxos uses a majority-based protocol to reliably
maintain a stable membership of live nodes [96] (i.e., as
in virtual synchrony [18]), which is guarded by leases. Informally,
nodes in Vertical Paxos locally store a lease, a membership
variable and an epoch_id. Nodes are operational as long as
their lease is valid.

Messages are tagged with the epoch_id
of the sender at the time of message creation, and a receiver
drops any message tagged with a different epoch_id than its
local epoch_id.

The membership variable establishes the set
of live nodes, which allows for efficient execution of reads
and writes on any node with a valid lease. During failure-free
operation, membership leases are regularly renewed. When
a failure is suspected, the membership variable is updated
reliably (and epoch_id is incremented) through a majority
based protocol but only after the expiration of leases. This
circumvents potential false-positives of unreliable failure
detection and maintains safety under network partitions
(§3.4). Simply put, updating the membership variable only
after lease expiration ensures that unresponsive nodes have
stopped serving requests before they are removed from the
membership and new requests complete only amongst the
remaining live nodes of the updated membership group.

Section 3:
"Hermes is a reliable membership-based broadcasting protocol..."

End of Section 3.1:
"As a membership-based protocol, Hermes is aided by RM to
provide a stable group membership of live nodes in the
face of failures and network partitions."

Resilience to network faults and RMWs are described in §3.4 and §3.6, respectively.

func (s *HermesNode) readModifyWriteReq(key Key, val Val, fromID string) {

	keym, ok := s.store[key]
	if !ok {
		// no update, just a new key/value pair. Q: treat as valid?
		keym = &keyMeta{
            IsRMW: true,
			key:   key,
			val:   val,
			state: sInvalid,
			TS: TS{
				//Version: vers,
				CoordID: s.PeerID, // Q: are we the coordinator?
			},
		}
	}
	switch keym.state {
	case sValid, sInvalid:
		s.actionW(tkt, key, keym, val, tkt.FromID, tkt.IsRMW)
		keym.state = sWrite
		invalidation := &INV{Key: key, Val: val, TS: keym.TS, EpochID: s.EpochID, IsRMW: true}
		s.bcastInval(key, invalidation)
	case sInvalidWR, sWrite, sReplay:
		s.actionAbRecordPending(keym, writer, val, fromID)
	}

}
*/

// initially we see alot more replays happening with this on.
const useBcastAckOptimization = true // now 30.5 sec. was 65 seconds for test suite. 004 hung w/o nextWake
//const useBcastAckOptimization = false // 22.5 seconds for test suite.

func (s *HermesNode) ack(inv *INV) {
	vv("%v top of ack(inv:'%v')", s.me, inv)
	// "F_ACK: Irrespective of the result of the
	// timestamp comparison, a follower always
	// responds with an ACK containing
	// the same timestamp as that in the INV
	// message of the write."
	a := &ACK{
		FromID:   s.PeerID, // this has to be us, so the ackVector can see all the different ACKers.
		Key:      inv.Key,
		TS:       inv.TS,
		EpochID:  s.EpochID,
		Val:      inv.Val, // TODO remove
		TicketID: inv.TicketID,
	}
	frag := s.myPeer.NewFragment()
	bts, err := a.MarshalMsg(nil)
	panicOn(err)
	frag.Payload = bts
	frag.FragOp = ACKmsg

	if useBcastAckOptimization {
		// [O3] Reducing blocking latency In the failure-free case,
		// and during a write to a key, followers block reads to that
		// key for up to a round-trip (§3.1). This blocking latency can
		// be reduced to a half round-trip if followers broadcast ACKs
		// to all replicas instead of just responding to the coordinator
		// of the write (FACK). Once all ACKs have been received by a
		// follower, it can service the reads to that key without waiting
		// for the VAL message. While this optimization increases the
		// number of ACKs, the actual bandwidth cost is minimal as
		// ACK messages have a small constant size. The bandwidth
		// cost is further offset by avoiding the need to broadcast VAL
		// messages. Thus, under the typical small replication degrees,
		// this optimization comes at negligible cost in bandwidth.
		for id, ckt := range s.ckt {
			_ = id
			_, err = ckt.SendOneWay(frag, -1, 0)
			_ = err // don't panic on halting.
			if err != nil {
				alwaysPrintf("non nil error '%v' on bcast/sending ACKmsg to '%v'", err, id)
			}
		}
	} else {

		ckt, ok := s.ckt[inv.FromID]
		if !ok {
			panic(fmt.Sprintf("at '%v', no ckt avail for '%v'", s.me, rpc.AliasDecode(inv.FromID)))
		}
		_, err = ckt.SendOneWay(frag, -1, 0)
		_ = err // don't panic on halting
	}
}

// received by followers
func (s *HermesNode) recvInvalidate(inv *INV) (err error) {

	// Already implemented in node.go filtering, so won't
	// be delivered here.
	// "During this transition period, any live follower that has
	// not yet received the latest m-update will simply drop the
	// INV messages, because those messages are tagged with an
	// epoch_id greater than the follower’s local epoch_id."
	// => not needed:
	//if inv.EpochID != s.EpochID {
	//	return nil
	//}

	key := inv.Key
	keym, ok := s.store[key]
	vv("%v top of recvInvalidate, inv:'%v'; ok='%v' (false => make new keym)", s.me, inv, ok)
	if !ok {
		// this does the same as actionI()
		keym = &keyMeta{
			key:          key,
			val:          inv.Val,
			TS:           inv.TS, // save for replay on recovery.
			lastWriterID: inv.FromID,
			IsRMW:        inv.IsRMW,
			state:        sInvalid,
		}
		s.store[key] = keym
		// we could just s.ack(inv) and return, but instead we
		// do a little bit of redundant work below to keep
		// the code paths uniform and more easily debugable.
	}

	// "• F_INV: Upon receiving an INV message, a follower compares
	// the timestamp from the incoming message to its local
	// timestamp of the key. If the received timestamp is higher
	// than the local timestamp, the follower transitions the key
	// to the Invalid state (Transient [sInvalidWR] state if the key was in the Write
	// or the Replay state) and updates the key's local timestamp
	// (both its version and cid) and value.
	//
	// • F_ACK: Irrespective of the result of the timestamp comparison,
	// a follower always responds with an ACK containing
	// the same timestamp as that in the INV message of the write."

	compare := inv.TS.Compare(&keym.TS)
	vv("%v recvInvalidate compare = %v; keym.state = '%v'", s.me, compare, stateString(keym.state))

	if inv.IsRMW {
		if compare >= 0 {
			// Section 3.6:
			// "Follower_RMW-ACK: A follower ACKs an INV message for an RMW
			// only if its timestamp is equal to or higher than the local
			// one; otherwise, the follower responds with an INV based
			// on its local state (i.e., same message used for write replay)"
			s.ack(inv)
			// anything else?
			return
		} else {
			// send INV... as described above...? how to?
		}
	}

	if keym.state != sValid {

		// "• Coord_RMW-abort: In contrast to non-conflicting writes, an RMW
		// with pending ACKs is aborted if its coordinator receives
		// an INV to the same key with a higher timestamp." (either Write OR RMW).
		//
		// me: pending ACKs means we are in any state except sValid.
		//
		// Section 3.6:
		// For this reason, an RMW update in Hermes is executed
		// similarly to a write, but it is conflicting. Hermes may abort
		// an RMW which is concurrently executed with another update
		// operation (either a write or another RMW) to the same key.
		//    Hermes commits an RMW if and only if the RMW has the
		// highest timestamp amongst any concurrent updates to that
		// key. Moreover, it purposefully assigns higher timestamps
		// to writes compared to their concurrent RMWs. As a result,
		// any write racing with an RMW to a given key is guaranteed
		// to have a higher timestamp, thus safely aborting the RMW.
		//   Meanwhile, if only RMW updates are racing, the RMW with
		// the highest node id will commit, and the rest will abort.
		//   More formally, Hermes always maintains safety and
		// guarantees progress in the absence of faults by ensuring two
		// properties: (1) writes always commit, and (2) at most one of
		// possible concurrent RMWs to a key commits.
		pqitems, ok := s.key2items[keym.key]
		if ok {
			// any tkt still in key2items has pending ACKS, else it
			// would have been removed by when they were all received.

			// copy as we'll modify pqitems.slc and we might do > 1 here.
			slc := append([]*pqTimeItem{}, pqitems.slc...)
			for _, it := range slc {
				tkt := it.tkt
				if tkt.Op == RMW && inv.TS.Compare(&tkt.TS) > 0 {
					tkt.Err = ErrAbortRMW
					s.deleteTicket(tkt.TicketID, true)
					tkt.Done.Close()
				}
			}
			// continue or return here?
		}
	}
	switch compare {
	case 0:
		s.actionI(inv, keym)
		s.ack(inv)
	case -1:
		s.ack(inv)
	case 1: // incoming INV for write with higher TS than ours.
		switch keym.state {
		case sValid:
			s.actionI(inv, keym)
			keym.state = sInvalid
			s.ack(inv)
		case sInvalid, sInvalidWR:
			s.actionI(inv, keym)
			s.ack(inv)
		case sWrite, sReplay:
			// sReplay is just smaller-scoped sWrite, where
			// the node acts like a coordinator and tries
			// to get unblocked acting like a writer, right?
			// But I think we go-into sReplay in response to...
			// a timeout from a write not getting all its ACKs,
			// and it starts by sending out new round of INV
			// without changing the keym.TS. So there will
			// be a stalled original writer on a different
			// coordinator (that might be crashed).

			// Since the old writer's value is going to be
			// discarded anyway, can we just respond to the
			// to-be-discarded writer/caller now, and not make them wait any longer?
			// I think they are in the keym.lastWriterID ? yes,
			// they are, so we must do this before the actionI
			// obliterates it. This could be a mini replay or a full writer.
			s.releaseDiscardedWriter(keym)
			// buffered readers can still wait until
			// the new write completes and get that value.
			// Since we do that, the reader(s) still need
			// need resuming from InvalidWR, and of course
			// the originating writer but they will always
			// be on a different node (and be in sWrite)
			// rather than on a follower (only followers
			// can get into sInvalidWR, b/c coordinators
			// never send INV to themselves).
			s.actionI(inv, keym)
			// "The Transient state indicates a coordinator with a
			// pending write that got invalidated. While not required,
			// the Transient state is useful for tracking when
			// the coordinator’s original write completes,
			// hence allowing the coordinator
			// to notify the client of the write's completion.
			// This is the only place we go into sInvalidWR.
			keym.state = sInvalidWR // == "Transient" state
			s.ack(inv)
		}
	}
	if useBcastAckOptimization {
		if s.cfg.ReplicationDegree <= 2 {
			// we know other guy is trying write, so with
			// only two nodes, we are done.
			vv("%v recvInvalid: setting sValid; keym='%v'", s.me, keym)
			// still have to apply the inv value.
			keym.state = sValid
			s.unblockReadsFor(keym)
		}
	}
	return
}

// send the response to the write requestor.
func (s *HermesNode) completeWrite(keym *keyMeta, ticketID string) {

	if ticketID != "" {
		// find the pending writes for this ticket
		item, ok := s.tkt2item[ticketID]
		if !ok {
			panic(fmt.Sprintf("no pending write for ticketID='%v', key='%v'", ticketID, string(keym.key)))
			return
		}
		tkt := item.tkt

		// completing the write
		keym.val = tkt.Val
		keym.TS = tkt.TS
		keym.lastWriterID = tkt.FromID

		s.deleteTicket(tkt.TicketID, true)
		//if the write was started locally, respond to any waiting writers.
		vv("%v completeWrite is about to tkt.Done.Close() on tkt='%v'", s.me, tkt.String())
		tkt.Done.Close()

		if s.nextWakeTicket == tkt {
			s.nextWakeTicket = nil
			s.nextWakeCh = nil
			s.nextWake = time.Time{}
			s.setWakeup()
		}
	} // end if ticketID != ""

	// all readers waiting on this key also need to get unblocked
	s.unblockReadsFor(keym)
	//for _, it := range readers {
	//	tkt2 := it.value
	//	s.completeRead(keym, tkt2)
	//}
}

/* replace by unblockReadsFor()
func (s *HermesNode) completeRead2(keym *keyMeta, tkt *HermesTicket) {
	if tkt != nil {
		vv("top of completeRead() for key '%v', tkt.TicketID='%v'", string(keym.key), tkt.TicketID)
		tkt.Val = keym.val
		tkt.TS = keym.TS
		s.deleteTicket(tkt.TicketID, false)

		vv("%v completeRead is about to tkt.Done.Close() on tkt='%v'", s.me, tkt.String())
		tkt.Done.Close()
		// continue below to check for other reads needing unblocking...
	} else {
		vv("top of completeRead() for key '%v', tkt=nil", string(keym.key))
	}
	s.unblockReadsFor(keym)
}
*/

func (s *HermesNode) unblockReadsFor(keym *keyMeta) {
	vv("%v unblockReadsFor('%v') top", s.me, string(keym.key))

	pqitems, ok := s.key2items[keym.key]
	if !ok {
		return
	}
	vv("In unblockReadsFor('%v'): %v other items to maybe complete if they are reads", string(keym.key), len(pqitems.slc))
	// filter out the writes
	// copy first since it will change as we delete Tickets.
	slc := append([]*pqTimeItem{}, pqitems.slc...)
	for _, it := range slc {
		tkt := it.tkt
		if tkt.Op != WRITE && tkt.Op != RMW {
			tkt.Val = keym.val
			tkt.TS = keym.TS
			s.deleteTicket(tkt.TicketID, false)

			if s.nextWakeTicket == tkt {
				s.nextWakeTicket = nil
			}

			vv("%v unblockReadsFor('%v') about to tkt.Done.Close() on tkt='%v'", s.me, string(keym.key), tkt.String())
			tkt.Done.Close()
		}
	}
	if s.nextWakeTicket == nil {
		s.nextWakeCh = nil
		s.nextWake = time.Time{}
		s.setWakeup()
	}
}

func (s *HermesNode) deleteTicket(ticketID string, wasWrite bool) *HermesTicket {
	vv("top of deleteTicket for ticketID='%v'", ticketID)
	item, ok := s.tkt2item[ticketID]
	if !ok {

		if wasWrite {
			panic(fmt.Sprintf("what here? we are in deleteTicket(): why is not ticketID='%v' found in our tkt2item index, at node '%v'; tkt2item='%#v'; wasWrite=%v", ticketID, s.me, s.tkt2item, wasWrite))
		}
		//vv("no ticketID '%v' ticket found. reads may just be already valid and not have needed a ticket (maybe?). len(s.buffered) = %v", ticketID, len(s.buffered))
		// debug...
		//for i, b := range s.buffered {
		//	tkt := b.value
		//	vv("buffered ticket[%v] has key '%v' (Val '%v'): '%#v' (deadline: '%v' in '%v')", i, string(tkt.Key), string(tkt.Val), tkt, b.priority, b.priority.Sub(time.Now()))
		//}
		return nil
	}
	tkt := item.tkt
	// only delete from tkt2item. also deletes from key2items and timeoutPQ.
	delete(s.tkt2item, ticketID)
	pqitems, ok := s.key2items[tkt.Key]
	if ok {
		for i, it := range pqitems.slc {
			if it == item {
				pqitems.slc = append(pqitems.slc[:i], pqitems.slc[i+1:]...)
				if len(pqitems.slc) == 0 {
					delete(s.key2items, tkt.Key)
				} else {
					vv("key '%v' still has %v queued tickets: '%#v'", string(tkt.Key), len(pqitems.slc), pqitems.slc)
				}
				break
			}
		}
	}
	pqSize := s.timeoutPQ.size()
	if pqSize > 0 {
		s.timeoutPQ.delOneItem(item)
	}
	// adjust the wakeup if we are deleting the top of the PQ.
	if s.nextWakeTicket == tkt {
		s.nextWakeCh = nil
		s.nextWakeTicket = nil
		s.nextWake = time.Time{}
		s.setWakeup()
	}

	for i, it := range s.buffered {
		if it == item {
			s.buffered = append(s.buffered[:i], s.buffered[i+1:]...)
			break
		}
	}
	return tkt
}

func (s *HermesNode) recvAck(ack *ACK) (err error) {

	var tkt *HermesTicket
	vv("%v recvAck(ack='%v')", s.me, ack)
	key := ack.Key
	keym, ok := s.store[key]
	vv("%v recvAck ok = '%v', for key='%v': ack='%v'", s.me, ok, string(key), ack)
	if !ok {
		// no keym, yet. should we be making one?
		if !useBcastAckOptimization {
			return ErrKeyNotFound
		}
		// due to network timing issues, the
		// bcast ACK could arrive before the bcast INV.
		// Go ahead and save the ACK on a ticket...

		// with O3 optimization broadcasting ACKS, we will often get an ack for a
		// ticket that is not our own. Don't freak, just make a new Ticket to
		// accumulate acks on, so we can O3: go faster (one net hop instead of two
		// to a valid read).

		tkt, ok = s.getTicket(ack.TicketID)
		if !ok {
			tkt = s.NewHermesTicket(WRITE, key, nil, ack.FromID, 0)
			tkt.TicketID = ack.TicketID // match the sender
			tkt.PseudoTicketForAckAccum = true
			//tkt.Val = inv.Val // not avail yet. save when the INV comes in.
			tkt.TS = ack.TS
			//tkt.keym = keym // not available yet.
			s.actionAbRecordPending(tkt)
		}
		tkt.ackVector[ack.FromID] = true
		return nil // no keym yet, nothing below to do.
	}
	// "[O1] Eliminating unnecessary validations When the
	// coordinator of a write gathers all of its ACKs but discovers a
	// concurrent write to the same key with a higher timestamp
	// (i.e., was in the transient sInvalidWR state), it does not need to broadcast VAL
	// messages (Coordinator_VAL), thus saving valuable network bandwidth.

	cmp := ack.TS.Compare(&keym.TS)
	if useBcastAckOptimization {
		// INVAR: have keym

		// due to network timing issues, the
		// bcast ACK could arrive before the bcast INV.
		// Go ahead and save the ACK on a ticket...
		tkt, ok := s.getTicket(ack.TicketID)

		if ok {
			vv("%v recvAck cmp='%v'; tkt[ack.TicketID]='%v'", s.me, cmp, tkt)
			if cmp < 0 {
				// ack too old
				vv("%v recvAck ack too old! delete it. cmp='%v'; tkt[ack.TicketID]='%v'", s.me, cmp, tkt)
				s.deleteTicket(tkt.TicketID, false)
				return
			}
		} else {
			vv("%v recvAck has no tkt for ack", s.me)
		}

		if !ok {
			// with O3 optimization broadcasting ACKS, we will often get an ack for a
			// ticket that is not our own. Don't freak, just make a new Ticket to
			// accumulate acks on, so we can go faster (one net hop instead of two
			// to a valid read).
			tkt = s.NewHermesTicket(WRITE, keym.key, nil, ack.FromID, 0)
			tkt.TicketID = ack.TicketID // match the sender
			tkt.PseudoTicketForAckAccum = true
			//tkt.Val = ack.Val // will be removed from ack at some point.
			tkt.TS = ack.TS
			tkt.keym = keym
			s.actionAbRecordPending(tkt)
		}
		tkt.ackVector[ack.FromID] = true
		if tkt.Val != nil && useBcastAckOptimization {
			if s.cfg.ReplicationDegree <= 2 {
				// we know other guy is trying write, so with
				// only two nodes, we are done.
				vv("%v recvAck: setting sValid; keym='%v'", s.me, keym)
				// still have to apply the inv value.

				keym.state = sValid
				s.completeWrite(keym, tkt.TicketID)
				//s.unblockReadsFor(keym)
				return
			}
		} // else have to wait for Val in INV.
	}
	vv("%v recvAck cmp = '%v', keym='%v'; ack='%v'", s.me, cmp, keym, ack)
	// won't find ticket... if that write already completed.

	// in recvAck
	if cmp != 0 {
		// is this correct? yes. We only want
		// to count ACKs for the most recent write;
		// ACKs for discarded writes should just be dropped.
		return
	}
	// we are in recvAck
	vv("%v recvAck state='%v', ack='%v'; keym='%v'", s.me, stateString(keym.state), ack, keym)
	switch keym.state {
	case sValid, sInvalid:
		// ignored, unless...
		if keym.state == sInvalid && useBcastAckOptimization {
			// same as sInvaliWR
			lastAck, isWrite, tkt2 := s.actionLA(ack, keym)
			if lastAck {
				vv("%v last ack seen for useBcastAckOptimization in sInvalid", s.me) // not seen

				// keym has the value, but pseudo tkt does not... and
				// completeWrite will over-write keym.val with txt.val, so:
				var ticketID string
				if tkt2 != nil {
					ticketID = tkt2.TicketID
					tkt2.TS = keym.TS
					tkt2.Val = keym.val
				}
				keym.state = sValid // TODO: want this here? its not in the transition table.
				if isWrite {
					s.completeWrite(keym, ticketID)
				} else {
					s.unblockReadsFor(keym)
				}
			}
		}
	case sInvalidWR:
		// does actionLA lookup by ack.TicketID? yes. That
		// is the ticket referenced.
		lastAck, isWrite, _ := s.actionLA(ack, keym)
		if lastAck {
			keym.state = sValid // TODO: want this here? its not in the transition table.
			if isWrite {
				// since the ack.TS matches the keym.TS, this is
				// the last ack over the over-writing most recent write.
				s.completeWrite(keym, ack.TicketID)
			} else {
				// TODO: keym.state = sInvalid // why this? it is in the transition
				// table but makes no sense:
				// after we finish re-playing (a recovery action)
				// and then have another later write come in on top of
				// of us and then that write completes...
				// we should resume the read, but mark as valid (now above).
				//s.actionRR(keym, ack.TicketID)
				s.unblockReadsFor(keym)
			}
			// Q: why don't we send out VALIDATES here, like sWrite below does?
			// A: because only a follower can get into sInvalidWR, so
			// the coordinator for the most recent INV will do that; it's
			// not our responsibility. But we have a most recent ack.TS, so...
			// we must have sent the INV in the first place... which means
			// I'm not sure how we can ever arrive here. Maybe in RMW?
		}
		// we are in recvAck
	case sWrite:
		lastAck, isWrite, _ := s.actionLA(ack, keym)
		vv("%v recvAck state sWrite, ack='%v'; lastAck='%v'; keym='%v'", s.me, ack, lastAck, keym) // node 0 only seen for 1st val 123, not for 2nd val 45; on test 006. and not for node2.
		if !isWrite {
			panic("really should be isWrite true")
		}
		if lastAck {
			keym.state = sValid
			s.completeWrite(keym, ack.TicketID)
			if !useBcastAckOptimization {
				valid := &VALIDATE{
					TicketID: ack.TicketID,

					// FromID could be s.PeerID? but methinks should correspond to TicketID
					FromID:  ack.FromID,
					Key:     key,
					EpochID: s.EpochID,
					TS:      ack.TS,  // ack.TS is equal to keym.TS, so either is fine.
					Val:     ack.Val, // TODO remove
				}
				// "Send validations once acknowledments are received from all alive nodes"
				s.bcastValid(valid)
			}
		}
		// we are in recvAck
	case sReplay:
		// if a node went down, causing a replay in
		// order to satisfy a read we will never
		// get the "last" ack... unless we have
		// an RM group membership size change.
		lastAck, isWrite, _ := s.actionLA(ack, keym)
		_ = isWrite
		// We can replay writes or reads...
		// "In contrast, the loss of a VAL message is handled by the
		// follower using a write replay."
		//
		// Also, from Hermes.tla: "[there is a possible] optimization to
		// not replay when we have gathered acks from all alive".

		// Safely replayable writes: Node and network faults during
		// a write to a key may leave the key in a permanently Invalid
		// state in some or all of the nodes. To prevent this, Hermes
		// allows any invalidated operational replica to replay the write
		// to completion without violating linearizability. This is
		// accomplished using two mechanisms. First, the new value for
		// a key is propagated to the replicas in INV messages
		// (Figure 2a). Such early value propagation guarantees that every
		// invalidated node is aware of the new value. Secondly,
		// logical timestamps enable a precise global ordering of writes in
		// each of the replicas. By combining these ideas, a node that
		// finds a key in an Invalid state for an extended period can
		// safely replay a write by taking on a coordinator role and
		// retransmitting INV messages to the replica ensemble with
		// the original timestamp (i.e., original version number and coordID),
		// hence preserving the global write order.

		if lastAck {
			keym.state = sValid
			// actionRR will apply the keym.val to tkt.Val
			//s.actionRR(keym, ack.TicketID)
			s.unblockReadsFor(keym)
			if !useBcastAckOptimization {
				valid := &VALIDATE{
					TicketID: ack.TicketID,
					FromID:   s.PeerID,
					Key:      key,
					EpochID:  s.EpochID,
					TS:       ack.TS,  // correct?
					Val:      ack.Val, // TODO remove
				}
				s.bcastValid(valid)
			}
		}
	}
	return // end of recvAck
}

func (s *HermesNode) recvValidate(v *VALIDATE) (err error) {

	key := v.Key
	keym, ok := s.store[key]
	vv("%v recvValidate(valid='%v'); keym='%v', ok='%v'", s.me, v, keym, ok)
	if !ok {
		panic("what here?")
		return ErrKeyNotFound
	}

	cmp := v.TS.Compare(&keym.TS)
	vv("%v recvValidate(valid='%v'); keym='%v', ok='%v'; cmp='%v'", s.me, v, keym, ok, cmp)
	if cmp != 0 {
		// "F_VAL: When a follower receives a VAL message,
		// it transitions the key to the Valid state if
		// and only if the received
		// timestamp is equal to the key’s local timestamp. Otherwise,
		// the VAL message is simply ignored."
		return
	}
	switch keym.state {
	case sValid:
		// nothing to do
	case sInvalid:
		keym.state = sValid
		// We known keym.TS == v.TS, but

		// now we have to resume any blocked reads, right?
		// this is not in the transition table...
		vv("seems like we should be unblocking reads here... len %v buffered='%#v'", len(s.buffered), s.buffered)
		//s.actionRR(keym, v.TicketID)
		s.unblockReadsFor(keym)

	case sInvalidWR:
		// do we have a pending?
		writers, readers := s.anyWritesInProgressFor(keym.key)
		_ = readers
		for _, w := range writers {
			s.completeWrite(keym, w.TicketID)
		}
		s.unblockReadsFor(keym)
		//for _, r := range readers {
		//	s.actionRR(keym, r.TicketID)
		//}
		keym.state = sValid
		// we are in recvValidate
	case sWrite:
		// X, N/A, not expected
	case sReplay:
		//s.actionRR(keym, v.TicketID)
		s.unblockReadsFor(keym)
		keym.state = sValid
	}
	return
}

// we should only be called when s.nextWakeCh fires,
// since we reset it. This has combined failed-coordinator
// and failed-follower logic, so it follows up on
// both reads and writes in progress.
//
// [A] node that finds a key in an Invalid
// state for an extended period can
// safely replay a write by taking on a coordinator role and
// retransmitting INV messages to the replica ensemble with
// the original timestamp (i.e., original version number and coordID),
// hence preserving the global write order.
func (s *HermesNode) checkCoordOrFollowerFailed() {
	vv("%v top of checkCoordFailed()", s.me)

	s.nextWakeCh = nil // allow GC of timer.
	s.nextWakeTicket = nil
	defer s.setWakeup()

	for {
		// process all until no more past their deadline
		if s.timeoutPQ.size() < 1 {
			return
		}
		tkt, deadline := s.timeoutPQ.peek() // s.timeoutPQ is type pqTime
		now := time.Now()
		if now.Before(deadline) {
			return
		}
		s.timeoutPQ.pop()
		vv("%v checkCoordFailed detects failure over deadline: tkt='%v'", s.me, tkt)
		keym := tkt.keym
		key := keym.key

		switch keym.state {
		case sInvalid: // failed coordinator case
			vv("%v checkCoordFailed sees sInvalid: (key='%v', tkt.FromID='%v')", s.me, string(key), rpc.AliasDecode(tkt.FromID))

			coordFailed := !tkt.messageLossTimeout.IsZero() && now.After(tkt.messageLossTimeout)

			vv("%v checkCoordFailed sees coordFailed = '%v'", s.me, coordFailed)
			if coordFailed {
				// buffer the read, "same as actionW, but without incrementing TS"
				s.actionBR(tkt)
				keym.state = sReplay
				invalidation := &INV{
					TicketID: tkt.TicketID,
					Key:      key,
					Val:      keym.val,
					TS:       keym.TS,
					EpochID:  s.EpochID,
					FromID:   s.PeerID, // must be us, so they reply to us.
				}
				s.bcastInval(key, invalidation)
			}
		case sInvalidWR: // failed follower case
			// TODO: I think we must wait (for consistency) for a majority of ACK first.

			if tkt.Op == WRITE || tkt.Op == RMW {
				s.completeWrite(keym, tkt.TicketID) // really?
			} else {
				keym.state = sInvalid
				//s.actionRR(keym, tkt.TicketID)
				s.unblockReadsFor(keym)
			}

		case sWrite: // failed follower case
			// TODO: It seems to me that we should be requiring a majority of ACK at
			// least before we do this...
			// TODO: I think we must wait (for consistency) for a majority of ACK first.

			vv("%v failed follower: complete write, move to Valid, bcast Valid.", s.me)
			if tkt.Op == WRITE {
				keym.state = sValid
				s.completeWrite(keym, tkt.TicketID)
				if !useBcastAckOptimization {
					valid := &VALIDATE{
						TicketID: tkt.TicketID,
						FromID:   s.PeerID,
						Key:      keym.key,
						EpochID:  s.EpochID,
						TS:       keym.TS,
						Val:      keym.val, // TODO remove
					}
					s.bcastValid(valid)
				} else {
					//s.actionRR(keym, tkt.TicketID)
					s.unblockReadsFor(keym)
				}
			}
		case sReplay: // failed follower case
			// TODO: I think we must wait (for consistency) for a full round of all member ACK first?
			keym.state = sValid
			//s.actionRR(keym, tkt.TicketID)
			s.unblockReadsFor(keym)
			if !useBcastAckOptimization {
				valid := &VALIDATE{
					TicketID: tkt.TicketID,
					FromID:   s.PeerID,
					Key:      keym.key,
					EpochID:  s.EpochID,
					TS:       keym.TS,
					Val:      keym.val, // TODO remove
				}
				s.bcastValid(valid)
			}
		}
	}
}

func (s *HermesNode) setWakeup() {
	//vv("setWakeup disabled for debugging.")
	//return
	sz := s.timeoutPQ.size()
	vv("%v setWakeup() called. timeoutPQ sz = %v", s.me, sz)
	if sz > 0 {
		tkt, nextWake := s.timeoutPQ.peek()
		if !nextWake.IsZero() && nextWake.After(s.nextWake) {
			// lets not wakeup only to have a few microseconds left to wait.
			s.nextWake = nextWake.Add(10 * time.Microsecond)
			dur := s.nextWake.Sub(time.Now())
			s.nextWakeCh = time.After(dur)
			s.nextWakeTicket = tkt
			vv("setWakeup set nextWakeCh to wake at %v in %v", s.nextWake, dur)
			return
		}
	}
	// no point in polling in select if nothing needed.
	s.nextWakeCh = nil
	s.nextWakeTicket = nil
	s.nextWake = time.Time{}
}

func (s *HermesNode) reconfigRM() {
	// "• CRMW-replay: After an RM reconfiguration, the coordinator
	// resets any gathered ACKs of a pending RMW and replays
	// the RMW to ensure it is not conflicting."

	//for _, item := range s.timeoutPQ {
	for it := s.timeoutPQ.tree.Min(); !it.Limit(); it = it.Next() {
		item := *it.Item().(*pqTimeItem)

		tkt := item.tkt
		if tkt.Op == RMW {
			tkt.ackVector = make(map[string]bool)
			s.replayRMW(tkt)
		}
	}
}

func (s *HermesNode) replayRMW(tkt *HermesTicket) {
	panic("TODO implement s.replayRMW()")
}

func (s *HermesNode) mustGetTicket(ticketID string) *HermesTicket {
	item, ok := s.tkt2item[ticketID]
	if !ok {
		panic(fmt.Sprintf("mustGetTicket(ticketID='%v') could not find Ticket", ticketID))
	}
	return item.tkt
}

func (s *HermesNode) getTicket(ticketID string) (tkt *HermesTicket, ok bool) {
	var item *pqTimeItem
	item, ok = s.tkt2item[ticketID]
	if !ok {
		return
	}
	tkt = item.tkt
	return
}
