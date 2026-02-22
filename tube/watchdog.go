package tube

import (
	"context"
	"fmt"
	"strings"
	"time"

	rpc "github.com/glycerine/rpc25519"
)

const dieOnConnectionRefused = false

// called by handleAppendEntries/Ack, tallyVote,
// tallyPreVote, and addToCktall() when a
// message is received on the given circuit.
// The mc is the remote/peer's current MC,
// but can be nil on this call when a new
// ckt arrives but we have not yet seen
// an actual fragment from them yet (in addToCktall).
func (c *cktPlus) seen(mc *MemberConfig, lastLogIndex, lastLogTerm, currentTerm int64) {

	//vv("%v seen called for PeerName='%v' cktPlus %p with mc='%v'; from '%v'", c.node.me(), c.PeerName, c, mc.Short(), fileLine(2)) // nil mc means addToCktall called us.
	now := time.Now()
	c.lastSeenMut.Lock()
	if now.After(c.lastSeenTm) {
		// lastSeenTm is polled by the watchdog background goroutine.
		c.lastSeenTm = now
	}
	c.lastSeenMut.Unlock()

	// too much cloning of same object here! reduce it
	// by comparing if we are just cloning the same thing.
	// membership should be very stable most of the time.
	copyNeeded := true
	if c.MC != nil && mc != nil {
		if c.MC.equal(mc) {
			// dup, can skip the copy
			copyNeeded = false
		}
	}
	if copyNeeded && mc != nil {
		c.MC = mc.Clone() // nil returns nil
	}

	if lastLogTerm > 0 {
		c.LastLogTerm = lastLogTerm
	}
	if lastLogIndex > 0 {
		c.LastLogIndex = lastLogIndex
	}
	if currentTerm > 0 {
		c.CurrentTerm = currentTerm
	}
	if len(c.stalledOnSeenTkt) > 0 {
		if c.node.role != LEADER {
			c.stalledOnSeenTkt = nil
		} else {
			tkt := c.stalledOnSeenTkt[0]
			c.stalledOnSeenTkt = c.stalledOnSeenTkt[1:]
			// resume ticket
			//vv("%v seen is unstalling membershipChange tkt='%v'", c.node.me(), tkt.Short())
			c.node.changeMembership(tkt)
		}
	} else {
		if c.node.role == LEADER && !c.node.state.MC.IsCommitted {
			// we might be able to conclude cur MC is committed now.
			// possible side effect: might set s.state.MC.IsCommitted = true.
			// in seen() here.
			c.node.onLeaderIsCurrentMCcommitted()
		}
	}
	return
}

// called by handleAppendEntries.
func (s *TubeNode) seenIndirect(c *cktPlus, leaderLastHeard time.Time) {
	//vv("%v (%p)cktPlus sees Indirect from %v: %v", s.name, c, c.PeerName, nice(leaderLastHeard))
	c.lastSeenMut.Lock()
	if leaderLastHeard.After(c.lastSeenTm) {
		c.lastSeenTm = leaderLastHeard
	}
	c.lastSeenMut.Unlock()
}

// called at top of startWatchdog
func (s *TubeNode) parkedInsert(c *cktPlus) {
	// note parked is type map[peerName]map[cktPlus.sn]*cktPlus
	m2, ok := s.parked[c.PeerName]
	if ok {
		m2[c.sn] = c
		return
	}
	m2 = make(map[int64]*cktPlus)
	m2[c.sn] = c
	s.parked[c.PeerName] = m2
}

// suppressWatchdogs is a helper for handleAppendEntries().
// We report that this remote peer has been seen to all watchdogs.
// Using the indirect information in the AE we also
// suppress follower-to-follower restarts. As long as
// the leader can talk to other followers, we'll
// assume that we can too, without checking, to
// conserve network bandwidth and avoid a full
// n*n ping storm going off constantly.
func (s *TubeNode) suppressWatchdogs(ae *AppendEntries) {

	//const gcParked = false // not really sure we want this!
	// try it:
	const gcParked = true
	var goners []*cktPlus

	cktP0, ok := s.cktall[ae.FromPeerID]
	if ok {
		//vv("%v calling seen() directly cktP0 = %p for %v", s.name, cktP0, cktP0.PeerName)
		if gcParked && cktP0.atomicConnRefused.Load() {
			s.deleteFromCktAll(cktP0)
		} else {
			cktP0.seen(ae.MC, ae.LeaderLLI, ae.LeaderLLT, ae.LeaderTerm) // in handleAE
		}
		// any others parked under this name?
		group, ok := s.parked[cktP0.PeerName]
		if ok {
			for _, cktp := range group {
				if gcParked && cktp.atomicConnRefused.Load() {
					goners = append(goners, cktp)
				} else {
					cktp.seen(ae.MC, ae.LeaderLLI, ae.LeaderLLT, ae.LeaderTerm) // in handleAE 2; really in suppressWatchdogs.
				}
			}
			for _, gone := range goners {
				delete(group, gone.sn) // GC
				s.deleteFromCktAll(gone)
			}
			goners = nil
		}
	} else {
		//panic(fmt.Sprintf("%v ugh! why no cktP to leader?!?", s.me()))
	}
	// transitively update for other followers
	for peerID, lastHeard := range ae.PeerID2LastHeard {
		cktP, ok := s.cktall[peerID]
		if ok {
			if gcParked && cktP.atomicConnRefused.Load() {
				s.deleteFromCktAll(cktP)
			} else {
				s.seenIndirect(cktP, lastHeard)
			}

			// and tell the background parked too
			group, ok := s.parked[cktP.PeerName]
			if ok {
				for _, park := range group {
					if park == cktP0 {
						continue // already told above
					}
					if gcParked && park.atomicConnRefused.Load() {
						goners = append(goners, park)
					} else {
						s.seenIndirect(park, lastHeard)
					}
				}
				for _, gone := range goners {
					// GC parked and cktall/cktReplica
					delete(group, gone.sn)
					s.deleteFromCktAll(gone)
				}
				goners = nil
			}
		}
	}
}

func refused(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "connection refused")
}

// we park all c into c.node.parked before doing anything else.
func (c *cktPlus) startWatchdog() {

	// store them all in parked for now. TODO: figure out how to GC them.
	c.node.parkedInsert(c)

	go func() {

		// how often to check if the ckt is up
		refresh := 2 * time.Second
		tick := time.NewTicker(refresh)
		if c.ckt == nil {
			tick.Stop() // do tick.Reset(refresh) once we first have a ckt
		}
		defer func() {
			//vv("%v watchdog exiting; c='%v'", c.node.name, c)
			tick.Stop()
			c.perCktWatchdogHalt.RequestStop()
			c.perCktWatchdogHalt.MarkDone()

			// cktPlus.Close() also does this, but synctest
			// shutdown deadlock is complaining so try to be
			// more aggressive about it.
			c.node.Halt.RemoveChild(c.perCktWatchdogHalt)
		}()

		var pack *packReconnect
		var prevpack *packReconnect
		_ = prevpack
		s := c.node
		var isUp bool
		var lastSeen time.Time

		if c.ckt != nil {
			isUp = true
			lastSeen = time.Now()
		}
		c.atomicIsUp.Store(isUp)
		//vv("%v watchdog init isUp=%v for '%v'", s.me(), isUp, c.PeerName)

		var lastReconnAttempt time.Time // don't attempt reconn too often
		var been time.Duration          // since heard from remote
		followDown := s.followerDownDur()
		recomputeIsUp := func() bool {
			if lastSeen.IsZero() {
				return false
			}
			been = time.Since(lastSeen)
			if been >= followDown {
				return false
			}
			return true
		}
		var err error
		for {
			// have we got a new submit request?
			// requestReconnect is like a channel
			// with exactly one space, and in that
			// space, an older unserviced request
			// can be overwritten with subsequent requets.
			now := time.Now()
			c.requestReconnectMut.Lock()

			// how this works: backgroundConnectToPeer() sets
			// a c.requestReconnect before pulsing on the
			// c.requestReconnectPulse channel, which
			// causes us to get here if we are not already
			// in middle of trying to restart.
			req := c.requestReconnect
			if req != nil {
				c.requestReconnect = nil // "de-queue"
				c.requestReconnectLastBeganTm = now
			}
			c.requestReconnectMut.Unlock()
			if req != nil {
				// yep!
				isUp, err = c.reconnect(req, fmt.Sprintf("requestReconnect: submit='%v'", nice(req.submit)))
				c.atomicIsUp.Store(isUp)

				now = time.Now()
				if isUp {
					lastSeen = now
					tick.Reset(refresh)
				} else {
					if dieOnConnectionRefused && refused(err) {
						c.atomicConnRefused.Store(true)
						//alwaysPrintf("%v shutting down watchdog(%p) because connection refused: '%v'", s.name, c, err)
						return
					}
				}
				lastReconnAttempt = now
			}

			var cktHalt chan struct{}
			if c.ckt != nil {
				cktHalt = c.ckt.Halt.ReqStop.Chan
			}
			select {
			case <-c.requestReconnectPulse:
				// immediately check the requestReconnect
				// one space queue above, as this
				// tells us there is something waiting.
				// client code can use default: and
				// we'll get it on the next refresh (2 seconds)
				// if we miss this immediate request because
				// an existing request is already being
				// processed. All we need do is continue,
				// to hit the code at the top of the loop.
				continue

			case <-tick.C:
				if pack == nil {
					continue
				}
				c.lastSeenMut.Lock()
				// c.lastSeenTm is set by seen() and seenIndirect().
				externalLastSeenTm := c.lastSeenTm
				c.lastSeenMut.Unlock()
				if externalLastSeenTm.After(lastSeen) {
					lastSeen = externalLastSeenTm
				}
				if !recomputeIsUp() {
					//vv("%v (%p)startWatchdog: <-tick.C been %v so need reconnect to '%v'; lastSeen='%v'; externalLastSeenTm='%v'", s.me(), c, been, pack.addr, nice(lastSeen), nice(externalLastSeenTm))

					now := time.Now()
					beenSinceAttempt := now.Sub(lastReconnAttempt)
					wait := refresh - beenSinceAttempt
					if wait > 0 {
						//vv("%v watchdog avoid reconnect for another %v to avoid fast spin", s.name, wait)
						tick.Reset(wait)
						continue
					}

					if s.cfg.isTest {
						panicAtCap(s.testWatchdogTimeoutReconnectCh) <- time.Now()
					}

					isUp = false
					c.atomicIsUp.Store(isUp)

					isUp, err = c.reconnect(pack, "tick.C")
					//vv("%v on <-tick.C reconnect('%v') -> isUp=%v", s.name, pack.addr, isUp)
					c.atomicIsUp.Store(isUp)
					now = time.Now()
					if isUp {
						lastSeen = now
						tick.Reset(refresh)
					} else {
						if dieOnConnectionRefused && refused(err) {
							c.atomicConnRefused.Store(true)
							//alwaysPrintf("%v shutting down watchdog(%p) because connection refused: '%v'", s.name, c, err)
							return
						}
					}
					lastReconnAttempt = now
				}
			case <-c.perCktWatchdogHalt.ReqStop.Chan:
				//alwaysPrintf("%v shutting down watchdog(%p) because <-c.perCktWatchdogHalt.ReqStop.Chan", s.name, c)
				return
			case <-cktHalt:
				//alwaysPrintf("%v shutting down watchdog(%p) because <-c.ckt.ReqStop.Chan", s.name, c)
				return

			case <-s.Halt.ReqStop.Chan:
				//alwaysPrintf("%v shutting down watchdog(%p) because <-s.Halt.ReqStop.Chan", s.name, c)
				return
			case <-s.MyPeer.Halt.ReqStop.Chan:
				return
			}
		}
	}()
}

// internal goro helper for startWatchdog.
// We have to be sure to tear down any extant
// client/connection first or else simnet
// will bark at us about re-used names: the
// "client name already taken" error.
// Well our server auto-cli needs to be able
// to support many client connections to
// the same, and it does now by appending
// a random suffix.
func (c *cktPlus) reconnect(pack *packReconnect, why string) (isUp bool, err error) {
	s := c.node
	addr := pack.addr

	if addr == "" {
		//vv("%v empty address in pack! pack='%#v'", s.name, pack)
		return false, fmt.Errorf("skip reconnect with empty addr")
	}

	//vv("%v top reconnect to addr '%v': why='%v'", s.name, addr, why)
	defer func() {
		//vv("%v end of reconnect to addr '%v'; isUp=%v (err='%v')", s.me(), addr, isUp, err)

		// we could clear out any requests made while
		// we were trying the last one, so we
		// don't busy loop as much; like this; of course
		// we would miss if the address changes, but
		// we should catch that on the next refresh loop.
		if isUp {
			c.requestReconnectMut.Lock()
			c.requestReconnect = nil
			c.requestReconnectMut.Unlock()
		}
	}()
	// We get a request under way. AND we block, or timeout 5 sec later.
	// The attempt might still succeed even if we timeout, of course.
	// The important thing is to only ever have one attempt outstanding.

	if addr == "pending" {
		// will crash the ckt2 PreferExtant call...
		// does this mean we already have one in progress?
		// maybe on another watchdog. Should we shutdown?
		//vv("%v pending addr ugh. do we have info in pack='%#v'", pack)
		return false, fmt.Errorf("skip reconnect with pending addr")
	}

	var errWriteDur time.Duration
	// maybe we should be timing out? yeah.
	errWriteDur = time.Second * 5

	t0 := time.Now()
	_ = t0
	ctxCanc, cancFunc := context.WithTimeout(context.Background(), errWriteDur)
	c.latestCancFuncMut.Lock()
	c.latestCancFunc = cancFunc
	c.latestCancFuncMut.Unlock()
	// placeholder until we implement versions fully
	var peerServiceNameVersion string

	// blocks until a response is received or
	// we timeout after 5 seconds.
	var ckt *rpc.Circuit
	var madeNewAutoCli bool
	var onlyPossibleAddr string
	circuitName := "tube-ckt"
	var userString string
	ckt, _, madeNewAutoCli, onlyPossibleAddr, err = s.MyPeer.PreferExtantRemotePeerGetCircuit(
		ctxCanc,
		circuitName,
		userString,
		nil,
		c.PeerServiceName,
		peerServiceNameVersion,
		addr,
		errWriteDur, s.MyPeer.NewCircuitCh, waitForAckTrue)
	_ = onlyPossibleAddr
	cancFunc() // release resources

	// We thought we could do waitForAck=false, but then
	// the PeerName and PeerID are empty because we
	// have not heard back yet.
	_, _ = ckt, madeNewAutoCli

	if err != nil {
		isUp = false
		c.atomicIsUp.Store(isUp)
		//alwaysPrintf("%v: ugh. watchdog reconnect(addr='%v') after '%v' got err = '%v'; netAddr='%v'; s.state.MC='%v'", s.me(), addr, time.Since(t0), err, pack.netAddr, s.state.MC.Short())
	} else {
		isUp = true
		c.atomicIsUp.Store(isUp)

		now := time.Now()
		c.lastSeenMut.Lock()
		if now.After(c.lastSeenTm) {
			c.lastSeenTm = now
		}
		c.lastSeenMut.Unlock()

		//alwaysPrintf("%v: good, watchdog reconnect() came back with nil err trying to connect to pack.addr='%v' (pack.url='%v'); s.MyPeer.NewCircuitCh=%p, we got ckt='%v'", s.me(), addr, pack.url, s.MyPeer.NewCircuitCh, ckt.RemotePeerName)
	}

	//if madeNewAutoCli {
	// addToCktall will shutdown
	// the old ckt when the new one arrives and takes
	// its place, so nothing needed here now.
	//}
	return
}
