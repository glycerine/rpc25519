package tube

import (
	"bytes"
	"context"
	"fmt"
	//"reflect"
	"strings"
	"sync/atomic"
	"time"

	"github.com/glycerine/blake3"
	"github.com/glycerine/idem"
	rpc "github.com/glycerine/rpc25519"
	"golang.org/x/time/rate" // for rate.Limiter
)

//go:generate greenpack

// Squady vs Czar: all members are Squady(s), and the
// first Squady to win the race to write/obtain the key
// lease on the "czar" key also becomes the Czar.

// ReliableMembershipListType is the
// Vtype for the ReliableMembershipList
// encoded Value payloads.
// Note: must be different from the type, else
// the go compiler gets confused; i.e. we
// cannot have this just be ReliableMembershipList
// as that is the name of the type.
const ReliableMembershipListType string = "ReliableMembershipListType"

const PeerDetailPlusType string = "PeerDetailPlusType"
const leaseAutoDelFalse = false
const leaseAutoDelTrue = true

// Czar must have greenpack to use the rpc.Call system.

//go:generate greenpack

type czarState int32

const (
	unknownCzarState czarState = 0
	amCzar           czarState = 1
	notCzar          czarState = 2
)

func (s czarState) String() string {
	switch s {
	case unknownCzarState:
		return "unknownCzarState"
	case amCzar:
		return "amCzar"
	case notCzar:
		return "notCzar"
	}
	return "unknown czarState"
}

// Czar is the RAM/memory only maintainer of
// the RMember system of membership. The acting Czar is elected
// via Raft by writing to the key "czar"
// in the configured RMember.TableSpace.
type Czar struct {
	slow bool // use sessions and refresh members/ dir in raft?

	Halt *idem.Halter `msg:"-"`

	tableSpace string
	keyCz      string

	// one single source of truth, no more separate nonCzarMembers
	members  *ReliableMembershipList
	vers     *RMVersionTuple
	listhash string // detect actual peerID list changes.

	heard map[string]time.Time
	sess  *Session

	// something for greenpack to serz (to use the rpc system via Ping)
	Placeholder int `zid:"0"`

	// this the client of Tube, not rpc.
	// It represents the TubeNode of the
	// Czar when it is active as czar (having
	// won the lease on the {tableSpace}/czar key in Tube).
	name string

	UpcallMembershipChangeCh chan *PingReply `msg:"-"`

	t0                 time.Time
	memberHeartBeatDur time.Duration
	memberLeaseDur     time.Duration // used to be called declaredDeadDur
	clockDriftBound    time.Duration

	memberHeartBeatCh <-chan time.Time
	refreshMembersCh  <-chan time.Time

	cli *TubeNode

	requestPingCh chan *pingReqReply

	cState atomic.Int32 // czarState

	renewCzarLeaseCh  <-chan time.Time
	renewCzarLeaseDue time.Time

	leaseDurCzar      time.Duration
	renewCzarLeaseDur time.Duration

	writeAttemptDur time.Duration

	membersTableLeaseDur   time.Duration
	refreshMembersTableDur time.Duration

	myDetail      *PeerDetailPlus
	myDetailBytes []byte

	rateLimiter *rate.Limiter

	blake *blake3.Hasher

	testingCrashIfNotMonotone bool // czar_test 809.
}

func NewCzar(tableSpace, name string, cli *TubeNode, clockDriftBound time.Duration) (s *Czar) {

	//memberHeartBeatDur := time.Second * 2
	memberHeartBeatDur := time.Second
	memberLeaseDur := memberHeartBeatDur * 6 // 12s if given 2s heartbeat
	members := cli.NewReliableMembershipList()
	members.MemberLeaseDur = memberLeaseDur

	s = &Czar{
		tableSpace:         tableSpace,
		name:               name,
		keyCz:              "czar",
		Halt:               idem.NewHalterNamed(name),
		members:            members,
		vers:               &RMVersionTuple{},
		heard:              make(map[string]time.Time),
		t0:                 time.Now(),
		memberLeaseDur:     memberLeaseDur,
		memberHeartBeatDur: memberHeartBeatDur,

		clockDriftBound: clockDriftBound,
		cli:             cli,
		requestPingCh:   make(chan *pingReqReply),
		blake:           blake3.New(64, nil),
	}
	// table hermes, key "czar"
	//s.leaseDurCzar = time.Second * 10
	s.leaseDurCzar = time.Second * 5
	s.renewCzarLeaseDur = s.leaseDurCzar / 5

	// how long each Write waits for a response.
	//s.writeAttemptDur = time.Second * 5
	s.writeAttemptDur = time.Second * 2

	// table /members, key member_name in the Tube kvstore.
	s.membersTableLeaseDur = time.Second * 5
	s.refreshMembersTableDur = time.Second
	//s.membersTableLeaseDur = time.Second * 30
	//s.refreshMembersTableDur = s.membersTableLeaseDur / 3

	// careful here. too tight (1 sec/10 burst) and we
	// see alot of goro memory leaks on the raft leader
	// even though the czar is the one limited. This
	// every 100ms and 1000 burst seems fine.
	hz := rate.Every(100 * time.Millisecond)
	burst := 1000
	s.rateLimiter = rate.NewLimiter(hz, burst)

	return s
}

func (czar *Czar) refreshMemberInTubeMembersTable(ctx context.Context) (err error) {
	if !czar.slow {
		return
	}
	vv("begin refreshMemberInTubeMembersTable()")
	t1 := time.Now()

	// we submit an approximation to provide rough information
	// about the remaining lease duration -- even though
	// the tube Leaf.LeaseUntilTm
	// is the actual decider. Hopefully they should be similar.
	// Also we mostly do this so that our table key/value dumps
	// do not print (current czar) next to them (the current czar
	// writes 0 time to its own hermes/czar list entry).
	czar.myDetail.RMemberLeaseUntilTm = time.Now().Add(czar.membersTableLeaseDur)
	czar.myDetail.RMemberLeaseDur = czar.membersTableLeaseDur
	czar.myDetailBytes, err = czar.myDetail.MarshalMsg(nil)
	panicOn(err)

	ctx5, canc := context.WithTimeout(ctx, time.Second*5)
	_, err = czar.sess.Write(ctx5, Key(czar.tableSpace), Key("members/"+czar.name), Val(czar.myDetailBytes), czar.writeAttemptDur, PeerDetailPlusType, czar.membersTableLeaseDur, leaseAutoDelTrue)
	canc()
	vv("members table every 10s refresh attempt done (took %v). err = '%v'", time.Since(t1), err)

	czar.refreshMembersCh = time.After(czar.refreshMembersTableDur)
	return err
}

var ErrNotIncreasingRMVersionTuple = fmt.Errorf("error: RMVersionTuple must be monotone increasing")

var ErrExpiredCzarLease = fmt.Errorf("error: CzarLeaseUntilTm has expired")

func (s *Czar) setVers(v *RMVersionTuple, list *ReliableMembershipList) (err error) {

	// is lease expired?
	now := time.Now()
	if now.After(v.CzarLeaseUntilTm) {
		return ErrExpiredCzarLease
	}

	// insist on CzarLeaseEpoch and then internal within-a-single czar WithinCzarVersion
	// monotonicity.
	cmp := v.VersionCompare(s.vers)

	if cmp < 0 {
		//vv("%v: rejecting setVers: insisting RMVersionTuple must never decrease, cmp=%v; s.vers=current='%v'; rejecting proposed new v = '%v'", s.name, cmp, s.vers, v)
		err = ErrNotIncreasingRMVersionTuple
		if s.testingCrashIfNotMonotone {
			// czar_test 809 asserts we never go backwards during that test.
			panicf("%v: rejecting setVers: insisting RMVersionTuple must never decrease, cmp=%v; s.vers=current='%v'; rejecting proposed new v = '%v'", s.name, cmp, s.vers, v)
		}
		return
	}

	listhash := s.hashPeerIDs(list, v)
	hashDiffers := (listhash != s.listhash)
	//if hashDiffers {
	//vv("%v: new membership seen (cur = '%v'; vs new = '%v')\n cur s.listhash='%v'\n versus new listhash='%v'\n", s.name, s.members, list, s.listhash, listhash) // seen a ton, of course.
	//}

	s.members = list.Clone()
	s.vers = v
	s.listhash = listhash
	s.t0 = time.Now()

	// but only upcall if members PeerIDs actually changed (or
	// CzarLeaseEpoch, or WithinCzarVersion).
	if !hashDiffers {
		// done early, no change in the set of PeerNames and PeerIDs,
		// so no upcall needed.
		//vv("%v: no change in the set of peerIDs in ReliableMembershipList", s.name) // seen, good.
		return
	}

	//vv("end of setVers(v='%v') s.members is now '%v')", v, s.members)
	s.doUpcall(nil)
	return nil
}

func (s *Czar) doUpcall(reply *PingReply) {
	// skip the members.Clone() if no upcall requested or possible;
	// s.UpcallMembershipChangeCh must be buffered so czar
	// loop does not pause excessively.
	if s.UpcallMembershipChangeCh == nil {
		return
	}
	if reply == nil {
		reply = &PingReply{
			Members: s.members.Clone(),
			Vers:    s.vers.Clone(),
			// note that Status gives the _local_ member's status as czar or not.
			Status: s.cState.Load(),
		}
	}
	select {
	// should be the only place, so we keep vers and list in sync.
	case s.UpcallMembershipChangeCh <- reply:
	case <-s.Halt.ReqStop.Chan:
	}
}

func (s *Czar) memberCount() (numMembers int) {

	if s.members == nil {
		return
	}
	return s.members.PeerNames.Len()
}

func (s *Czar) expireSilentNodes(fromList *ReliableMembershipList) (changed bool, newlist *ReliableMembershipList) {

	if fromList == nil {
		fromList = s.members.Clone()
	}

	now := time.Now()
	for name, plus := range s.members.PeerNames.All() {
		if name == s.name {
			// now that we exclude czar from PeerNames, should not be needed:
			/*
				// we ourselves are obviously alive so
				// we don't bother to heartbeat to ourselves.
				// Ah-hah! but omitting the below makes us look stale to
				// other members, yikes! we add in ourselves now:
				plus.RMemberLeaseUntilTm = now.Add(s.memberLeaseDur)
				plus.RMemberLeaseDur = s.memberLeaseDur
			*/
			continue
		}
		killIt := false
		lastHeard, ok := s.heard[name]
		if !ok {
			// if we have not been listening for heartbeats
			// for very long, give them a chance--we may
			// have just loaded them in from the czar key's value.
			uptime := time.Since(s.t0)
			if plus.RMemberLeaseUntilTm.IsZero() ||
				(uptime > s.memberLeaseDur &&
					// zero time are actually dead former czar,
					// and we do need to delete those.
					// Immediate deletion is safe since Tube's
					// write lease mechansim insists on expiry
					// before any new write can succeed.
					now.After(plus.RMemberLeaseUntilTm.Add(s.clockDriftBound))) {

				killIt = true
				//pp("expiring dead node '%v' -- would upcall membership change too. either dead former czar or nothing heard after uptime = '%v'", name, uptime)
			}
		} else {
			been := now.Sub(lastHeard)
			if plus.RMemberLeaseUntilTm.IsZero() ||
				(been > s.memberLeaseDur &&
					now.After(plus.RMemberLeaseUntilTm.Add(s.clockDriftBound))) {

				killIt = true
				//pp("expiring dead node '%v' -- would upcall membership change too. either dead former czar or been '%v'", name, been)
			}
		}
		if killIt {
			changed = true // might happen multiple times.
			delete(s.heard, name)
			// Omap.All allows delete in the middle of iteration.
			if newlist == nil {
				newlist = s.members.Clone()
			}
			newlist.PeerNames.Delkey(name)
		}
	}

	return
}

var ErrNotCzar = fmt.Errorf("I am not the czar. Re-query the Tube czar key.")

type pingReqReply struct {
	ctx   context.Context
	args  *PeerDetailPlus
	reply *PingReply
	done  *idem.IdemCloseChan
	err   error

	inspectOnly bool
}

// for simnet tests; would not work across real network at the moment.
func (s *Czar) inspect(ctx context.Context) (reply *PingReply, err error) {

	reply = &PingReply{}
	rr := &pingReqReply{
		ctx:         ctx,
		reply:       reply,
		done:        idem.NewIdemCloseChan(),
		inspectOnly: true,
	}

	select {
	case s.requestPingCh <- rr:
		// submitted. proceed to next select below.

	case <-s.Halt.ReqStop.Chan:
		err = rpc.ErrHaltRequested
		return
	case <-ctx.Done():
		err = ctx.Err()
		if err == nil {
			err = rpc.ErrContextCancelled
		}
		return
	}

	select {
	case <-rr.done.Chan:
		err = rr.err
		return

	case <-s.Halt.ReqStop.Chan:
		err = rpc.ErrHaltRequested
		return

	case <-ctx.Done():
		err = ctx.Err()
		if err == nil {
			err = rpc.ErrContextCancelled
		}
		return
	}
	return
}

func (s *Czar) Ping(ctx context.Context, args *PeerDetailPlus, reply *PingReply) (err error) {

	// since the rpc system will call us on a
	// new goroutine, separate from the main goroutine,
	// we submit the request over a channel to
	// avoid mutex/deadlock issues.

	// but do a quick check that we are even czar,
	// because otherwise no point in wasting time
	// calling into the czar/main goro. Ping is
	// only for members to keep their lease with the
	// czar active. They should find the current
	// czar if they are talking to us by mistake.
	cur := czarState(s.cState.Load())
	if cur != amCzar {
		vv("external Ping goro sees we are currently not czar")
		return ErrNotCzar
	}

	rr := &pingReqReply{
		ctx:   ctx,
		args:  args,
		reply: reply,
		done:  idem.NewIdemCloseChan(),
	}

	select {
	case s.requestPingCh <- rr:
		// submitted. proceed to next select below.

	case <-s.Halt.ReqStop.Chan:
		return rpc.ErrHaltRequested
	case <-ctx.Done():
		err = ctx.Err()
		if err == nil {
			err = rpc.ErrContextCancelled
		}
		return
	}

	select {
	case <-rr.done.Chan:
		if rr.reply != nil {
			*reply = *(rr.reply)
		}
		return rr.err

	case <-s.Halt.ReqStop.Chan:
		return rpc.ErrHaltRequested

	case <-ctx.Done():
		err = ctx.Err()
		if err == nil {
			err = rpc.ErrContextCancelled
		}
		return
	}

}

// handlePing is internal; called in response to <-czar.requestPingCh.
// We may change the s.cState. Be prepared to
// not be czar anymore after this, even if we started as czar.
func (s *Czar) handlePing(rr *pingReqReply) {
	//vv("top of Czar.handlePing from '%v' PID: %v", rr.args.Det.Name, rr.args.Det.PID)
	defer func() {
		//vv("end of Czar.handlePing from '%v' PID: %v; rr.err='%v'; rr.reply='%v'", rr.args.Det.Name, rr.args.Det.PID, rr.err, rr.reply)
		rr.done.Close() // required! else ping will stall forever.
	}()

	// czar_test wants to inspect state of member/czar
	if rr.inspectOnly {
		rr.reply = &PingReply{
			Members: s.members.Clone(),
			Vers:    s.vers.Clone(),
			Status:  s.cState.Load(),
		}
		return
	}

	cur := czarState(s.cState.Load())
	if cur != amCzar {
		vv("Czar.handlePing: not czar atm, rejecting straight off.")
		rr.err = ErrNotCzar
		return
	}

	now := time.Now()
	if now.After(s.vers.CzarLeaseUntilTm) {
		vv("Czar.handlePing: lease has expired, so not czar atm, rejecting.")

		s.cState.Store(int32(unknownCzarState))
		rr.err = ErrNotCzar
		return
	}
	//vv("Czar.handlePing: am czar, process this ping from '%v'", rr.args.Det.Name)

	args := rr.args
	origVers := s.vers

	if hdr, ok := rpc.HDRFromContext(rr.ctx); ok {
		////vv("Ping, from ctx: hdr='%v'", hdr)
		////vv("Ping, from ctx: hdr.Nc.LocalAddr()='%v'; hdr.Nc.RemoteAddr()='%v'", hdr.Nc.LocalAddr(), hdr.Nc.RemoteAddr()) // we want remote
		// critical: replace Addr with the rpc.Client of the czar
		// address, rather than the tube client peer server address.
		////vv("changing args.Addr from '%v' -> '%v'", args.Addr, hdr.Nc.RemoteAddr())
		args.Det.Addr = hdr.Nc.RemoteAddr().String()
	} else {
		panic("must have rpc.HDRFromContext(ctx) set so we know which tube-client to drop when the rpc.Client drops!")
	}
	////vv("Ping called at cliName = '%v', since args = '%v'; origVers='%v'", s.CliName, args, origVers)
	if s.memberLeaseDur < time.Millisecond {
		panicf("s.memberLeaseDur too small! '%v'", s.memberLeaseDur)
	}

	leasedUntilTm := now.Add(s.memberLeaseDur)
	args.RMemberLeaseUntilTm = leasedUntilTm
	args.RMemberLeaseDur = s.memberLeaseDur

	// don't think we want this now... right? czar is only
	// in CzarName/Det back from tube leader czar key....
	// if false {

	// 	// always refresh our (czar) lease in the member list too,
	// 	// especially in Vers.CzarLeaseUntilTm (!)
	// 	mePlus, ok := s.members.PeerNames.Get2(s.name)
	// 	if !ok {
	// 		// try to fix instead of panic-ing... after checking, it looks
	// 		// like maybe s.members is stuck?!?

	// 		// fired! why??
	// 		//panicf("must have self as czar in members! s.name='%v' not found in s.members = '%v'", s.name, s.members)

	// 		// maybe something like this:
	// 		mePlus = getMyPeerDetailPlus(s.cli)
	// 		//myDetailBytes, err = mePlus.MarshalMsg(nil)
	// 		//panicOn(err)
	// 		s.members.PeerNames.Set(s.name, mePlus)
	// 	}
	// 	mePlus.RMemberLeaseUntilTm = leasedUntilTm
	// 	mePlus.RMemberLeaseDur = s.memberLeaseDur
	// 	_ = mePlus
	// }

	s.heard[s.name] = now
	// but (only) *this* is what the members are checking!!

	updated := false
	var updatedList *ReliableMembershipList
	var updatedVers *RMVersionTuple

	det, ok := s.members.PeerNames.Get2(args.Det.Name)
	if !ok {
		////vv("args.Name('%v') is new, adding to PeerNames", args.Name)
		det = args
		updatedList = s.members.Clone()
		updatedList.PeerNames.Set(args.Det.Name, args)
		updatedVers = s.vers.Clone()
		updatedVers.WithinCzarVersion++
		updated = true

	} else {
		if detailsChanged(det.Det, args.Det) {
			////vv("args.Name('%v') details have changed, updating PeerNames", args.Name)
			det = args
			updatedList = s.members.Clone()
			updatedList.PeerNames.Set(args.Det.Name, args)
			updatedVers = s.vers.Clone()
			updatedVers.WithinCzarVersion++
			updated = true

		} else {
			// do we want lease-extension to increment the Version?
			// I don't think so. This is the most common
			// failure free path here. On the otherhand, its
			// hard to know who is most recent and avoid
			// a late duplicate down-date if we don't increment
			// Version--besides it is cheap since it is
			// fully in RAM/memory only. Thus we set a policy
			// for users: a higher Version may only mean
			// a later lease duration, not a different
			// set of members.
			//s.vers.LeaseUpdateCounter++
		}
		////vv("args.Name '%v' already exists in PeerNames, det = '%v'", args.Name, det)
	}
	// INVAR: det is the Ping caller's PeerDetailPlus.

	// extend their lease in the visible return value;
	// det points into the s.members.PeerNames map, so
	// it will get cloned in the Clone() call below
	// and thus assigned to reply.
	det.RMemberLeaseUntilTm = leasedUntilTm
	det.RMemberLeaseDur = s.memberLeaseDur
	s.members.MemberLeaseDur = s.memberLeaseDur

	s.heard[args.Det.Name] = now
	changed, newlist := s.expireSilentNodes(updatedList)
	if changed {
		updatedList = newlist
		if !updated {
			updated = true
			updatedVers.WithinCzarVersion++
		}
	}
	if updated {
		err := s.setVers(updatedVers, updatedList)
		panicOn(err)
		vv("Czar.Ping: membership has changed (was %v) is now: {%v}", origVers, s.shortRMemberSummary())

	} else {
		vv("Czar.Ping: no membership change with this call. cur: '%v'", s.shortRMemberSummary())
	}

	reply := &PingReply{
		Members: s.members.Clone(),
		Vers:    s.vers.Clone(),
		// note that Status gives the _local_ member's status as czar or not.
		Status: s.cState.Load(),
	}
	rr.reply = reply

	////vv("czar sees Czar.Ping(cliName='%v') called with args='%v', reply with current membership list, czar replies with ='%v'", s.cliName, args, reply)
}

func (s *Czar) shortRMemberSummary() (r string) {
	n := s.members.PeerNames.Len()
	r = fmt.Sprintf("[%v members; Vers:%v]{\n", n, s.vers)
	i := 0
	for name, plus := range s.members.PeerNames.All() {
		r += fmt.Sprintf("[%02d] %v: %v\n", i, name, plus.Det.URL)
		i++
	}
	r += "}"
	return
}

//msgp:ignore RMember

// RMember provies a ReliableMembership service with these goals:
//
// a) Maintain a current view of who's in the group.
//
// b) Notify members when that view changes.
//
// c) Ensure all members converge to the same view
// by providing the RMVersionTuple versioning.
//
// The implementation takes the following approach.
// The default test/tablespace is/was called "hermes"
// because we wrote this with an implementation
// of the Hermes replication protocol in mind.
// Hermes must be built atop a reliable membership
// service. See https://hermes-protocol.com/
//
// (0) we must be pre-configured with the raft nodes addresses,
// so we can bootrstrap from them. Any tube/raft cluster
// member can tell us the leader, and the leader
// can tell us about all cluster nodes if need be.
//
// (1) as a RMember, I register my name and PeerDetail
// under table:{tableSpace} e.g. "hermes" key:"czar"
// with a 20 second lease.
//
// I renew it every 10 seconds or so.
// The first writer to grab the lease "wins" the election
// to czar. All members stand ready to take over as czar
// if the first czar fails. The member also includes the
// most recent list it has of other members when
// it writes the czar key.
//
// (2) Each Hermes/tablespace member node
// heartbeats to the Czar saying:
// "I'm a member, and who else is a member and at what epoch/version?"
// If the epoch or version changes, update the membership list
// in the Hermes upcall. The czar regularly scans
// the list for members that have not sent a heartbeat
// recently. When discovered, they are removed from the list
// and an upcall to update membership is made.
//
// (3) If the Czar cannot be reached,
// the Hermes node starts trying to become the Czar by
// writing a lease through Raft to a pre-configured "czar" key.
//
// Repeat until either the current Czar can be reached
// or a new Czar is elected. Again by contending for
// table:hermes(tableSpace) key:czar.
//
// Notice that only a single lease is taken out through
// Raft/Tube, and it only needs updating every 10
// seconds or so.
//
// The membership list stored under the czar key
// is not authoritative. It is is probably stale,
// it cannot be assumed to be the most recent version.
//
// It is only written
// when the current czar renews its lease.
// The czar must be contacted for the most
// up-to-date version of the membership.
// It is just a reasonable place to start for
// the new czar--it hopefully avoids some
// membership churn, but correctness never
// depends on it, and the written Details.RMVersionTuple
// will always be incorrect and stale, since
// we cannot set the lease epoch correctly
// before actually writing and winning the
// lease.
//
// After the heartbeats are received or not
// then the membership will converge as
// stale nodes are pruned.
//
// Each new czar starts a new CzarLeaseEpoch based
// on the consensus LeaseEpoch obtained when
// write is put through Raft. Within each
// epoch, the RAM-only membership updates are
// denoted by incrementing the Version number
// of the RMVersionTuple.
//
// To use Tube as a consistent DNS more generally, we
// could also write our name -> details
// into a separate tableSpace, if so desired.
// We have not needed to do so yet.
//
//	func main() {
//	    clockDriftBound := 500 * time.Millisecond
//	    mem := tube.NewRMember("hermes", clockDriftBound)
//	    mem.Start()
//	    <-mem.Ready.Chan
//	    // ... rest of code here, or just
//	    select {}
//	}
//
// See also rpc25519/tube/cmd/member for a simple
// demonstration program and its example.cfg.
type RMember struct {

	// TableSpace is set by the NewRMember() constructor.
	// The "czar" key at the root of this TableSpace
	// is used to elect/lease out the czar-ship (leader
	// of the membership who maintains current membership
	// in RAM and receives Ping calls from members to
	// maintain their membership).
	TableSpace string

	// UpcallMembershipChangeCh tells users about
	// changes in membership.
	UpcallMembershipChangeCh chan *PingReply

	// Ready is closed when UpcallMembershipChangeCh is set
	// and the RMember is ready to use (call Start(), then
	// wait for Ready to be closed).
	Ready *idem.IdemCloseChan

	clockDriftBound time.Duration

	Cfg *TubeConfig

	testingAmCzarCh chan bool

	czar *Czar

	name string
}

// NewRMember creates a member of the given tableSpace.
// Users must call Start() and then wait until Ready is closed
// before accessing UpcallMembershipChangeCh to
// get membership changes.
func NewRMember(tableSpace string, cfg *TubeConfig) (rm *RMember) {
	if cfg == nil {
		panic("cannot have nil cfg in NewRMember")
	}
	if cfg.MyName == "" {
		panicf("ugh: cfg.MyName cannot be empty! cfg='%v'", cfg)
	}

	cp := *cfg
	rm = &RMember{
		TableSpace:      tableSpace,
		Ready:           idem.NewIdemCloseChan(),
		Cfg:             &cp,
		clockDriftBound: cp.ClockDriftBound,
		name:            cfg.MyName,

		UpcallMembershipChangeCh: make(chan *PingReply, 10),
	}
	if cfg.isTest {
		rm.testingAmCzarCh = make(chan bool, 10)
	}
	return
}

// Start elects a Czar and manages the RMember's membership
// in the TableSpace.
func (membr *RMember) Start() {
	go membr.start()
}

func (membr *RMember) start() {

	tableSpace := membr.TableSpace

	cliCfg := membr.Cfg
	name := cliCfg.MyName

	//vv("tubeCliName = '%v'", name) // e.g. member_suM7r8JkqBYkgUm1U4AS

	cli := NewTubeNode(name, cliCfg)
	err := cli.InitAndStart()
	panicOn(err)

	czar := NewCzar(tableSpace, name, cli, membr.clockDriftBound)
	membr.czar = czar

	if cliCfg.isTest && cliCfg.testNum == 809 {
		czar.testingCrashIfNotMonotone = true
		//vv("set testingCrashIfNotMonotone true for 809") // seen on 809 and not 808.
	}

	// was leaking at end of 809 test; declare here so we
	// close in defer.
	var rpcClientToCzar *rpc.Client

	defer func() {
		if rpcClientToCzar != nil {
			rpcClientToCzar.Close()
		}
		rpcClientToCzar = nil

		cli.Close()
		vv("%v: member/czar has closed its cli", name)
		czar.Halt.ReqStop.Close()
		czar.Halt.Done.Close()
	}()

	cli.Srv.RegisterName("Czar", czar)

	vv("cli.MyPeer.PeerName = '%v'", cli.MyPeer.PeerName)
	if cli.MyPeer.PeerName == "" {
		panic("cli.MyPeer.PeerName must not be empty")
	}

	czar.UpcallMembershipChangeCh = membr.UpcallMembershipChangeCh

	// tell user it is safe to listen on
	// membr.UpcallMembershipChangeCh now.
	membr.Ready.Close()

	vv("s.memberLeaseDur = '%v'", czar.memberLeaseDur)

	//topT0 := time.Now()

	bkg := context.Background()
	_ = bkg

fullRestart:
	for j := 0; ; j++ {
		vv("%v: top of fullRestart j=%v", name, j)

		if j > 0 {
			//czar.rateLimiter.Wait(bkg)
		}
		//topT0 = time.Now()

		// let the close session pace it now...

		ctx := context.Background()

		select {
		case <-czar.Halt.ReqStop.Chan:
			vv("%v: czar halt requested (at fullRestart top). exiting.", name)
			return
		default:
		}

		if czar.slow {
			if czar.sess != nil {
				ctx2, canc := context.WithTimeout(ctx, time.Second*2)
				err = cli.CloseSession(ctx2, czar.sess)
				canc()
				if err != nil {
					vv("%v: closing prior session err='%v'", name, err)
				}
			}
		}

		const requireOnlyContact = false
		for k := 0; ; k++ {
			//vv("%v: find leader loop k = %v", name, k)
			//vv("%v: cliCfg.Node2Addr = '%#v'", name, cliCfg.Node2Addr)
			leaderURL, leaderName, _, reallyLeader, _, err := cli.HelperFindLeader(ctx, cliCfg, "", requireOnlyContact, KEEP_CKT_ONLY_IF_LEADER) // KEEP_CKT_UP) // KEEP_CKT_ONLY_IF_LEADER)
			//vv("%v: helper said: leaderURL = '%v'; reallyLeader=%v; err='%v'", name, leaderURL, reallyLeader, err)
			panicOn(err)
			if !reallyLeader {
				vv("%v: arg. we see not really leader? why?", name)
				cli.closeAutoClientSockets()
				continue fullRestart
			}
			// should have updated our notion of leader, else on leader change we can be stuck
			// see peerListReplyHandler() tube.go:13234
			insp := cli.Inspect()
			if insp.CurrentLeaderName != "" &&
				insp.CurrentLeaderName != leaderName {
				panicf("why was insp.CurrentLeaderName(%v) != leaderName(%v) back from helper?", insp.CurrentLeaderName, leaderName)
			}

			if !czar.slow {
				break
			}
			ctx5, canc := context.WithTimeout(ctx, time.Second*5)
			czar.sess, err = cli.CreateNewSession(ctx5, leaderName, leaderURL)
			canc()
			//panicOn(err) // panic: hmm. no leader known to me (node 'node_0')
			if err == nil {
				vv("%v: got sess = '%v'", name, czar.sess)
				break
			}
			alwaysPrintf("%v: got err from CreateNewSession, sleep 1 sec and try again: '%v'", name, err)
			time.Sleep(time.Second)
		}

		czar.myDetail = getMyPeerDetailPlus(cli)
		czar.myDetailBytes, err = czar.myDetail.MarshalMsg(nil)
		panicOn(err)

		//vv("myDetail = '%v' for tubeCliName = '%v'; myDetailBytes len %v", czar.myDetail, name, len(czar.myDetailBytes))

		//var czarURL string
		//var czarCkt *rpc.Circuit

		var rpcClientToCzarDoneCh chan struct{}
		var czarLeaseUntilTm time.Time

		var expireCheckCh <-chan time.Time

		//halt := idem.NewHalter()
		//defer halt.Done.Close()

		// TODO: handle needing new session, maybe it times out?
		// should survive leader change, but needs checking.

		err = czar.refreshMemberInTubeMembersTable(ctx)
		if err != nil {
			vv("%v: about to fullRestart b/c/ czar.refreshMemberInTubeMembersTable() gave err='%v'", name, err)
			continue fullRestart
		}
		//vv("begin main loop at haveSess")

		var closedSockets bool
		_ = closedSockets

	haveSess:
		for ii := 0; ; ii++ {
			////vv("main loop ii = %v   fullRestart j = %v   cState = %v", ii, j, czarState(czar.cState.Load()))
			//if ii > 0 {
			//	time.Sleep(time.Millisecond * 100)
			//}

			if ii > 0 {
				//czar.rateLimiter.Wait(bkg)
			}

			select {
			case <-czar.Halt.ReqStop.Chan:
				vv("%v: czar halt requested (at haveSess top). exiting.", name)
				return
			default:
			}

			var errCzarAttempt error
			switch czarState(czar.cState.Load()) {

			case unknownCzarState:
				closedSockets = false

				// find the czar. it might be me.
				// we try to write to the "czar" key with a lease.
				// first one there wins. everyone else reads the winner's URL.
				list := czar.members.Clone()
				vers := czar.vers.Clone()
				var prevLeaseEpoch int64
				if vers != nil {
					prevLeaseEpoch = vers.CzarLeaseEpoch
				}

				// // I think this is borked and giving us split brain:

				// // start with the highest version list we can find.
				// nonCzarMembers := czar.getNonCzarMembers()
				// if nonCzarMembers != nil {
				// 	if nonCzarMembers.Vers.VersionGT(list.Vers) {
				// 		//vv("nonCzarMembers.Vers(%v) sz=%v; was > list.Vers(%v) sz=%v", nonCzarMembers.Vers, nonCzarMembers.PeerNames.Len(), list.Vers, list.PeerNames.Len())
				// 		list = nonCzarMembers.Clone()

				// 		// the lease czar key Vers version is garbage and
				// 		// always overwritten
				// 		// anyway with the LeaseEpoch -- used to create a new version,
				// 		// so there is no need to bother to update it in the raft log.
				// 	} else {
				// 		//vv("nonCzarMembers.Vers(%v) nonCzarMembers.sz=%v; was <= list.Vers(%v) list.sz=%v", nonCzarMembers.Vers, nonCzarMembers.PeerNames.Len(), list.Vers, list.PeerNames.Len())
				// 	}
				// }

				// if we win the write race, we are the czar.
				// and the old czar is out; so prepare for that:
				// so 1) delete the old czar from the list we submit;

				list.PeerNames.Delkey(list.CzarName)
				list.PeerNames.Delkey(name)
				// and 2) add ourselves as new czar in the list we submit.
				list.CzarName = name

				czar.myDetail = getMyPeerDetailPlus(cli)
				if czar.myDetail == nil {
					panic("must have myDetail here!")
				}
				list.CzarDet = czar.myDetail.Clone()
				if list.CzarDet == nil {
					panic("list.CzarDet cannot be nil!")
				}
				pp("submit to CAS list = '%v'", list)

				bts2, err := list.MarshalMsg(nil)
				panicOn(err)

				// if "we" own the lease, the CAS on prevLeaseEpoch
				// means we are only allowed to
				// ratchet from our own lease epoch to the
				// next when renewing before expiry. This
				// prevents multiple instances of "us"
				// from interferring, if by mistake two
				// members under the same name are started.
				// We could also assign
				// the lease to a particular peerID to prevent this
				// but... so far leases only track name and not peerID too.
				//
				// Also by not using auto-deleting keys, we get tombstones
				// (keys with expired leases) and can ratchet strictly
				// monotonically increasing
				// lease epochs, instead of accidentally zapping the key's
				// lease epoch back down to zero on auto-delete.
				prevLeaseEpoch = 0
				if vers != nil {
					prevLeaseEpoch = vers.CzarLeaseEpoch
				}
				// in cState == unknownCzarState here

				// Notice that when we write(cas) into the tube/raft leader,
				// we do not include the vers; the LeaseEpoch would be wrong
				// since it will get incremented only if the CAS succeeds,
				// and we don't know what it will be in advance; the minor number
				// WithinCzarVersion can never be up to date since it is only
				// the czar who knows it anyway, from its last increment
				// of WithinCzarVersion in RAM-only. Hence we reconstruct it
				// from the czar's Ping reponses and enforce monotonicity
				// over the reconstructed version tuple only--although we
				// do make an effort to not reset WithinCzarVersion to 0
				// if the lease epoch is the same.

				var czarTkt *Ticket
				// stuck here 35 minutes huh. use a timeout.
				ctx5, canc := context.WithTimeout(ctx, time.Second*5)
				if czar.slow {
					czarTkt, errCzarAttempt = czar.sess.CAS(ctx5, Key(czar.tableSpace), Key(czar.keyCz), nil, Val(bts2), czar.writeAttemptDur, ReliableMembershipListType, czar.leaseDurCzar, leaseAutoDelFalse, 0, prevLeaseEpoch)
				} else {
					czarTkt, errCzarAttempt = cli.CAS(ctx5, Key(czar.tableSpace), Key(czar.keyCz), nil, Val(bts2), czar.writeAttemptDur, nil, ReliableMembershipListType, czar.leaseDurCzar, leaseAutoDelFalse, 0, prevLeaseEpoch)
				}
				canc()

				if czarTkt == nil {
					// context timeout, retry.
					continue fullRestart
				}

				czarLeaseUntilTm = czarTkt.LeaseUntilTm

				// INVAR: state == unknownCzarState here
				if errCzarAttempt == nil {

					// I am czar.
					czar.cState.Store(int32(amCzar))

					if membr.testingAmCzarCh != nil {

						membr.testingAmCzarCh <- true
						vv("%v: reported am czar", name)
					}

					now := time.Now()
					left := czarLeaseUntilTm.Sub(now)
					if left < 2*time.Second {
						//vv("less than 2 sec left on lease as czar?!? try again from haveSess")
						//cState = unknownCzarState
						czar.cState.Store(int32(unknownCzarState))

						continue haveSess
					}
					// INVAR left >= 2s
					checkAgainIn := 5 * time.Second
					if left < checkAgainIn {
						checkAgainIn = left - time.Millisecond*500
					}
					expireCheckCh = time.After(checkAgainIn)

					// unpack the list we got back. It should be
					// what we wrote, but confirm this for sanity check.
					if czarTkt.Vtype != ReliableMembershipListType {
						panicf("expected nil errCzarAttempt ticket to have czarTkt.Vtype == ReliableMembershipListType; got '%v'", czarTkt.Vtype)
					}
					if len(czarTkt.Val) != len(bts2) ||
						bytes.Compare(czarTkt.Val, bts2) != 0 {
						panicf("expected nil errCzarAttempt ticket to have czarTkt.Val)(len %v) == bts2(len %v)': why did we not get back what we wrote if errCzarAttempt was nil???", len(czarTkt.Val), len(bts2))
					}

					// can we note old dead leasor czar for reporting
					var oldCzarName string
					if czarTkt.PrevLeaseVtype == ReliableMembershipListType &&
						len(czarTkt.PrevLeaseVal) > 0 {

						oldval := &ReliableMembershipList{}
						oldval.UnmarshalMsg(czarTkt.PrevLeaseVal)
						oldCzarName = oldval.CzarName
					}

					vers2 := &RMVersionTuple{
						CzarLeaseEpoch:    czarTkt.LeaseEpoch,
						WithinCzarVersion: 0, // NOT czarTkt.VersionRead,
						CzarLeaseUntilTm:  czarTkt.LeaseUntilTm,
						WriteLogIndex:     czarTkt.LogIndex,       // not LeaseWriteRaftLogIndex, that is on fail to write.
						LeaseEpochT0:      czarTkt.RaftLogEntryTm, // not  czarTkt.LeaseEpochT0, that is on fail to write.
					}
					if vers2.CzarLeaseEpoch == vers.CzarLeaseEpoch {
						// keep WithinCzarVersion monotone
						vers2.WithinCzarVersion = vers.WithinCzarVersion
					}
					//vers = vers2 // avoid confusion with old stale vers below if we print

					err = czar.setVers(vers2, list) // does upcall for us.
					if err != nil {
						// non-monotone error on tube servers restart hmm...
						vv("%v: see err = '%v', doing full restart", name, err) // seen!?!
						continue fullRestart
					}

					sum := czar.shortRMemberSummary()
					_ = sum
					left = time.Until(czar.vers.CzarLeaseUntilTm)

					vv("%v: err=nil on lease write. I am czar (name='%v'; oldCzarName='%v'), send heartbeats to tube/raft to re-lease the hermes/czar key to maintain that status. left on lease='%v'; czar='%v'", name, name, oldCzarName, left, sum)

					czar.renewCzarLeaseDue = time.Now().Add(czar.renewCzarLeaseDur)
					czar.renewCzarLeaseCh = time.After(czar.renewCzarLeaseDur)

				} else { // errCzarAttempt != nil, CAS did not succeed.

					errs := errCzarAttempt.Error()
					if strings.Contains(errs, "no leader known") {
						vv("%v: see errCzarAtttempt = '%v', with 'no leader known', doing full restart", name, errCzarAttempt)
						continue fullRestart
					}

					if strings.Contains(errs, "I am not leader") {
						vv("%v: see errCzarAtttempt = '%v', with 'I am not leader', doing full restart", name, errCzarAttempt)
						continue fullRestart
					}

					// lets insist on a 'valid' write error... or retry; just
					// so we don't leave the czar-ship unoccupied for long.
					if strings.HasPrefix(errs, rejectedWritePrefix) {
						// good, expected
					} else {
						vv("%v: did not get error prefixed with 'rejected write' so, doing full restart; errCzarAttempt='%v'", name, errCzarAttempt)
						continue fullRestart
					}

					if czarTkt.Vtype != ReliableMembershipListType {
						panicf("%v: why not ReliableMembershipListType back? got '%v'", name, czarTkt.Vtype)
					}

					nonCzarMembers := &ReliableMembershipList{}
					_, err = nonCzarMembers.UnmarshalMsg(czarTkt.Val)
					panicOn(err)

					if nonCzarMembers.CzarName == name && !czarTkt.LeaseUntilTm.IsZero() {

						left := time.Until(czarTkt.LeaseUntilTm)

						vv("%v ugh: I should not be czar on a failed write; try again! Could be a staile lease of course (left=%v), but then why did the write give an error? err='%v'; ", name, left, err)
						panic("why am I czar on failed write?")
						continue fullRestart
					}

					czar.cState.Store(int32(notCzar))

					if membr.testingAmCzarCh != nil {

						membr.testingAmCzarCh <- false
						vv("%v: reported not czar", name)
					}

					czarLeaseUntilTm = czarTkt.LeaseUntilTm
					expireCheckCh = nil

					nonCzarMembers.MemberLeaseDur = czar.memberLeaseDur
					vers2 := &RMVersionTuple{
						CzarLeaseEpoch:    czarTkt.LeaseEpoch,
						WithinCzarVersion: 0, // NOT czarTkt.VersionRead,
						CzarLeaseUntilTm:  czarTkt.LeaseUntilTm,
						WriteLogIndex:     czarTkt.LeaseWriteRaftLogIndex,
						LeaseEpochT0:      czarTkt.LeaseEpochT0,
					}
					if vers2.CzarLeaseEpoch == vers.CzarLeaseEpoch {
						// keep WithinCzarVersion monotone
						vers2.WithinCzarVersion = vers.WithinCzarVersion
					}

					err = czar.setVers(vers2, nonCzarMembers)
					if err != nil {
						// non-monotone error on tube servers restart hmm...
						vv("%v: see err = '%v', doing full restart", name, err) // seen!?!
						continue fullRestart
					}

					vv("%v: just went from unknown to nonCzar, created new vers='%v' (left='%v'); errCzarAttempt was '%v' ; from czarTkt.Val, we got back nonCzarMembers = '%v'", name, vers2, time.Until(vers2.CzarLeaseUntilTm), errCzarAttempt, nonCzarMembers)

					// do the upcall? or should we wait until
					// we ping the czar for a more reliable/centralized
					// view? probably more consistent that way,
					// and less churn--especially since a Ping means
					// the membership will include us/ourselves, which
					// is a nice property to have--and a bit awkward
					// otherwise. "Do something with this group you
					// are not in..." seems like a non-starter.
					//select {
					//case czar.UpcallMembershipChangeCh <- nonCzarMembers.Clone():
					//default:
					//}

					//pp("I am not czar, did not write to key: '%v'", err) // seen regularly???
					////pp("I am not czar, did not write to key: '%v'; nonCzarMembers = '%v'", err, nonCzarMembers) // too much to list all 100 in the log.
					// contact the czar and register ourselves.
				}

			case amCzar:

				//vv("%v: amCzar", name) // logs every 0.1 ms so leave off!

				//cs := cli.Srv.ListClients()

				until := czar.vers.CzarLeaseUntilTm

				now := time.Now()
				left := until.Sub(now)
				if left < 0 {
					vv("%v: ouch! I think I am czar, but my lease has expired without renewal... really we need to fix the renewal proces. CzarLeaseUntil(%v) - now(%v) = left = '%v' on czar.vers='%v'", name, nice(until), nice(now), left, czar.vers)
					//cState = unknownCzarState
					czar.cState.Store(int32(unknownCzarState))
					continue fullRestart
				}

				// if false {
				// 	vv("%v: I am czar with (left on lease: %v) memberCount() = %v (from s.members.PeerNames.Len());  cli.Srv.ListClients() len %v", tubeCliName, left, czar.memberCount(), len(cs))
				// 	for i, c := range cs {
				// 		fmt.Printf("[%03d] %v\n", i, c)
				// 	}
				// 	fmt.Println()
				// }
				select {
				case rr := <-czar.requestPingCh:
					czar.handlePing(rr)
					cur := czarState(czar.cState.Load())
					if cur != amCzar {
						vv("%v: about to fullRestart b/c/ I am no longer czar after handlePing'", name, err)
						continue fullRestart
					}

				case <-time.After(left):
					vv("%v: ouch2! I think I am czar, but my lease has expired without renewal... really we need to fix the renewal proces.", name)
					//cState = unknownCzarState
					czar.cState.Store(int32(unknownCzarState))

					continue fullRestart

				case <-czar.refreshMembersCh:
					err := czar.refreshMemberInTubeMembersTable(ctx)
					if err != nil {
						vv("%v: about to fullRestart b/c/ czar.refreshMemberInTubeMembersTable() gave err='%v'", name, err)
						continue fullRestart
					}

				case <-expireCheckCh:
					changed, newlist := czar.expireSilentNodes(nil)
					if changed {
						//pp("Czar check for heartbeats: membership changed, is now: {%v}", czar.shortRMemberSummary())
						newvers := czar.vers.Clone()
						newvers.WithinCzarVersion++
						czar.setVers(newvers, newlist)
					}
					expireCheckCh = time.After(5 * time.Second)

				case <-czar.renewCzarLeaseCh:

					// if we update, are we going to lose the Plus part leases?
					// naw, I think that does not apply to the czar,
					// who gives out leases and only itself lease from the Tube/Raft cluster.
					czar.myDetail = getMyPeerDetailPlus(cli)
					czar.myDetailBytes, err = czar.myDetail.MarshalMsg(nil)
					panicOn(err)

					// we don't want this any more
					//czar.members.PeerNames.Set(czar.myDetail.Det.Name, czar.myDetail)

					czar.members.PeerNames.Delkey(czar.myDetail.Det.Name)

					czar.members.CzarDet = czar.myDetail

					bts2, err := czar.members.MarshalMsg(nil)
					panicOn(err)

					prevUntil := czar.vers.CzarLeaseUntilTm

					var prevLeaseEpoch int64
					if czar.vers != nil {
						prevLeaseEpoch = czar.vers.CzarLeaseEpoch
					}

					// hung here on cluster leader bounce, write
					// has failed.
					ctx5, canc := context.WithTimeout(ctx, time.Second*5)
					var czarTkt *Ticket
					if czar.slow {
						//czarTkt, err = czar.sess.Write(ctx5, Key(czar.tableSpace), Key(czar.keyCz), Val(bts2), czar.writeAttemptDur, ReliableMembershipListType, czar.leaseDurCzar, leaseAutoDelFalse)
						czarTkt, err = czar.sess.CAS(ctx5, Key(czar.tableSpace), Key(czar.keyCz), nil, Val(bts2), czar.writeAttemptDur, ReliableMembershipListType, czar.leaseDurCzar, leaseAutoDelFalse, 0, prevLeaseEpoch)
					} else {
						//czarTkt, err = cli.Write(ctx5, Key(czar.tableSpace), Key(czar.keyCz), Val(bts2), czar.writeAttemptDur, nil, ReliableMembershipListType, czar.leaseDurCzar, leaseAutoDelFalse)
						czarTkt, err = cli.CAS(ctx5, Key(czar.tableSpace), Key(czar.keyCz), nil, Val(bts2), czar.writeAttemptDur, nil, ReliableMembershipListType, czar.leaseDurCzar, leaseAutoDelFalse, 0, prevLeaseEpoch)
					}
					canc()
					if err != nil {
						vv("%v: renewCzarLeaseCh attempt to renew lease with CAS-write to keyCz:'%v' failed: err='%v'", name, czar.keyCz, err)

						//cState = unknownCzarState
						czar.cState.Store(int32(unknownCzarState))

						continue fullRestart
					}

					now := time.Now()
					previousLeaseLeft := prevUntil.Sub(now)
					_ = previousLeaseLeft
					//pp("renewed czar lease (with %v left on it), good until %v (%v out). Will renew in %v at ~ '%v' for %v", previousLeaseLeft, nice(czarTkt.LeaseUntilTm), time.Until(czarTkt.LeaseUntilTm), renewCzarLeaseDur, now.Add(renewCzarLeaseDur), leaseDurCzar)

					switch {
					case czar.vers.CzarLeaseEpoch < czarTkt.LeaseEpoch:
						czar.vers.CzarLeaseEpoch = czarTkt.LeaseEpoch
						//czar.vers.Version = 0

					case czar.vers.CzarLeaseEpoch == czarTkt.LeaseEpoch:
						//czar.vers.Version++
					default:
						panicf("%v: tube LeaseEpoch must be monotone up, but czar.vers.CzarLeaseEpoch('%v') already > what we just got back: czarTkt.LeaseEpoch('%v')", name, czar.vers.CzarLeaseEpoch, czarTkt.LeaseEpoch)
						continue fullRestart
					}
					czar.vers.CzarLeaseUntilTm = czarTkt.LeaseUntilTm

					czar.renewCzarLeaseDue = now.Add(czar.renewCzarLeaseDur)
					czar.renewCzarLeaseCh = time.After(czar.renewCzarLeaseDur)

				case <-czar.Halt.ReqStop.Chan:
					vv("%v: czar halt requested (in amCzar state). exiting.", name)
					return
				}

			case notCzar:

				//vv("%v: notCzar", name) // too much logging, leave off.

				czarLeaseUntilTm = czar.vers.CzarLeaseUntilTm

				if rpcClientToCzar == nil {
					//pp("notCzar top: rpcClientToCzar is nil")

					list := czar.members.Clone()
					vers := czar.vers.Clone()
					_ = vers
					if list == nil {
						alwaysPrintf("%v: wat? in notCzar, why is nonCzarMembers nil?", name)
						panic("nonCzarMembers should never be nil now")
						continue fullRestart
					}
					if list.PeerNames == nil {
						alwaysPrintf("%v: wat? in notCzar, why is nonCzarMembers.PeerNames nil?", name)
						continue fullRestart
					}
					if list.CzarName == name {
						vv("%v: internal logic error? we are not czar but list.CzarName shows us: '%v'", name, list.CzarName)
						czar.cState.Store(int32(unknownCzarState))
						continue fullRestart
					}

					czarDetPlus := list.CzarDet
					if czarDetPlus == nil {
						panicf("%v: list with winning czar did not include czar Detail itself?? list='%v'", name, list)
					}
					vv("%v: will contact czar '%v' at URL: '%v'", name, list.CzarName, czarDetPlus.Det.URL)
					// what we want Call Ping to return to us:
					pingReplyToFill := &PingReply{} // ReliableMembershipList{}

					ccfg := *cli.GetConfig().RpcCfg
					dialTarget := removeTcp(czarDetPlus.Det.Addr)
					ccfg.ClientDialToHostPort = dialTarget
					vv("after removeTcp('%v'), dial to: '%v'", czarDetPlus.Det.Addr, dialTarget)

					rpcClientToCzar, err = rpc.NewClient(name+"_pinger_"+cryRand15B(), &ccfg)
					panicOn(err)
					err = rpcClientToCzar.Start()
					if err != nil {
						now := time.Now()
						var waitDur time.Duration
						if now.Before(czarLeaseUntilTm) {
							waitDur = czarLeaseUntilTm.Sub(now)
						}

						vv("%v: could not contact czar, err='%v' ... might have to wait out the lease... waitDur='%v'", name, err, waitDur)
						if rpcClientToCzar != nil {
							rpcClientToCzar.Close()
						}
						rpcClientToCzar = nil
						rpcClientToCzarDoneCh = nil
						//cState = unknownCzarState
						czar.cState.Store(int32(unknownCzarState))
						now = time.Now()
						if waitDur > 0 {
							vv("%v: waitDur= '%v' to wait out the current czar lease before trying again", name, waitDur)
							time.Sleep(waitDur)
						}
						continue fullRestart
					}
					rpcClientToCzarDoneCh = rpcClientToCzar.GetHostHalter().Done.Chan

					//pp("about to rpcClientToCzar.Call(Czar.Ping, myDetail='%v')", czar.myDetail)
					callStart := time.Now()
					_ = callStart

					ctx5, canc := context.WithTimeout(ctx, time.Second*5)
					err = rpcClientToCzar.Call("Czar.Ping", czar.myDetail, pingReplyToFill, ctx5)
					//pp("rpcClientToCzar.Call(Czar.Ping) took %v; err = '%v'", time.Since(callStart), err)
					canc()
					if err != nil {
						vv("%v: error back from Ping: '%v'", name, err)
						if rpcClientToCzar != nil {
							rpcClientToCzar.Close()
						}
						rpcClientToCzar = nil
						rpcClientToCzarDoneCh = nil
						//cState = unknownCzarState
						czar.cState.Store(int32(unknownCzarState))

						time.Sleep(time.Second)
						continue fullRestart
					}
					//pp("member(name='%v') did rpc.Call to Czar.Ping, got reply of %v nodes", name, reply.PeerNames.Len()) // seen regularly

					if pingReplyToFill == nil {
						panicf("err was nil, how can pingReplyToFill be nil??")
					}

					err = czar.setVers(pingReplyToFill.Vers, pingReplyToFill.Members)
					if err != nil {
						// non-monotone error on tube servers restart hmm...
						vv("%v: see err = '%v', doing full restart", name, err) // seen!?!
						continue fullRestart
					}

					czar.memberHeartBeatCh = time.After(czar.memberHeartBeatDur)
				} // end if rpcClientToCzar == nil

				// INVAR: rpcClientToCzar != nil
				//pp("notCzar: rpcClient != nil, about to select")
				select {
				case <-czar.refreshMembersCh:
					err := czar.refreshMemberInTubeMembersTable(ctx)
					if err != nil {
						vv("%v: about to fullRestart b/c/ czar.refreshMemberInTubeMembersTable() gave err='%v'", name, err)
						continue fullRestart
					}

				case <-rpcClientToCzarDoneCh:
					vv("%v: direct client to czar dropped! rpcClientToCzarDoneCh closed.", name)
					rpcClientToCzar.Close()
					rpcClientToCzar = nil
					rpcClientToCzarDoneCh = nil
					//cState = unknownCzarState
					czar.cState.Store(int32(unknownCzarState))

					continue fullRestart

				case <-czar.memberHeartBeatCh:
					// in notCzar here.

					reply := &PingReply{} // ReliableMembershipList{}

					// We might be talking to a stale random member who
					// is no longer czar, so check the reply czar LeaseUntilTm
					err = rpcClientToCzar.Call("Czar.Ping", czar.myDetail, reply, nil)
					////vv("member called to Czar.Ping, err='%v'", err)
					if err != nil {
						vv("%v: connection refused to (old?) czar, transition to unknownCzarState and write/elect a new czar", name)
						if rpcClientToCzar != nil {
							rpcClientToCzar.Close()
						}
						rpcClientToCzar = nil
						rpcClientToCzarDoneCh = nil
						//cState = unknownCzarState
						czar.cState.Store(int32(unknownCzarState))

						continue fullRestart
					}
					if reply == nil {
						panicf("err was nil, how can reply be nil??")
					}

					if reply != nil && reply.Members != nil && reply.Members.PeerNames != nil {
						//pp("member called to Czar.Ping, got reply with member count='%v'; rpcClientToCzar.RemoteAddr = '%v':\n reply = %v\n", reply.PeerNames.Len(), rpcClientToCzar.RemoteAddr(), reply)
						// check for bug in czar: did they add me to the list?
						_, ok2 := reply.Members.PeerNames.Get2(czar.myDetail.Det.Name)
						if !ok2 {
							panicf("%v: member detected bug in czar: got ping back without ourselves (myDetail.Det.Name='%v') in it!: reply='%v'", name, czar.myDetail.Det.Name, reply)
						}
					}
					if !reply.Vers.CzarLeaseUntilTm.IsZero() {
						deadline := reply.Vers.CzarLeaseUntilTm.Add(-membr.clockDriftBound)
						now := time.Now()
						//vv("reply.Vers = '%v';\n deadline = '%v' \n now = '%v'", reply.Vers, nice(deadline), nice(now))
						if lte(deadline, now) {
							// this is causing too many restarts! and then we leak clients/auto-clients? hazard of mixing rpc and circuit stuff maybe. kinda want
							vv("stale czar answer (not really the czar now), reconnect/contend; deadline(%v) <= now(%v)", nice(deadline), nice(now))

							if rpcClientToCzar != nil {
								rpcClientToCzar.Close()
							}
							rpcClientToCzar = nil
							rpcClientToCzarDoneCh = nil
							//cState = unknownCzarState
							czar.cState.Store(int32(unknownCzarState))

							continue fullRestart
						}
					}

					err = czar.setVers(reply.Vers, reply.Members)
					if err != nil {
						// non-monotone error on tube servers restart hmm...
						vv("%v: see err = '%v', doing full restart", name, err)
						continue fullRestart
					}

					// try more often, still have members keeping conn to raft leader
					if true { // !closedSockets {
						// just takes up file handles.
						closedSockets = true
						//vv("non-czar: closing autoCli")
						cli.closeAutoClientSockets()
					}

					czar.memberHeartBeatCh = time.After(czar.memberHeartBeatDur)

				case rr := <-czar.requestPingCh:
					// non czar responds too, to help czar_test inspect queries.
					czar.handlePing(rr)

				case <-czar.Halt.ReqStop.Chan:
					vv("%v: czar halt requested (in nonCzar state). exiting.", name)
					return
				}
			}
		}
	} // end fullRestart loop
}

func removeTcp(s string) string {
	if strings.HasPrefix(s, "tcp://") {
		return s[6:]
	}
	if strings.HasPrefix(s, "udp://") {
		return s[6:]
	}
	if strings.HasPrefix(s, "simnet://") {
		return s[9:]
	}
	return s
}
func detailsChanged(a, b *PeerDetail) bool {
	if a.Name != b.Name {
		return true
	}
	if a.URL != b.URL {
		return true
	}
	if a.PeerID != b.PeerID {
		return true
	}
	if a.Addr != b.Addr {
		return true
	}
	if a.PeerServiceName != b.PeerServiceName {
		return true
	}
	if a.PeerServiceNameVersion != b.PeerServiceNameVersion {
		return true
	}
	if a.NonVoting != b.NonVoting {
		return true
	}
	return false
}

// RMVersionTuple is the two part Reliable Membership tuple.
// Compare the CzarLeaseEpoch first, then the WithinCzarVersion.
// The CzarLeaseEpoch must go higher when the Czar
// changes through Raft, and the WithinCzarVersion can increment
// when per-Czar members are added/lost.
// Used by cmd/member/member.go whose implementation is here in czar.go.
type RMVersionTuple struct {
	CzarLeaseEpoch     int64     `zid:"0"`
	WithinCzarVersion  int64     `zid:"1"`
	LeaseUpdateCounter int64     `zid:"2"`
	CzarLeaseUntilTm   time.Time `zid:"3"`
	WriteLogIndex      int64     `zid:"4"`
	LeaseEpochT0       time.Time `zid:"5"`
}

func (s *RMVersionTuple) Clone() (r *RMVersionTuple) {
	if s == nil {
		return nil
	}
	r = &RMVersionTuple{
		CzarLeaseEpoch:     s.CzarLeaseEpoch,
		WithinCzarVersion:  s.WithinCzarVersion,
		LeaseUpdateCounter: s.LeaseUpdateCounter,
		CzarLeaseUntilTm:   s.CzarLeaseUntilTm,
		WriteLogIndex:      s.WriteLogIndex,
		LeaseEpochT0:       s.LeaseEpochT0,
	}
	return
}

func (z *RMVersionTuple) String() (r string) {
	r = "&RMVersionTuple{\n"
	r += fmt.Sprintf("    CzarLeaseEpoch: %v\n", z.CzarLeaseEpoch)
	r += fmt.Sprintf(" WithinCzarVersion: %v\n", z.WithinCzarVersion)
	r += fmt.Sprintf("LeaseUpdateCounter: %v\n", z.LeaseUpdateCounter)
	r += fmt.Sprintf("  CzarLeaseUntilTm: %v\n", nice(z.CzarLeaseUntilTm))
	r += fmt.Sprintf("     WriteLogIndex: %v\n", z.WriteLogIndex)
	r += fmt.Sprintf("      LeaseEpochT0: %v\n", nice(z.LeaseEpochT0))
	r += "}\n"
	return
}

type PeerDetailPlus struct {
	Det *PeerDetail `zid:"0"`

	// RMembers are granted leases here.
	RMemberLeaseUntilTm time.Time     `zid:"1"`
	RMemberLeaseDur     time.Duration `zid:"2"`
}

func (s *PeerDetailPlus) Clone() (r *PeerDetailPlus) {
	r = &PeerDetailPlus{
		RMemberLeaseUntilTm: s.RMemberLeaseUntilTm,
		RMemberLeaseDur:     s.RMemberLeaseDur,
	}
	if s.Det != nil {
		r.Det = s.Det.Clone()
	}
	return
}

func (s *PeerDetailPlus) String() string {
	now := time.Now()
	x := ""
	switch {
	case s.RMemberLeaseUntilTm.IsZero():
		//x = "current czar"
	case gte(now, s.RMemberLeaseUntilTm):
		x = "expired"
	default:
		x = fmt.Sprintf("%v left", s.RMemberLeaseUntilTm.Sub(now))
	}
	d := s.Det
	return fmt.Sprintf(`PeerDetailPlus{
                  Name: %v
                   URL: %v
                PeerID: %v
                  Addr: %v
       PeerServiceName: %v
PeerServiceNameVersion: %v
             NonVoting: %v
                   PID: %v
       RMemberLeaseDur: %v
   RMemberLeaseUntilTm: %v (%v)
}`, d.Name, d.URL, d.PeerID, d.Addr,
		d.PeerServiceName, d.PeerServiceNameVersion,
		d.NonVoting,
		d.PID,
		s.RMemberLeaseDur,
		nice(s.RMemberLeaseUntilTm), x)
}

// ReliableMembershipList is written under the czar key
// in Tube (Raft). Tube must set the Vers.CzarLeaseEpoch
// when it is written for us, since the submitting
// client won't know what that is. It depends on
// which write won the race and arrived first.
// Used by cmd/member/member.go.
type ReliableMembershipList struct {
	CzarName string          `zid:"0"`
	CzarDet  *PeerDetailPlus `zid:"1"`

	// PeerNames never contains the czar itself now, for
	// ease of update: we don't need to subtract the old
	// czar if we win as new czar and carry over the old list;
	// we just write ourselves in as the new czar and subtract
	// ourselves from PeerNames.
	PeerNames *Omap[string, *PeerDetailPlus] `msg:"-"`

	SerzPeerDetails []*PeerDetailPlus `zid:"2"`

	// members _must_ stop operations
	// after their lease has expired. It
	// is this long, and their PeerNames entry
	// PeerDetail.RMemberLeaseUntilTm
	// gives the deadline exactly.
	MemberLeaseDur time.Duration `zid:"3"`

	blake *blake3.Hasher
}

// detect peerID changes
func (s *Czar) hashPeerIDs(list *ReliableMembershipList, vers *RMVersionTuple) (h string) {

	s.blake.Reset()
	s.blake.Write([]byte(fmt.Sprintf("epoch:%v\nwithin:%v\n", vers.CzarLeaseEpoch, vers.WithinCzarVersion)))
	s.blake.Write([]byte(list.CzarName))
	s.blake.Write([]byte(list.CzarDet.Det.PeerID))

	for name, detp := range list.PeerNames.All() {
		s.blake.Write([]byte(name))
		s.blake.Write([]byte(detp.Det.PeerID))
	}

	h = blake3ToString33B(s.blake)
	return
}

type PingReply struct {
	Members *ReliableMembershipList `zid:"0"`
	Vers    *RMVersionTuple         `zid:"1"`
	Status  int32                   `zid:"2"`
}

func (z *PingReply) String() (r string) {
	r = "&PingReply{\n"
	r += fmt.Sprintf("Members: %v,\n", z.Members)
	r += fmt.Sprintf("   Vers: %v,\n", z.Vers)
	r += fmt.Sprintf(" Status: %v,\n", czarState(z.Status))
	r += "}\n"
	return
}

func (s *ReliableMembershipList) Clone() (r *ReliableMembershipList) {
	r = &ReliableMembershipList{
		CzarName:  s.CzarName,
		PeerNames: NewOmap[string, *PeerDetailPlus](),
	}
	if s.CzarDet != nil {
		r.CzarDet = s.CzarDet.Clone()
	}
	for name, det := range s.PeerNames.All() {
		r.PeerNames.Set(name, det.Clone())
	}
	ns := s.PeerNames.Len()
	nr := r.PeerNames.Len()
	if nr != ns {
		panicf("nr = %v but ns = %v", nr, ns)
	}
	return
}

func (s *ReliableMembershipList) String() (r string) {
	r = "&ReliableMembershipList{\n"
	npeer := s.PeerNames.Len()
	r += fmt.Sprintf("PeerNames: (%v present)\n", npeer)
	i := 0
	czarFound := false
	for name, plus := range s.PeerNames.All() {
		r += fmt.Sprintf("[%02d] %v\n", i, plus)
		i++
		if name == s.CzarName {
			czarFound = true
		}
	}
	numWithCzar := npeer
	if !czarFound {
		numWithCzar++
	}
	r += fmt.Sprintf(" CzarName: \"%v\"\n", s.CzarName)
	r += fmt.Sprintf("[ %v PeerNames listed above (%v total with czar) ]\n", npeer, numWithCzar)
	r += fmt.Sprintf("  CzarDet: %v\n", s.CzarDet)
	r += "}\n"
	return
}

// ignores the RMemberLeaseUntilTm and RMemberLeaseDur PeerDetailPlus fields.
func (a *ReliableMembershipList) Equal(b *ReliableMembershipList) bool {
	if a.CzarName != b.CzarName {
		return false
	}
	if !a.CzarDet.Det.equal(b.CzarDet.Det) {
		return false
	}
	if a.PeerNames.Len() != b.PeerNames.Len() {
		return false
	}
	for nameA, plusA := range a.PeerNames.All() {
		plusB, ok := b.PeerNames.Get2(nameA)
		if !ok {
			return false
		}
		if !plusA.Det.equal(plusB.Det) {
			return false
		}
	}
	// names are distinct and sets are equal sized, so
	// we have checked all the PeerNames.
	return true
}

func (i *RMVersionTuple) VersionGT(j *RMVersionTuple) bool {
	if i.CzarLeaseEpoch > j.CzarLeaseEpoch {
		return true
	}
	if i.CzarLeaseEpoch == j.CzarLeaseEpoch {
		return i.WithinCzarVersion > j.WithinCzarVersion
	}
	//if i.WithinCzarVersion == j.WithinCzarVersion {
	//	return i.LeaseUpdateCounter > j.LeaseUpdateCounter
	//}
	return false
}

func (i *RMVersionTuple) VersionLT(j *RMVersionTuple) bool {
	if i.CzarLeaseEpoch < j.CzarLeaseEpoch {
		return true
	}
	if i.CzarLeaseEpoch == j.CzarLeaseEpoch {
		return i.WithinCzarVersion < j.WithinCzarVersion
	}
	//if i.WithinCzarVersion == j.WithinCzarVersion {
	//	return i.LeaseUpdateCounter < j.LeaseUpdateCounter
	//}
	return false
}

func (i *RMVersionTuple) VersionGTE(j *RMVersionTuple) bool {
	if i.CzarLeaseEpoch > j.CzarLeaseEpoch {
		return true
	}
	if i.CzarLeaseEpoch == j.CzarLeaseEpoch {
		return i.WithinCzarVersion >= j.WithinCzarVersion
	}
	//if i.WithinCzarVersion == j.WithinCzarVersion {
	//	return i.LeaseUpdateCounter >= j.LeaseUpdateCounter
	//}
	return false
}

func (i *RMVersionTuple) VersionLTE(j *RMVersionTuple) bool {
	if i.CzarLeaseEpoch < j.CzarLeaseEpoch {
		return true
	}
	if i.CzarLeaseEpoch == j.CzarLeaseEpoch {
		return i.WithinCzarVersion <= j.WithinCzarVersion
	}
	//if i.WithinCzarVersion == j.WithinCzarVersion {
	//	return i.LeaseUpdateCounter <= j.LeaseUpdateCounter
	//}
	return false
}

func (i *RMVersionTuple) VersionEqual(j *RMVersionTuple) bool {
	return i.CzarLeaseEpoch == j.CzarLeaseEpoch &&
		i.WithinCzarVersion == j.WithinCzarVersion
}

func (i *RMVersionTuple) VersionCompare(j *RMVersionTuple) int {
	if i.CzarLeaseEpoch == j.CzarLeaseEpoch && i.WithinCzarVersion == j.WithinCzarVersion {
		return 0
	}
	if i.CzarLeaseEpoch < j.CzarLeaseEpoch {
		return -1
	}
	if i.CzarLeaseEpoch > j.CzarLeaseEpoch {
		return 1
	}
	if i.WithinCzarVersion < j.WithinCzarVersion {
		return -1
	}
	if i.WithinCzarVersion > j.WithinCzarVersion {
		return 1
	}
	return 0 // should never be reached, hmm.
}

func (i *RMVersionTuple) VersionAndLeaseUpdateEqual(j *RMVersionTuple) bool {
	return i.CzarLeaseEpoch == j.CzarLeaseEpoch &&
		i.WithinCzarVersion == j.WithinCzarVersion
	// && i.LeaseUpdateCounter == j.LeaseUpdateCounter
}

func (i *RMVersionTuple) EpochsEqual(j *RMVersionTuple) bool {
	return i.CzarLeaseEpoch == j.CzarLeaseEpoch
}

func (i *RMVersionTuple) EpochCompare(j *RMVersionTuple) int {
	if i.CzarLeaseEpoch == j.CzarLeaseEpoch {
		return 0
	}
	if i.CzarLeaseEpoch < j.CzarLeaseEpoch {
		return -1
	}
	if i.CzarLeaseEpoch > j.CzarLeaseEpoch {
		return 1
	}
	return 0
}

func (s *ReliableMembershipList) PreSaveHook() {
	s.SerzPeerDetails = s.SerzPeerDetails[:0]
	if s.PeerNames == nil {
		return
	}
	for _, kv := range s.PeerNames.Cached() {
		s.SerzPeerDetails = append(s.SerzPeerDetails, kv.val)
	}
}

func (s *ReliableMembershipList) PostLoadHook() {
	s.PeerNames = NewOmap[string, *PeerDetailPlus]()
	for _, d := range s.SerzPeerDetails {
		s.PeerNames.Set(d.Det.Name, d)
	}
	s.SerzPeerDetails = s.SerzPeerDetails[:0]
}

func (s *TubeNode) NewReliableMembershipList() *ReliableMembershipList {
	r := &ReliableMembershipList{
		PeerNames: NewOmap[string, *PeerDetailPlus](),
	}
	if s == nil {
		panic("s cannot be nil")
	}
	return r
}

// allow tup and tube to display values better.
func StringFromVtype(val Val, vtyp string) string {
	switch vtyp {
	case ReliableMembershipListType:
		rm := &ReliableMembershipList{}
		rm.UnmarshalMsg(val)
		return rm.String()
	case PeerDetailPlusType:
		////vv("attempting to unmarshal into PeerDetailPlus len %v bytes: '%v'", len(val), string(val))
		det := &PeerDetailPlus{}
		det.UnmarshalMsg(val)
		////vv("after UnmarshalMsg, det = '%v'", det)
		return det.String()
	}

	return string(val)
}

func getMyPeerDetailPlus(cli *TubeNode) (myDetail *PeerDetailPlus) {
	myDetail = &PeerDetailPlus{
		Det: cli.GetMyPeerDetail(),
	}
	if myDetail.Det.Addr == "" {
		// think we fixed this now in rpc.Server startup.
		panicf("must have Addr?!? cli.GetMyPeerDetail() gave use '%v'", myDetail.Det)
	}
	return
}

/*
Butler Lampson writes, in
WDAG 1996: Workshop on Distributed Algorithms, Bologna, Italy
"How to Build a Highly Available System Using Consensus":

"""
2.3 Hierarchical Leases

In a fault-tolerant system leases must be granted and renewed by running consensus. If this much use of consensus is still too expensive, the solution is hierarchical leases. Run consensus once to elect a czar C and give C a lease on a large part of the state. Now C gives out sub-leases on x and y to masters. Each master controls its own re- sources. The masters renew their sub-leases with the czar. This is cheap since it doesn’t require any coordination. The czar renews its lease by consensus. This costs more, but there’s only one czar lease. Also, the czar can be simple and less likely to fail, so a longer lease may be acceptable.
Hierarchical leases are commonly used in replicated file systems and in clusters.

By combining the ideas of consensus, leases, and hierarchy, it’s possible to build highly available systems that are also highly efficient.
"""

me(jea): There are only two types of servers: the Raft consensus servers, and the masters. We will refer to the set of the Raft consensus servers as just the monolithic "Raft system". When the masters come up they try to write to a pre-configured Raft key, the "czar" key. The first writer wins and takes a lease on the key. The subsequent masters do not win the lease and instead the content of the key with the lease holder and its IP address are returned. The masters then contact the czar (just a master who was first to lease/write the key), and then tell the czar about themselves. The czar keeps the membership list up to date, as masters lease or die and fail to renew their leases with the czar. When the czar dies the whole process repeats, starting from writing (or reading on write-failure) to the czar key in Raft. So masters make 2 network calls: first to Raft to see if they are czar or who is czar, and then to the czar to register and maintain their leases in the Czar's RAM/memory.


Claude.ai:

This is elegant. Let me work through it:

What works brilliantly:

Bootstrap problem solved: The "czar" key in Raft is the single durable anchor point. Every master knows to look there, so discovery is automatic.

Efficient normal operation: Once a master finds the czar, all subsequent communication is directly master-to-czar (RAM-based, cheap). The expensive Raft write happens only once during czar election, not on every membership change.

Fault tolerance: When the czar dies, masters detect the failure (lease timeout or failed renewal), and the next master to write to the czar key in Raft becomes the new czar. The Raft system itself provides consensus for the czar election.

Minimal Raft usage: Only the single czar key uses Raft. All the sub-lease renewals and membership updates stay out of consensus.

One potential issue I'd raise:

When a master tries to write to the czar key and loses the write race, you say it gets back "the content of the key with the lease holder and its IP address." But what if the czar crashes between when the master reads that value and when the master tries to contact it? The master would attempt to contact a dead czar.

The master could detect this through timeout and retry the czar key read (which would now return a new czar), but there's a window of stale information. Is that acceptable, or do you envision something else?

Otherwise, I think this solves Lampson's unsaid problem quite well. You've essentially made the Raft system's role minimal and surgical—just enough consensus to elect a czar, nothing more.

me(jea): The window of stale information isn't wrong information, just that the czar is unreachable, so that means reduced availability but not incorrect processing, right?

Claude.ai:

Exactly right. The master learns the czar's address from Raft, goes to contact it, and finds it unreachable. The master times out and detects the failure, then retries the read from the czar key in Raft to find the new czar.

During that window, the system has reduced availability (the master can't renew its lease or register), but there's no correctness problem. The information isn't stale in the sense of being wrong—it's just that the endpoint isn't reachable.

The master's lease with the old czar will eventually expire anyway, bounding how long the master can operate on potentially outdated assumptions. Once the master contacts the new czar, it re-establishes its leases and everything is consistent again.

This is actually a clean separation of concerns: Raft provides consistency (who is the czar), and leases provide liveness/availability (detecting when contact is lost). A temporarily unreachable czar causes an availability blip, not a consistency violation.

Your scheme elegantly minimizes the Raft footprint to just the election mechanism while keeping all the efficient, fast sub-lease operations out of consensus. That's exactly the efficiency gain Lampson was after, but with a concrete answer to the bootstrap and recovery problem that his text left implicit.

me(jea): We do need to keep the end of the master leases within the end of the Czar lease, right?

Claude.ai:

Yes, absolutely. This is a critical constraint.

The czar's lease with Raft must **outlive** all of the masters' sub-leases with the czar. Otherwise, you could have a scenario where:

1. A master's sub-lease with the czar expires and the master stops operating (correctly)
2. But the czar's lease with Raft hasn't expired yet
3. So no new czar is elected
4. The system appears to have a valid czar that's actually dead (from the masters' perspective)

Or worse, the czar's lease expires and a new czar is elected, but some masters still think their old sub-leases are valid because their leases haven't expired yet. Now you have two potential czars in the masters' views.

**The constraint:** If the czar's lease duration is T_czar and a master's sub-lease duration is T_master, then T_master < T_czar.

The masters renew their sub-leases frequently enough that their leases reset well before the czar's lease expires. If the czar dies, all master sub-leases will expire before a new czar is elected. When the new czar comes up, all the old sub-leases are definitely dead, and the masters start fresh by querying the czar key in Raft.

This ensures a clean handoff between czar generations with no overlapping claims of authority.

me(jea): later addition: the czar might as well include all of the
other members that it knows about when it renews its lease. By
piggybacking that member list and saving it too with the czar
lease renewal, then recovery
by the next czar after failure can have a memberlist to start with.
It can get out of date, but up to the lease renewal duration,
but that could be okay, as the member epoch is stored with it.

In the meantime, while the czar is alive, once members (masters)
locate the czar, they only need to talk to it (and not to Raft).
As long as the czar is up/available, they do not need to go back
to the Raft servers.

Claude.ai

At the core, Reliable Membership just needs to:

a) Maintain a current view of who's in the group
b) Notify members when that view changes
c) Ensure all members converge to the same view

Given your hierarchical lease structure, here's a minimal design:
The Czar maintains the membership list in RAM. When a master:

Renews its sub-lease with the czar -> czar notes it's alive
Fails to renew -> czar's lease timeout detects it, czar removes it from membership
Crashes and comes back -> it re-registers with the current czar

For up-calls/alerts: The czar could maintain a version
number on the membership list (incremented each time
membership changes). When masters renew their sub-leases,
the czar includes the current membership version. If the
master's cached version is stale, the czar sends back
the full updated membership list.

That's it. No separate group membership protocol.
The lease renewal itself becomes the heartbeat, and
membership changes are piggybacked on the renewal response.

me(jea):

Each Hermes node just heartbeats to the Czar saying:
"I'm a member, and who else is a member and at what epoch?"
If the epoch changes, update the membership list
in the Hermes upcall.

If the Czar cannot be reached, in addition to heartbeats,
the Hermes node starts trying to become the Czar by
writing a lease through Raft to a pre-configured "czar" key.
Repeat until either the current Czar can be reached
or a new Czar is elected.

The current czar starts with the memberlist last saved
in raft, and increments the member epoch to be the
raft log index of the reply, so that it is always current.

We could have lost memberships, but the newer membership
number will cause those to be overwritten with the
latest current member epoch because the raft log index
is always the largest and most current member epoch.

We do need to use a 2 part tuple (Czar-lease-last-raft-index,
current membership version) so that additional
members can join/leave the current czar without having
to update through raft. When the raft index (first
part of the tuple chagnes), that of course wins over
all earlier membership versions from the older czar.

Claude.ai:

Ah, now I see it. That's the key refinement.

The tuple structure:

`(Czar-Lease-Last-Raft-Index, Current-Membership-Version)`

This allows:
- Czar-Lease-Last-Raft-Index: The Raft log index when the current czar took office (wrote to the czar key). This changes only when a new czar is elected.
- Current-Membership-Version: Incremented in the czar's RAM each time membership changes (a master joins, fails, renews). This does NOT require a Raft write.

Why this works:

1. When Czar-1 is alive with `(1000, 5)`, masters renew and membership version increments to `(1000, 6)`, `(1000, 7)`, etc.—all in czar RAM, no Raft overhead.

2. When Czar-1 crashes and Czar-2 is elected, Czar-2 writes to the czar key at Raft index 1001. It reads the last saved membership from Raft (which had the highest version from Czar-1, say `(1000, 10)`), and restarts with `(1001, 0)` or `(1001, 1)`.

3. A Hermes node comparing `(1000, 10)` vs `(1001, 1)` immediately sees that `1001 > 1000`, so the new czar's view _always trumps_ the old czar's view, regardless of the membership version number.

4. But within a single czar's tenure, `(1001, 5)` < `(1001, 7)`, so versions are properly ordered.

This is elegant: the first tuple element provides strong fencing across czar transitions, while the second element allows cheap updates during normal operation.

me(jea): but Czar lease renewal will increment the raft index without
changing the Czar so we want to the use czar key's LeaseEpoch instead.

## gemini on fencing token/versioing vs strict lease timeout subset (which can be impossible to provide):

Q: Since it seems very difficult to maintain that the master leases do not outlive the czar leases, I want to understand how to use a fencing token to avoid problems of 2 masters or 2 czars at once, even when the lease for a master outlives the lease for its czar. How is this possible?

gemini.ai:

The challenge you're highlighting is a classic problem in distributed systems: _clock drift_ or _process pauses_ (like GC pauses) can cause a leaseholder to believe it still holds authority when it actually doesn't.

If a Master's sub-lease outlives the Czar’s lease, you risk a "split-brain" scenario where a new Czar is elected and appoints a *new* Master for the same resource, while the *old* Master is still operating.

### The Mechanism: Fencing Tokens

To solve this without perfectly synchronized clocks, you shift the responsibility of validation from the _sender_ (the Master) to the _receiver_ (the storage or resource being modified). This is done using a _Fencing Token_.

#### 1. The Generation of the Token

The Fencing Token is typically a _monotonically increasing number_ (a counter) generated by the consensus service (the "Source of Truth").

* When a Czar is elected via consensus, they are issued a `Czar_Epoch`.
* When that Czar grants a sub-lease to a Master, they include a `Master_Token` which is derived from or paired with that `Czar_Epoch`.

#### 2. The Storage Guard

The resource being managed (e.g., a disk, a database row, or a file) must maintain a record of the _highest token it has ever seen_.

#### 3. The Validation Step

When a Master wants to write to the resource, it must include its `Master_Token` in the request. The resource performs a simple check:

* _If `Request_Token` < `Current_Highest_Token`:_ Reject the request. The Master is "stale" (a zombie).
* _If `Request_Token` >= `Current_Highest_Token`:_ Accept the request and update the `Current_Highest_Token`.

---

### How this handles the "Lease Outliving" Problem

Even if the old Master’s sub-lease technically hasn't "expired" according to its own faulty local clock, the system remains safe because:

1. _Czar Failure:_ The old Czar’s lease expires in the consensus layer.
2. _New Czar/Master:_ Consensus elects a _New Czar_ with a higher epoch (e.g., `Epoch 101`). This New Czar grants a sub-lease to a _New Master_ with `Token 101`.
3. _The Fence is Set:_ The New Master performs a write. The storage now records `Highest_Token = 101`.
4. _The Zombie Attack:_ The _Old Master_ (from `Epoch 100`) finally wakes up from a long GC pause. It still thinks its sub-lease is valid. It tries to write to storage using `Token 100`.
5. _The Rejection:_ The storage sees that `100 < 101` and rejects the write.

_The safety property no longer relies on time (the lease duration), but on the total ordering of the tokens._

---

### Implementation in Hierarchical Leases

In Lampson’s hierarchy, you can implement this by nesting the tokens:

| Component | Responsibility |
| --- | --- |
| _Consensus Layer_ | Increments the `Czar_Epoch` every time a new Czar is elected. |
| _Czar_ | Issues `Master_Tokens` that are unique and higher than previous ones (e.g., `Czar_Epoch.SubCounter`). |
| _Master_ | Attaches the full token to every I/O operation. |
| _Resource_ | Acts as the "Fencing Agent," rejecting any token lower than the one it last processed. |

This ensures that even if the hierarchy of *time* (leases) breaks down, the hierarchy of *order* (tokens) prevents data corruption.

Would you like to see a sequence diagram or a pseudocode example of how the storage layer handles these token checks?


Q: would not we want to deduplicate fencing tokens to by requiring strictly monotone increasing fencing token versions, rather than allowing equals/repeats?

gemini.ai:

You are exactly right. In a production-grade distributed system, you almost always want _strictly monotonic_ tokens () rather than just monotonically increasing ones ().

Allowing "equal" tokens creates a dangerous loophole: it assumes that a single Master is always behaving correctly and won't suffer from internal concurrency issues or "retroactive" out-of-order writes.

Here is why _strictly monotonic_ tokens are the standard for robust fencing:

### 1. Guarding Against Internal Retries and Reordering

If a Master uses the same token for its entire lease duration, it cannot protect against its own network issues.

* _The Scenario:_ A Master sends "Write A," which gets delayed in the network. The Master thinks it timed out and sends "Write B" (an update to A).
* _The Risk:_ If both use the same token, the storage accepts both. If "Write A" arrives *after* "Write B" due to a network fluke, it will overwrite the newer data with the older data.
* _The Fix:_ By incrementing the token for every single request (or using a sub-sequence number), the storage would see that "Write A" has a lower sequence than the already-processed "Write B" and reject it.

### 2. Precise "Fencing" of the Old Leader

The primary goal of fencing is to ensure that once a new leader is recognized, the old one is _instantly and permanently_ invalidated.

* If the New Master is granted `Token 101` and the storage accepts it, the "fence" is now set at `101`.
* If we allowed "equal to," and the Old Master somehow had `101` (due to a logic error or clock-sync-based token generation), it could still sneak writes in.
* By requiring strictly greater-than, the very first successful write by a New Master creates a "point of no return."

---

### The Two-Tiered Token Strategy

To make this work in the Hierarchical Lease model Lampson describes, systems often use a _composite token_ (sometimes called a "Lexicographical Token"):

The storage compares these as a pair:

1. _Check the `Czar_Epoch`:_ If a write comes in with a higher `Czar_Epoch`, it immediately trumps everything from previous Czars.
2. _Check the `Master_Sequence`:_ If the `Czar_Epoch` is the same as the last seen, the `Master_Sequence` must be _strictly greater_ than the last seen.

### When "Equal" is actually used (Idempotency)

There is one specific case where "Equal" is allowed: _Server-Side Idempotency_.
Some systems allow a write with the *exact same* token and *exact same* data to succeed if it's a retry of the same operation. This prevents the system from erroring out just because a network ACK was lost. However, for the purpose of _fencing_ (blocking a rival Master), strict monotonicity is the only way to guarantee safety.

---

### Summary Table: Monotonic vs. Strictly Monotonic

| Feature | Monotonic () | Strictly Monotonic () |
| --- | --- | --- |
| _Prevents Split-Brain?_ | Yes (mostly) | Yes (completely) |
| _Prevents Out-of-Order Writes?_ | No | Yes |
| _Handles Master Retries?_ | Risky (requires extra logic) | Safe (via unique sequence) |
| _Implementation Complexity_ | Lower | Higher (requires stateful counters) |

*/
