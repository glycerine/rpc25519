package tube

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/glycerine/idem"
	"github.com/glycerine/ipaddr"
	rpc "github.com/glycerine/rpc25519"
	//"github.com/glycerine/rpc25519/tube/art"
)

//go:generate greenpack

const tableHermes string = "hermes"

// the Vtype for the ReliableMembershipList
// encoded Value payloads.
// Note: must be different from the type, else
// the go compiler gets confused; i.e. we
// cannot have this just be ReliableMembershipList
// as that is the name of the type.
const ReliableMembershipListType string = "ReliableMembershipListType"

type RMember struct {

	// Cfg would usually be supplied. If not
	// set on Start() will we make our own.
	// We act as a TUBE_CLIENT and so to do
	// so will spin up our own TubeNode.
	Cfg *TubeConfig `msg:"-"`

	TubeNode *TubeNode `msg:"-"`

	MyPeer *rpc.LocalPeer `msg:"-"`

	Halt *idem.Halter `msg:"-"`

	sess *Session

	name string
	//serviceName string
	URL    string
	PeerID string

	amCzar   bool
	myDetail *PeerDetail

	rml *ReliableMembershipList

	PeerServiceName        string
	PeerServiceNameVersion string

	Srvname              string
	Srv                  *rpc.Server `msg:"-"`
	rpcServerAddr        net.Addr
	startupNodeUrlSafeCh *idem.IdemCloseChan
}

func NewRMember() (m *RMember, err error) {
	m = &RMember{
		startupNodeUrlSafeCh: idem.NewIdemCloseChan(),
	}
	err = m.setupConfigForTube()
	m.PeerServiceName = "reliable-member"
	return
}

// InitAndStart sets everything up and Start()s the node.
func (s *RMember) InitAndStart() error {

	// start the tube client
	s.TubeNode.InitAndStart()

	s.Srvname = "reliable_member_srv_" + s.name
	//s.srvname = s.name
	s.Srv = rpc.NewServer(s.Srvname, s.Cfg.RpcCfg)

	serverAddr, err := s.Srv.Start()
	panicOn(err)
	s.rpcServerAddr = serverAddr
	vv("%v RMember.InitAndStart() started srv at '%v'", s.name, serverAddr)

	err = s.Srv.PeerAPI.RegisterPeerServiceFunc(string(s.PeerServiceName), s.Start)
	panicOn(err)

	// coordinated shutdown
	// try having Cluster own instead?
	//s.Srv.GetHostHalter().AddChild(s.Halt) // already has parent if cluster owns it

	// racy so try not to touch 's' now, the Start() goro has it!
	_, err = s.Srv.PeerAPI.StartLocalPeer(context.Background(), string(s.PeerServiceName), s.PeerServiceNameVersion, nil, s.name, true)
	panicOn(err)

	// to avoid racing on reading s.URL and PeerID to wire
	// up the grid, wait for startupNodeUrlSafeCh to be closed.
	select {
	case <-s.startupNodeUrlSafeCh.Chan:
		//vv("TubeNode.InitAndStart end: node '%v' has started, at url '%v'", s.name, s.URL)
	case <-s.Halt.ReqStop.Chan:
		return ErrShutDown
	}
	return nil
}

func (s *RMember) Start(
	myPeer *rpc.LocalPeer,
	ctx0 context.Context,
	newCircuitCh <-chan *rpc.Circuit,

) (err0 error) {

	s.MyPeer = myPeer
	s.URL = myPeer.URL()
	s.PeerID = myPeer.PeerID
	if s.startupNodeUrlSafeCh != nil {
		s.startupNodeUrlSafeCh.Close()
	}

	// 0) we must be pre-configured with the raft nodes addresses,
	//    so we can bootrstrap from them.
	//
	s.findMyFriends(ctx0)

	// 1) register my name and PeerDetail under table:hermes key:names/myname
	// with a 5 minute lease.
	// renew it every 4 minutes or so.
	periodicallyRenewNameLeaseCh := s.renewNameLease(ctx0)

	//
	// 2) Each Hermes node just heartbeats to the Czar saying:
	// "I'm a member, and who else is a member and at what epoch?"
	// If the epoch changes, update the membership list
	// in the Hermes upcall. table:hermes key:czar
	//

	// find the czar. it might be me.
	// we try to write to the "czar" key with a lease.
	// first one there wins. everyone else reads the winner's URL.
	list := s.TubeNode.NewReliableMembershipList()
	list.CzarName = s.name // if we win the write race, we are the czar.
	list.PeerNames.Set(s.name, s.myDetail)
	bts2, err := list.MarshalMsg(nil)
	panicOn(err)

	keyCz := "czar"
	leaseDurCz := time.Minute
	czarTkt, err := s.sess.Write(ctx0, Key(tableHermes), Key(keyCz), Val(bts2), 0, ReliableMembershipListType, leaseDurCz)
	panicOn(err)
	_ = czarTkt

	vers := RMVersionTuple{
		CzarLeaseEpoch: czarTkt.LeaseEpoch,
		Version:        0,
	}

	if err == nil {
		// I am czar, send heartbeats to tube/raft to re-lease
		// the hermes/czar key to maintain that status.
		s.amCzar = true

		// Since I am czar, my main job is to maintain the list
		// of members, and to update all other members when some
		// member leaves or a new member joins.
		list.Vers = vers
		s.rml = list

	} else {
		// some other node is czar, contact them.
		s.amCzar = false
		// Heartbeat to them regularly that we are online,
		// and want to participate as a Hermes node.

		if czarTkt.Vtype != ReliableMembershipListType {
			panicf("czarTkt got back Vtype '%v' not '%v'", czarTkt.Vtype, ReliableMembershipListType)
		}
		rml := &ReliableMembershipList{}
		_, err := rml.UnmarshalMsg(czarTkt.Val)
		panicOn(err)
		rml.Vers = vers
		vv("we see that czar is '%v'", rml.CzarName)
		czarDet, ok := rml.PeerNames.Get2(rml.CzarName)
		if !ok {
			panicf("czar '%v' did not include their own contact details", rml.CzarName)
		}
		s.rml = rml

		heartBeatFrag := s.TubeNode.newFrag()
		heartBeatFrag.FragOp = ReliableMemberHeartBeatToCzar
		heartBeatFrag.FragSubject = "heartbeat Hermes ReliableMember"
		heartBeatFrag.SetUserArg("URL", s.myDetail.URL)
		ckt, _, _, err := s.MyPeer.NewCircuitToPeerURL("czar", czarDet.URL, heartBeatFrag, 0)
		panicOn(err)
		_ = ckt
	}

	// 3) If the Czar cannot be reached, in addition to heartbeats,
	// the Hermes node starts trying to become the Czar by
	// writing a lease through Raft to a pre-configured "czar" key.
	// Repeat until either the current Czar can be reached
	// or a new Czar is elected. /names/hermes/czar/ will be the key.
	//
	// Reliable Membership goals:
	//
	// a) Maintain a current view of who's in the group
	// b) Notify members when that view changes
	// c) Ensure all members converge to the same view
	//    by providing the RMVersionTuple versioning.

	defer func() {
		r := recover()
		//vv("%v: (%v) end of RMember.Start() inside defer, about to return/finish; recover='%v'", s.me(), myPeer.ServiceName(), r)

		s.TubeNode.Close() // shut down TubeNode TUBE_CLIENT

		s.MyPeer.Close()
		s.Halt.Done.Close()
		if r != nil {
			panic(r)
		}
	}()

	done0 := ctx0.Done()
	for i := 0; ; i++ {
		select {
		case <-periodicallyRenewNameLeaseCh:
			// time to renew name lease with Tube
			periodicallyRenewNameLeaseCh = s.renewNameLease(ctx0)

		case <-s.Halt.ReqStop.Chan:
			//vv("%v shutdown initiated, s.Halt.ReqStop seen", s.me())
			//s.shutdown()
			return rpc.ErrHaltRequested

		case ckt := <-newCircuitCh:
			_ = ckt
			//vv("%v ckt := <-newCircuitCh, from ckt.RemotePeerName='%v'; ckt.RemotePeerID='%v'", s.me(), ckt.RemotePeerName, ckt.RemotePeerID)
			//err := s.handleNewCircuit(ckt, done0, arrivingNetworkFrag, cktHasError, cktHasDied)
			//if err != nil {
			//	return err
			//}
		case <-done0:
			//s.ay("%v <-done0", s.me())

			//vv("%v: done0! myPeer closing up b/c context canceled", s.name)
			return rpc.ErrContextCancelled

		} // end select
	} // end for i
	return nil
}

func (s *RMember) Close() {
	s.Halt.ReqStop.Close()
}

func (s *RMember) setupConfigForTube() error {

	// get/create a config
	dir := GetConfigDir()
	pathCfg := dir + "/" + "rm.default.config"
	if fileExists(pathCfg) {
		vv("rm using config file: '%v'", pathCfg)
	} else {
		fd, err := os.Create(pathCfg)
		panicOn(err)
		cfg := &TubeConfig{
			// distinguish multiple tup clients
			MyName:          "rm_" + CryRand15B(),
			PeerServiceName: TUBE_CLIENT,
		}
		fmt.Fprintf(fd, "%v\n", cfg.SexpString(nil))
		fd.Close()
		err = fmt.Errorf("rm error: no config file. Created one from template in '%v'. Please complete it.\n", pathCfg)
		panicOn(err)
		return err
	}
	by, err := os.ReadFile(pathCfg)
	panicOn(err)
	pp("got by = '%v'", string(by))

	cfg, err := NewTubeConfigFromSexpString(string(by), nil)
	panicOn(err)
	// distinguish multiple rm clients
	s.Cfg = cfg
	cfg.MyName = "rm_" + CryRand15B()
	cfg.PeerServiceName = TUBE_CLIENT

	myHost := ipaddr.GetExternalIP()
	myPort := ipaddr.GetAvailPort()
	cfg.RpcCfg.ServerAddr = fmt.Sprintf("%v:%v", myHost, myPort)

	// set up our config
	const quiet = false
	const isTest = false
	cfg.Init(quiet, isTest)

	cfg.UseSimNet = false

	cfg.RpcCfg.TCPonly_no_TLS = cfg.TCPonly_no_TLS
	cfg.RpcCfg.ServerAutoCreateClientsToDialOtherServers = true
	cfg.RpcCfg.QuietTestMode = true

	cfg.ClientProdConfigSaneOrPanic()

	cfg.ConvertToExternalAddr()

	vv("rm findMyFriends/setupConfig: cfg = '%v'", cfg.ShortSexpString(nil))
	return nil
}

func (s *RMember) renewNameLease(ctx0 context.Context) <-chan time.Time {
	s.name = s.Cfg.MyName
	key := "names/" + s.name
	nameLeaseDur := time.Minute * 5
	nameLeaseRenewDur := time.Minute * 2
	periodicallyRenewNameLeaseCh := time.After(nameLeaseRenewDur)

	detail := &PeerDetail{
		Name:                   s.name,
		URL:                    s.MyPeer.URL(),
		PeerID:                 s.MyPeer.PeerID,
		Addr:                   s.MyPeer.BaseServerAddr,
		PeerServiceName:        s.MyPeer.PeerServiceName,
		PeerServiceNameVersion: s.MyPeer.PeerServiceNameVersion,
	}
	s.myDetail = detail
	detailBits, err := s.myDetail.MarshalMsg(nil)
	panicOn(err)
	nameTkt, err := s.sess.Write(ctx0, Key(tableHermes), Key(key), Val(detailBits), 0, "PeerDetail", nameLeaseDur)
	panicOn(err)
	_ = nameTkt

	return periodicallyRenewNameLeaseCh
}

func (s *RMember) findMyFriends(ctx context.Context) (err error) {

	cfg := s.Cfg

	//nodeID := rpc.NewCallID("")
	name := cfg.MyName

	// maybe we don't need a full tube node?? just
	// an rpc25519 client might do.
	node := NewTubeNode(name, cfg)

	// start TUBE_CLIENT TubeNode.
	err = node.InitAndStart()
	panicOn(err)

	s.TubeNode = node

	// Use HelperFindLeader for better chance of locating a leader

	// If requireOnlyContact is true,
	// then HelperFindLeader will
	// immediately exit(1) if the contactName is
	// not also the current leader.
	const requireOnlyContact = false
	contactName := ""
	leaderURL, leaderName, _, reallyLeader, _, err := node.HelperFindLeader(cfg, contactName, requireOnlyContact)
	_ = leaderName
	panicOn(err)

	if false { // needed?
		// try to fix the sporadic race where (see 710 client_test:422 )
		// cli might not have updated its own s.leaderName !?!
		// and so sends the next Read off into the void at the old dead leader.
		// getCircuitToLeader sets the updated s.leaderName.
		//ckt2, onlyPossibleAddr2, _, err2 :=
		_, _, _, err2 := node.getCircuitToLeader(ctx, leaderURL, nil, false)
		panicOn(err2)
	}
	if !reallyLeader {
		panicf("could not really find Tube/Raft leader; leaderName='%v'; leaderURL = '%v'; reallyLeader=%v", leaderName, leaderURL, reallyLeader)
	}

	// note: if we must contact new peers, use this pattern:
	//s.contactNewPeer(url) // defined below

	if !reallyLeader {
		panicf("could not really find leader; leaderName='%v'; leaderURL = '%v'", leaderName, leaderURL)
	}

	// when no leader, we hang, our tkt in awaitingLeader.
	vv("%v: rm calling node.CreateNewSession(leaderURL = '%v')", cfg.MyName, leaderURL)
	sess, err := node.CreateNewSession(ctx, leaderURL)
	panicOn(err)
	//defer sess.Close()
	//pp("back from node.CreateNewSession(leaderURL='%v')", leaderURL)

	s.sess = sess
	return
}

func (s *RMember) needNewSess(ctx context.Context, sess *Session, leaderURL string, err error) (s2 *Session) {
	if err == nil {
		return sess
	}
	errs := err.Error()
	if strings.Contains(errs, "call CreateNewSession first") {
		sess.Close()
		s2, err := s.TubeNode.CreateNewSession(ctx, leaderURL)
		panicOn(err)
		return s2
	}
	return sess
}

// note: if we must contact new peers, use this pattern,
// where saving the ckt is critically important!
func (s *RMember) contactNewPeer(url string, firstFrag *rpc.Fragment) {
	ckt, _, _, _ := s.MyPeer.NewCircuitToPeerURL("rm-ckt", url, firstFrag, 0)
	if ckt != nil {
		select {
		case s.MyPeer.NewCircuitCh <- ckt:
		case <-s.MyPeer.Halt.ReqStop.Chan:
		}
	}
}

// RMTuple is the two part Reliable Membership tuple.
// Compare the CzarLeaseEpoch first, then the Version.
// The CzarLeaseEpoch must go higher when the Czar
// changes through Raft, and the Version can increment
// when per-Czar members are added/lost.
type RMVersionTuple struct {
	CzarLeaseEpoch int64 `zid:"0"`
	Version        int64 `zid:"1"`
}

// ReliableMembershipList is written under the czar key
// in Tube (Raft). Tube must set the Vers.CzarLeaseEpoch
// when it is written for us, since the submitting
// client won't know what that is. It depends on
// which write won the race and arrived first.
type ReliableMembershipList struct {
	CzarName string         `zid:"0"`
	Vers     RMVersionTuple `zid:"1"`

	PeerNames *Omap[string, *PeerDetail] `msg:"-"`

	SerzPeerDetails []*PeerDetail `zid:"2"`
}

func (s *ReliableMembershipList) Clone() (r *ReliableMembershipList) {
	r = &ReliableMembershipList{
		CzarName:  s.CzarName,
		Vers:      s.Vers,
		PeerNames: NewOmap[string, *PeerDetail](),
	}
	for name, det := range s.PeerNames.All() {
		r.PeerNames.Set(name, det.Clone())
	}
	return
}

func (s *ReliableMembershipList) String() (r string) {
	r = "&ReliableMembershipList{\n"
	r += fmt.Sprintf(" CzarName: \"%v\",\n", s.CzarName)
	r += fmt.Sprintf("     Vers: %#v,\n", s.Vers)
	r += fmt.Sprintf("PeerNames: (%v present)\n", s.PeerNames.Len())
	i := 0
	for _, det := range s.PeerNames.All() {
		r += fmt.Sprintf("[%02d] %v\n", i, det)
		i++
	}
	r += "}\n"
	return
}

func (i *RMVersionTuple) VersionGT(j *RMVersionTuple) bool {
	if i.CzarLeaseEpoch > j.CzarLeaseEpoch {
		return true
	}
	if i.CzarLeaseEpoch == j.CzarLeaseEpoch {
		return i.Version > j.Version
	}
	return false
}

func (i *RMVersionTuple) VersionGTE(j *RMVersionTuple) bool {
	if i.CzarLeaseEpoch > j.CzarLeaseEpoch {
		return true
	}
	if i.CzarLeaseEpoch == j.CzarLeaseEpoch {
		return i.Version >= j.Version
	}
	return false
}

func (i *RMVersionTuple) VersionEqual(j *RMVersionTuple) bool {
	return i.CzarLeaseEpoch == j.CzarLeaseEpoch &&
		i.Version == j.Version
}

func (i *RMVersionTuple) EpochsEqual(j *RMVersionTuple) bool {
	return i.CzarLeaseEpoch == j.CzarLeaseEpoch
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
	s.PeerNames = NewOmap[string, *PeerDetail]()
	for _, d := range s.SerzPeerDetails {
		s.PeerNames.Set(d.Name, d)
	}
	s.SerzPeerDetails = s.SerzPeerDetails[:0]
}

func (s *TubeNode) NewReliableMembershipList() *ReliableMembershipList {
	r := &ReliableMembershipList{
		PeerNames: NewOmap[string, *PeerDetail](),
	}
	if s == nil {
		panic("s cannot be nil")
	}
	return r
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

*/
