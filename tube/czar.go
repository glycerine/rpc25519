package tube

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/glycerine/idem"
	rpc "github.com/glycerine/rpc25519"
)

//go:generate greenpack

// ReliableMembershipListType is the
// Vtype for the ReliableMembershipList
// encoded Value payloads.
// Note: must be different from the type, else
// the go compiler gets confused; i.e. we
// cannot have this just be ReliableMembershipList
// as that is the name of the type.
const ReliableMembershipListType string = "ReliableMembershipListType"

// Czar must have greenpack to use the rpc.Call system.

//go:generate greenpack

type czarState int

const (
	unknownCzarState czarState = 0
	amCzar           czarState = 1
	notCzar          czarState = 2
)

// Czar is the RAM/memory only maintainer of
// the Member system of membership. The acting Czar is elected
// via Raft by writing to the key "czar"
// in the configured Member.TableSpace.
type Czar struct {
	mut sync.Mutex

	Halt *idem.Halter `msg:"-"`

	members *ReliableMembershipList
	heard   map[string]time.Time

	// something for greenpack to serz
	// this the client of Tube, not rpc.
	// It represents the TubeNode of the
	// Czar when it is active as czar (having
	// won the lease on the hermes.czar key in Tube).
	TubeCliName string `zid:"0"`

	UpcallMembershipChangeCh chan *ReliableMembershipList `msg:"-"`

	t0                 time.Time
	declaredDeadDur    time.Duration
	memberHeartBeatDur time.Duration
}

func (s *Czar) setVers(v RMVersionTuple, list *ReliableMembershipList, t0 time.Time) error {
	s.mut.Lock()
	defer s.mut.Unlock()

	if v.VersionGT(&s.members.Vers) {
		// okay
	} else {
		return fmt.Errorf("error: RMVersionTuple must be monotone increasing, current='%v'; rejecting proposed new Vers '%v'", s.members.Vers, v)
	}

	s.members = list.Clone()
	s.members.Vers = v
	s.t0 = t0

	vv("end of setVers(v='%#v') s.members is now '%v')", v, s.members)
	select {
	case s.UpcallMembershipChangeCh <- s.members.Clone():
	default:
	}
	return nil
}

func (s *Czar) remove(droppedCli *rpc.ConnHalt) {
	s.mut.Lock()
	defer s.mut.Unlock()
	raddr := droppedCli.Conn.RemoteAddr().String()

	vv("Czar.remove() for raddr='%v'", raddr)

	// linear search, for now. TODO: map based lookup?
	// we could make the name key be the rpc.Client addr
	// and use PeerNames.Get2()...
	for name, detail := range s.members.PeerNames.All() {
		addr := removeTcp(detail.Addr)
		//vv("checking addr='%v' against raddr='%v'", addr, raddr)
		if addr == raddr {
			s.members.PeerNames.Delkey(name)
			s.members.Vers.Version++
			vv("remove dropped client '%v', vers='%#v'", name, s.members.Vers)
			select {
			case s.UpcallMembershipChangeCh <- s.members.Clone():
			default:
			}
			return
		}
	}
	vv("remove could not find dropped client raddr '%v'", raddr)
}

func (s *Czar) expireSilentNodes(skipLock bool) (changed bool) {
	now := time.Now()
	if !skipLock {
		s.mut.Lock()
		defer s.mut.Unlock()
	}
	defer func() {
		if changed {
			// just one notifcation for all the deletes we did.
			s.members.Vers.Version++
			select {
			case s.UpcallMembershipChangeCh <- s.members.Clone():
			default:
			}
		}
	}()

	for name := range s.members.PeerNames.All() {
		if name == s.TubeCliName {
			// we ourselves are obviously alive so
			// we don't bother to heartbeat to ourselves.
			continue
		}
		killIt := false
		lastHeard, ok := s.heard[name]
		if !ok {
			// if we have not been listening for heartbeats
			// for very long, give them a chance--we may
			// have just loaded them in from the czar key's value.
			uptime := time.Since(s.t0)
			if uptime > s.declaredDeadDur {
				killIt = true
				vv("expiring dead node '%v' -- would upcall membership change too. nothing heard after uptime = '%v'", name, uptime)
			}
		} else {
			been := now.Sub(lastHeard)
			if been > s.declaredDeadDur {
				killIt = true
				vv("expiring dead node '%v' -- would upcall membership change too. been '%v'", name, been)
			}
		}
		if killIt {
			changed = true
			delete(s.heard, name)
			// Omap.All allows delete in the middle of iteration.
			s.members.PeerNames.Delkey(name)
		}
	}
	return
}

func (s *Czar) Ping(ctx context.Context, args *PeerDetail, reply *ReliableMembershipList) error {

	// since the rpc system will call us on a
	// new goroutine, separate from the main goroutine,
	// we must lock to prevent data races.
	s.mut.Lock()
	defer s.mut.Unlock()

	orig := s.members.Vers

	if hdr, ok := rpc.HDRFromContext(ctx); ok {
		//vv("Ping, from ctx: hdr.Nc.LocalAddr()='%v'; hdr.Nc.RemoteAddr()='%v'", hdr.Nc.LocalAddr(), hdr.Nc.RemoteAddr()) // we want remote
		// critical: replace Addr with the rpc.Client of the czar
		// address, rather than the tube client peer server address.
		//vv("changing args.Addr from '%v' -> '%v'", args.Addr, hdr.Nc.RemoteAddr())
		args.Addr = hdr.Nc.RemoteAddr().String()
	} else {
		panic("must have rpc.HDRFromContext(ctx) set so we know which tube-client to drop when the rpc.Client drops!")
	}
	//vv("Ping called at cliName = '%v', since args = '%v'; orig='%#v'", s.CliName, args, orig)
	det, ok := s.members.PeerNames.Get2(args.Name)
	if !ok {
		//vv("args.Name('%v') is new, adding to PeerNames", args.Name)
		s.members.PeerNames.Set(args.Name, args)
		s.members.Vers.Version++
	} else {
		if detailsChanged(det, args) {
			//vv("args.Name('%v') details have changed, updating PeerNames", args.Name)
			s.members.PeerNames.Set(args.Name, args)
			s.members.Vers.Version++
		} else {
			//vv("args.Name '%v' already exists in PeerNames, det = '%v'", args.Name, det)
		}
	}
	*reply = *(s.members.Clone())

	s.heard[args.Name] = time.Now()
	s.expireSilentNodes(true) // true since mut is already locked.
	if s.members.Vers.Version != orig.Version {
		vv("Czar.Ping: membership has changed (was %#v; now %#v), is now: {%v}", orig, s.members.Vers, s.shortMemberSummary())
	}

	//vv("czar sees Czar.Ping(cliName='%v') called with args='%v', reply with current membership list, czar replies with ='%v'", s.cliName, args, reply)

	return nil
}

func (s *Czar) shortMemberSummary() (r string) {
	n := s.members.PeerNames.Len()
	r = fmt.Sprintf("[%v members; Vers:(CzarLeaseEpoch: %v, Version:%v)]{\n", n, s.members.Vers.CzarLeaseEpoch, s.members.Vers.Version)
	i := 0
	for name, det := range s.members.PeerNames.All() {
		r += fmt.Sprintf("[%02d] %v: %v\n", i, name, det.URL)
		i++
	}
	r += "}"
	return
}

func NewCzar(cli *TubeNode, hbDur time.Duration) *Czar {
	list := cli.NewReliableMembershipList()
	return &Czar{
		Halt:                     idem.NewHalter(),
		members:                  list,
		heard:                    make(map[string]time.Time),
		t0:                       time.Now(),
		declaredDeadDur:          hbDur * 3,
		memberHeartBeatDur:       hbDur,
		UpcallMembershipChangeCh: make(chan *ReliableMembershipList, 1000),
	}
}

//msgp:ignore Member

// Member provies a ReliableMembership service with these goals:
//
// a) Maintain a current view of who's in the group
// b) Notify members when that view changes
// c) Ensure all members converge to the same view
//
//		by providing the RMVersionTuple versioning.
//
//	 0. we must be pre-configured with the raft nodes addresses,
//	    so we can bootrstrap from them.
//
// 1) register my name and PeerDetail under table:hermes key:names/myname
// with a 20 second lease.
// renew it every 10 seconds or so.
//
// 2) Each Hermes node just heartbeats to the Czar saying:
// "I'm a member, and who else is a member and at what epoch?"
// If the epoch changes, update the membership list
// in the Hermes upcall. table:hermes key:czar
//
// 3) If the Czar cannot be reached, in addition to heartbeats,
// the Hermes node starts trying to become the Czar by
// writing a lease through Raft to a pre-configured "czar" key.
// Repeat until either the current Czar can be reached
// or a new Czar is elected. table:hermes key:czar will be the key.
type Member struct {

	// TableSpace is set by the NewMember() constructor.
	// The "czar" key at the root of this TableSpace
	// is used to elect/lease out the czar-ship (leader
	// of the membership who maintains current membership
	// in RAM and receives Ping calls from members to
	// maintain their membership).
	TableSpace string

	// UpcallMembershipChangeCh tells users about
	// changes in membership.
	UpcallMembershipChangeCh chan *ReliableMembershipList

	// Ready is closed when UpcallMembershipChangeCh is set
	// and the Member is ready to use (call Start(), then
	// wait for Ready to be closed).
	Ready chan struct{}
}

// NewMember creates a member of the given tableSpace.
// Users must call Start() and then wait until Ready is closed
// before accessing UpcallMembershipChangeCh to
// get membership changes.
func NewMember(tableSpace string) *Member {
	return &Member{
		TableSpace: tableSpace,
		Ready:      make(chan struct{}),
	}
}

// Start elects a Czar and manages the Member's membership
// in the TableSpace.
func (membr *Member) Start() {
	go membr.start()
}

func (membr *Member) start() {
	tableSpace := membr.TableSpace

	const quiet = false
	const isTest = false
	const useSimNet = false
	cliCfg, err := LoadFromDiskTubeConfig("member", quiet, useSimNet, isTest)
	panicOn(err)
	//vv("cliCfg = '%v'", cliCfg)
	tubeCliName := cliCfg.MyName

	vv("tubeCliName = '%v'", tubeCliName)

	cli := NewTubeNode(tubeCliName, cliCfg)
	err = cli.InitAndStart()
	panicOn(err)
	defer cli.Close()

	ctx := context.Background()
	var sess *Session
	for {
		leaderURL, leaderName, _, reallyLeader, _, err := cli.HelperFindLeader(cliCfg, "", false)
		panicOn(err)
		vv("got leaderName = '%v'; leaderURL = '%v'; reallyLeader='%v'", leaderName, leaderURL, reallyLeader)

		sess, err = cli.CreateNewSession(ctx, leaderURL)
		//panicOn(err) // panic: hmm. no leader known to me (node 'node_0')
		if err == nil {
			break
		}
		alwaysPrintf("got err from CreateNewSession, sleep 1 sec and try again: '%v'", err)
		time.Sleep(time.Second)
	}
	//vv("got sess = '%v'", sess)

	keyCz := "czar"

	var renewCzarLeaseCh <-chan time.Time

	leaseDurCz := time.Second * 10
	renewCzarLeaseDur := leaseDurCz / 2

	var cState czarState = unknownCzarState

	memberHeartBeatDur := time.Second * 10
	writeAttemptDur := time.Second * 5

	czar := NewCzar(cli, memberHeartBeatDur)
	czar.TubeCliName = tubeCliName

	cli.Srv.RegisterName("Czar", czar)

	// used by czar to notice when client drops
	// and change membership quickly. If the
	// client comes back, well, we just change
	// membership again.
	cli.Srv.NotifyAllNewClients = make(chan *rpc.ConnHalt, 1000)
	//vv("cli.Srv.NotifyAllNewClients = %p", cli.Srv.NotifyAllNewClients)

	// funnel all client disconnects down to one channel.
	funneler := newFunneler(czar.Halt)

	membr.UpcallMembershipChangeCh = czar.UpcallMembershipChangeCh

	// tell user it is safe to listen on
	// membr.UpcallMembershipChangeCh now.
	close(membr.Ready)

	myDetail := cli.GetMyPeerDetail()
	//vv("myDetail = '%v' for tubeCliName = '%v'", myDetail, tubeCliName)

	//var czarURL string
	//var czarCkt *rpc.Circuit

	var rpcClientToCzar *rpc.Client
	var rpcClientToCzarDoneCh chan struct{}
	var czarLeaseUntilTm time.Time

	var memberHeartBeatCh <-chan time.Time

	var expireCheckCh <-chan time.Time

	//halt := idem.NewHalter()
	//defer halt.Done.Close()

	var nonCzarMembers *ReliableMembershipList

	// TODO: handle needing new session, maybe it times out?
	// should survive leader change, but needs checking.

looptop:
	for {

		switch cState {
		case unknownCzarState:

			// find the czar. it might be me.
			// we try to write to the "czar" key with a lease.
			// first one there wins. everyone else reads the winner's URL.
			czar.mut.Lock()
			list := czar.members.Clone()
			czar.mut.Unlock()

			// start with the highest version list we can find.
			if nonCzarMembers != nil && nonCzarMembers.Vers.VersionGT(&list.Vers) {
				list = nonCzarMembers.Clone()

				// the lease czar key Vers version is garbage and
				// always overwritten
				// anyway with the LeaseEpoch -- used to create a new version,
				// so there is no need to bother to update it in the raft log.
			}

			// if we win the write race, we are the czar.
			list.CzarName = tubeCliName
			list.PeerNames.Set(tubeCliName, myDetail.Clone())

			bts2, err := list.MarshalMsg(nil)
			panicOn(err)

			czarTkt, err := sess.Write(ctx, Key(tableSpace), Key(keyCz), Val(bts2), writeAttemptDur, ReliableMembershipListType, leaseDurCz)

			if err == nil {
				czarLeaseUntilTm = czarTkt.LeaseUntilTm
				cState = amCzar
				expireCheckCh = time.After(5 * time.Second)
				vers := RMVersionTuple{
					CzarLeaseEpoch: czarTkt.LeaseEpoch,
					Version:        0,
				}
				t0 := time.Now()                   // since we took over as czar
				err = czar.setVers(vers, list, t0) // does upcall for us.
				panicOn(err)                       // non monotone version panics

				czar.mut.Lock()
				sum := czar.shortMemberSummary()
				czar.mut.Unlock()

				vv("err=nil on lease write. I am czar (tubeCliName='%v'), send heartbeats to tube/raft to re-lease the hermes/czar key to maintain that status. vers = '%#v'; czar='%v'", tubeCliName, vers, sum)
				renewCzarLeaseCh = time.After(renewCzarLeaseDur)
			} else {
				cState = notCzar
				czarLeaseUntilTm = czarTkt.LeaseUntilTm
				expireCheckCh = nil

				if czarTkt.Vtype != ReliableMembershipListType {
					panicf("why not ReliableMembershipListType back? got '%v'", czarTkt.Vtype)
				}

				// avoid re-use of prior pointed to values!
				nonCzarMembers = &ReliableMembershipList{}
				_, err = nonCzarMembers.UnmarshalMsg(czarTkt.Val)
				panicOn(err)
				select {
				case czar.UpcallMembershipChangeCh <- nonCzarMembers.Clone():
				default:
				}

				vv("I am not czar, did not write to key: '%v'; nonCzarMembers = '%v'", err, nonCzarMembers)
				// contact the czar and register ourselves.
			}

		case amCzar:
			select {
			case cliConnHalt := <-cli.Srv.NotifyAllNewClients:
				//vv("czar received on cli.Srv.NotifyAllNewClients, has new client '%v'", cliConnHalt.Conn.RemoteAddr())
				// tell the funneler to listen for it to drop.
				// It will notify us on clientDroppedCh below.
				select {
				case funneler.newCliCh <- cliConnHalt:
				case <-czar.Halt.ReqStop.Chan:
					vv("czar halt requested. exiting.")
					return
				}

			case dropped := <-funneler.clientDroppedCh:
				czar.remove(dropped)

			case <-expireCheckCh:
				changed := czar.expireSilentNodes(false)
				if changed {
					vv("Czar check for heartbeats: membership changed, is now: {%v}", czar.shortMemberSummary())
				}
				expireCheckCh = time.After(5 * time.Second)

			case <-renewCzarLeaseCh:
				czar.mut.Lock()
				bts2, err := czar.members.MarshalMsg(nil)
				czar.mut.Unlock()
				panicOn(err)

				czarTkt, err := sess.Write(ctx, Key(tableSpace), Key(keyCz), Val(bts2), writeAttemptDur, ReliableMembershipListType, leaseDurCz)
				panicOn(err)
				//vv("renewed czar lease, good until %v", nice(czarTkt.LeaseUntilTm))
				czarLeaseUntilTm = czarTkt.LeaseUntilTm

				renewCzarLeaseCh = time.After(renewCzarLeaseDur)
			case <-czar.Halt.ReqStop.Chan:
				vv("czar halt requested. exiting.")
				return
			}

		case notCzar:
			if rpcClientToCzar == nil {
				list := nonCzarMembers
				czarDetail, ok := list.PeerNames.Get2(list.CzarName)
				if !ok {
					panicf("list with winning czar did not include czar itself?? list='%v'", list)
				}
				vv("will contact czar '%v' at URL: '%v'", list.CzarName, czarDetail.URL)
				reply := &ReliableMembershipList{}

				ccfg := *cli.GetConfig().RpcCfg
				ccfg.ClientDialToHostPort = removeTcp(czarDetail.Addr)

				rpcClientToCzar, err = rpc.NewClient(tubeCliName+"_pinger", &ccfg)
				panicOn(err)
				err = rpcClientToCzar.Start()
				if err != nil {
					vv("could not contact czar, err='%v' ... might have to wait out the lease...", err)
					rpcClientToCzar.Close()
					rpcClientToCzar = nil
					rpcClientToCzarDoneCh = nil
					cState = unknownCzarState

					waitDur := czarLeaseUntilTm.Sub(time.Now()) + time.Second
					vv("waitDur= '%v' to wait out the current czar lease before trying again", waitDur)
					time.Sleep(waitDur)
					continue looptop
				}
				rpcClientToCzarDoneCh = rpcClientToCzar.GetHostHalter().Done.Chan

				//vv("about to rpcClientToCzar.Call(Czar.Ping, myDetail='%v')", myDetail)
				err = rpcClientToCzar.Call("Czar.Ping", myDetail, reply, nil)
				if err != nil {
					rpcClientToCzar.Close()
					rpcClientToCzar = nil
					rpcClientToCzarDoneCh = nil
					cState = unknownCzarState
					continue looptop
				}
				//vv("member(tubeCliName='%v') did rpc.Call to Czar.Ping, got reply='%v'", tubeCliName, reply)
				// store view of membership as non-czar
				if nonCzarMembers == nil || nonCzarMembers.Vers.VersionLT(&reply.Vers) {
					nonCzarMembers = reply
					select {
					case czar.UpcallMembershipChangeCh <- nonCzarMembers.Clone():
					default:
					}
				}
				memberHeartBeatCh = time.After(memberHeartBeatDur)
			}
			select {
			case <-rpcClientToCzarDoneCh:
				vv("direct client to czar dropped! rpcClientToCzarDoneCh closed.")
				rpcClientToCzar.Close()
				rpcClientToCzar = nil
				rpcClientToCzarDoneCh = nil
				cState = unknownCzarState
				continue looptop

			case <-memberHeartBeatCh:

				reply := &ReliableMembershipList{}

				err = rpcClientToCzar.Call("Czar.Ping", myDetail, reply, nil)
				//vv("member called to Czar.Ping, err='%v'", err)
				if err != nil {
					vv("connection refused to (old?) czar, transition to unknownCzarState and write/elect a new czar")
					rpcClientToCzar.Close()
					rpcClientToCzar = nil
					rpcClientToCzarDoneCh = nil
					cState = unknownCzarState
					continue
				}
				//vv("member called to Czar.Ping, got reply='%v'", reply)
				if nonCzarMembers == nil || nonCzarMembers.Vers.VersionLT(&reply.Vers) {
					nonCzarMembers = reply
					select {
					case czar.UpcallMembershipChangeCh <- nonCzarMembers.Clone():
					default:
					}
				}
				memberHeartBeatCh = time.After(memberHeartBeatDur)

			case <-czar.Halt.ReqStop.Chan:
				vv("czar halt requested. exiting.")
				return
			}
		}
	}
}

func removeTcp(s string) string {
	if strings.HasPrefix(s, "tcp://") {
		return s[6:]
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

// funneler allows us to listen for up to 65535 clients
// disconnecting from the czar by monitoring a single
// clientDroppedCh.
type funneler struct {
	newCliCh        chan *rpc.ConnHalt
	clientDroppedCh chan *rpc.ConnHalt
	clientConns     []reflect.SelectCase
	clientConnHalt  []*rpc.ConnHalt
}

func newFunneler(halt *idem.Halter) (r *funneler) {
	newCliCh := make(chan *rpc.ConnHalt)
	clientDroppedCh := make(chan *rpc.ConnHalt, 1024)
	r = &funneler{
		newCliCh:        newCliCh,
		clientDroppedCh: clientDroppedCh,

		// add an empty clientConnHalt[0] to keep
		// aligned with clientConns which always has newCliCh at [0],
		// (and the halter at [1]).
		clientConnHalt: []*rpc.ConnHalt{nil, nil},
	}
	r.clientConns = append(r.clientConns, reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(newCliCh),
	})
	r.clientConns = append(r.clientConns, reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(halt.ReqStop.Chan),
	})

	go func() {
		for {
			chosen, recv, _ := reflect.Select(r.clientConns)
			//vv("reflect.Select chosen='%v'", chosen)
			if chosen == 1 {
				// halt requested.
				return
			}
			if chosen == 0 {
				// new client arrives, listen on its Halt.Done.Chan
				connHalt := recv.Interface().(*rpc.ConnHalt)
				r.clientConnHalt = append(r.clientConnHalt, connHalt)

				r.clientConns = append(r.clientConns, reflect.SelectCase{
					Dir:  reflect.SelectRecv,
					Chan: reflect.ValueOf(connHalt.Halt.Done.Chan),
				})
				if len(r.clientConns) > 65536 {
					panicf("only 65536 recieves supported by reflect.Select!")
				}
				continue
			}
			// stop listening for it
			dropped := r.clientConnHalt[chosen]
			//vv("czar rpc.Client departs: '%v'", dropped.Conn.RemoteAddr())
			r.clientConnHalt = append(r.clientConnHalt[:chosen], r.clientConnHalt[(chosen+1):]...)
			r.clientConns = append(r.clientConns[:chosen], r.clientConns[(chosen+1):]...)
			// notify czar that server noticed a client disconnect.
			select {
			case clientDroppedCh <- dropped:
			default:
			}
		}
	}()
	return
}

// RMTuple is the two part Reliable Membership tuple.
// Compare the CzarLeaseEpoch first, then the Version.
// The CzarLeaseEpoch must go higher when the Czar
// changes through Raft, and the Version can increment
// when per-Czar members are added/lost.
// Used by cmd/member/member.go.
type RMVersionTuple struct {
	CzarLeaseEpoch int64 `zid:"0"`
	Version        int64 `zid:"1"`
}

// ReliableMembershipList is written under the czar key
// in Tube (Raft). Tube must set the Vers.CzarLeaseEpoch
// when it is written for us, since the submitting
// client won't know what that is. It depends on
// which write won the race and arrived first.
// Used by cmd/member/member.go.
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

func (i *RMVersionTuple) VersionLT(j *RMVersionTuple) bool {
	if i.CzarLeaseEpoch < j.CzarLeaseEpoch {
		return true
	}
	if i.CzarLeaseEpoch == j.CzarLeaseEpoch {
		return i.Version < j.Version
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

func (i *RMVersionTuple) VersionLTE(j *RMVersionTuple) bool {
	if i.CzarLeaseEpoch < j.CzarLeaseEpoch {
		return true
	}
	if i.CzarLeaseEpoch == j.CzarLeaseEpoch {
		return i.Version <= j.Version
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
