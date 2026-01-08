package main

import (
	//"bufio"
	"context"
	"flag"
	"fmt"
	"reflect"
	//"io"
	"os"
	"strings"
	"sync"
	//"path/filepath"
	//"sort"
	"time"

	rpc "github.com/glycerine/rpc25519"
	//"github.com/glycerine/ipaddr"
	"github.com/glycerine/idem"
	"github.com/glycerine/rpc25519/tube"
	//"github.com/glycerine/rpc25519/tube/art"
)

//go:generate greenpack

var sep = string(os.PathSeparator)

type ConfigMember struct {
	ContactName string // -c name of node to contact
	Help        bool   // -h for help, false, show this help
	Verbose     bool   // -v verbose: show config/connection attempts.
}

func (c *ConfigMember) SetFlags(fs *flag.FlagSet) {
	fs.StringVar(&c.ContactName, "c", "", "name of node to contact (defaults to leader)")
	fs.BoolVar(&c.Help, "h", false, "show this help")
	fs.BoolVar(&c.Verbose, "v", false, "verbose diagnostics logging to stdout")
}

func (c *ConfigMember) FinishConfig(fs *flag.FlagSet) (err error) {
	return
}

func (c *ConfigMember) SetDefaults() {}

type czarState int

const (
	unknownCzarState czarState = 0
	amCzar           czarState = 1
	notCzar          czarState = 2
)

type Czar struct {
	mut sync.Mutex

	halt *idem.Halter

	members *tube.ReliableMembershipList
	heard   map[string]time.Time

	// something for greenpack to serz
	// this the client of Tube, not rpc.
	// It represents the TubeNode of the
	// Czar when it is active as czar (having
	// won the lease on the hermes.czar key in Tube).
	CliName string `zid:"0"`

	UpcallMembershipChangeCh chan *tube.ReliableMembershipList

	t0 time.Time
}

var declaredDeadDur = time.Second * 25

func (s *Czar) setVers(v tube.RMVersionTuple, list *tube.ReliableMembershipList, t0 time.Time) error {
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
		if name == s.CliName {
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
			if uptime > declaredDeadDur {
				killIt = true
				vv("expiring dead node '%v' -- would upcall membership change too. nothing heard after uptime = '%v'", name, uptime)
			}
		} else {
			been := now.Sub(lastHeard)
			if been > declaredDeadDur {
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

func (s *Czar) Ping(ctx context.Context, args *tube.PeerDetail, reply *tube.ReliableMembershipList) error {

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
	r = fmt.Sprintf("[%v members; Vers:(CzarLeaseEpoch: %v, Version:%v)]{", n, s.members.Vers.CzarLeaseEpoch, s.members.Vers.Version)
	i := 0
	for name := range s.members.PeerNames.All() {
		r += name
		i++
		if i < n {
			r += ", "
		}
	}
	r += "}"
	return
}

func NewCzar(cli *tube.TubeNode) *Czar {
	list := cli.NewReliableMembershipList()
	return &Czar{
		halt:    idem.NewHalter(),
		members: list,
		heard:   make(map[string]time.Time),
		t0:      time.Now(),

		UpcallMembershipChangeCh: make(chan *tube.ReliableMembershipList, 1000),
	}
}

func main() {
	cmdCfg := &ConfigMember{}

	fs := flag.NewFlagSet("member", flag.ExitOnError)
	cmdCfg.SetFlags(fs)
	fs.Parse(os.Args[1:])
	cmdCfg.SetDefaults()
	err := cmdCfg.FinishConfig(fs)
	panicOn(err)

	if cmdCfg.Verbose {
		verboseVerbose = true
		tube.VerboseVerbose.Store(true)
	}
	if cmdCfg.Help {
		fmt.Fprintf(os.Stderr, "member help:\n")
		fs.PrintDefaults()
		return
	}

	const quiet = false
	const isTest = false
	const useSimNet = false
	cliCfg, err := tube.LoadFromDiskTubeConfig("member", quiet, useSimNet, isTest)
	panicOn(err)
	//vv("cliCfg = '%v'", cliCfg)
	cliName := cliCfg.MyName

	vv("cliName = '%v'", cliName)

	cli := tube.NewTubeNode(cliName, cliCfg)
	err = cli.InitAndStart()
	panicOn(err)
	defer cli.Close()

	ctx := context.Background()
	var sess *tube.Session
	for {
		leaderURL, leaderName, _, reallyLeader, _, err := cli.HelperFindLeader(cliCfg, "", false)
		panicOn(err)
		vv("got leaderName = '%v'; leaderURL = '%v'; reallyLeader='%v'", leaderName, leaderURL, reallyLeader)

		sess, err = cli.CreateNewSession(ctx, leaderURL)
		//panicOn(err) // panic: hmm. no leader known to me (node 'node_0')
		if err == nil {
			break
		}
		vv("got err fro CreateNewSession, sleep 1 sec and try again: '%v'", err)
		time.Sleep(time.Second)
	}
	//vv("got sess = '%v'", sess)

	keyCz := "czar"
	tableHermes := "hermes"
	var renewCzarLeaseCh <-chan time.Time

	leaseDurCz := time.Second * 10
	renewCzarLeaseDur := leaseDurCz / 2

	var cState czarState = unknownCzarState

	czar := NewCzar(cli)
	czar.CliName = cliName

	cli.Srv.RegisterName("Czar", czar)

	// used by czar to notice when client drops
	// and change membership quickly. If the
	// client comes back, well, we just change
	// membership again.
	cli.Srv.NotifyAllNewClients = make(chan *rpc.ConnHalt, 1000)
	vv("cli.Srv.NotifyAllNewClients = %p", cli.Srv.NotifyAllNewClients)
	funneler := newFunneler()

	myDetail := cli.GetMyPeerDetail()
	//vv("myDetail = '%v' for cliName = '%v'", myDetail, cliName)

	//var czarURL string
	//var czarCkt *rpc.Circuit

	var rpcClientToCzar *rpc.Client
	var rpcClientToCzarDoneCh chan struct{}
	var czarLeaseUntilTm time.Time

	var memberHeartBeatCh <-chan time.Time
	memberHeartBeatDur := time.Second * 10
	writeAttemptDur := time.Second * 5
	declaredDeadDur = memberHeartBeatDur * 3

	var expireCheckCh <-chan time.Time

	//halt := idem.NewHalter()
	//defer halt.Done.Close()

	var nonCzarMembers *tube.ReliableMembershipList

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

			list.CzarName = cliName // if we win the write race, we are the czar.
			// without the Clone, myDetail was getting overwritten! by
			// the czar detail... wat?!? when we unmarshall below into
			// the czar.Members which did not re-allocate the pointer!??
			list.PeerNames.Set(cliName, myDetail.Clone())

			bts2, err := list.MarshalMsg(nil)
			panicOn(err)

			czarTkt, err := sess.Write(ctx, tube.Key(tableHermes), tube.Key(keyCz), tube.Val(bts2), writeAttemptDur, tube.ReliableMembershipListType, leaseDurCz)

			if err == nil {
				czarLeaseUntilTm = czarTkt.LeaseUntilTm
				cState = amCzar
				expireCheckCh = time.After(5 * time.Second)
				vers := tube.RMVersionTuple{
					CzarLeaseEpoch: czarTkt.LeaseEpoch,
					Version:        0,
				}
				t0 := time.Now() // since we took over as czar
				err = czar.setVers(vers, list, t0)
				panicOn(err) // non monotone versioning

				czar.mut.Lock()
				sum := czar.shortMemberSummary()
				czar.mut.Unlock()

				vv("err=nil on lease write. I am czar (cliName='%v'), send heartbeats to tube/raft to re-lease the hermes/czar key to maintain that status. vers = '%#v'; czar='%v'", cliName, vers, sum)
				renewCzarLeaseCh = time.After(renewCzarLeaseDur)
			} else {
				cState = notCzar
				czarLeaseUntilTm = czarTkt.LeaseUntilTm
				expireCheckCh = nil

				if czarTkt.Vtype != tube.ReliableMembershipListType {
					panicf("why not tube.ReliableMembershipListType back? got '%v'", czarTkt.Vtype)
				}

				// avoid re-use of prior pointed to values!
				nonCzarMembers = &tube.ReliableMembershipList{}
				_, err = nonCzarMembers.UnmarshalMsg(czarTkt.Val)
				panicOn(err)

				vv("I am not czar, did not write to key: '%v'; nonCzarMembers = '%v'", err, nonCzarMembers)
				// contact the czar and register ourselves.
			}

		case amCzar:
			select {
			case cliConnHalt := <-cli.Srv.NotifyAllNewClients:
				vv("czar received on cli.Srv.NotifyAllNewClients, has new client '%v'", cliConnHalt.Conn.RemoteAddr())
				// tell the funneler to listen for it to drop.
				// It will notify us on clientDroppedCh below.
				select {
				case funneler.newCliCh <- cliConnHalt:
				case <-czar.halt.ReqStop.Chan:
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

				czarTkt, err := sess.Write(ctx, tube.Key(tableHermes), tube.Key(keyCz), tube.Val(bts2), writeAttemptDur, tube.ReliableMembershipListType, leaseDurCz)
				panicOn(err)
				vv("renewed czar lease, good until %v", nice(czarTkt.LeaseUntilTm))
				czarLeaseUntilTm = czarTkt.LeaseUntilTm

				renewCzarLeaseCh = time.After(renewCzarLeaseDur)
			case <-czar.halt.ReqStop.Chan:
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
				//czarURL = czarDetail.URL

				// heartBeatFrag := cli.MyPeer.NewFragment()
				// heartBeatFrag.FragOp = tube.ReliableMemberHeartBeatToCzar
				// heartBeatFrag.FragSubject = "ReliableMemberHeartBeatToCzar"
				// heartBeatFrag.SetUserArg("URL", myDetail.URL)

				reply := &tube.ReliableMembershipList{}

				ccfg := *cli.GetConfig().RpcCfg
				ccfg.ClientDialToHostPort = removeTcp(czarDetail.Addr)

				rpcClientToCzar, err = rpc.NewClient(cliName+"_pinger", &ccfg)
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
				// TODO: arrange for: defer rpcClientToCzar.Close()
				//halt.AddChild(rpcClientToCzar.halt) // unexported .halt

				//vv("about to rpcClientToCzar.Call(Czar.Ping, myDetail='%v')", myDetail)
				err = rpcClientToCzar.Call("Czar.Ping", myDetail, reply, nil)
				if err != nil {
					rpcClientToCzar.Close()
					rpcClientToCzar = nil
					rpcClientToCzarDoneCh = nil
					cState = unknownCzarState
					continue looptop
				}
				//vv("member(cliName='%v') called to Czar.Ping, got reply='%v'", cliName, reply)
				// czarCkt, _, _, err = cli.MyPeer.NewCircuitToPeerURL("member-to-czar", czarURL, heartBeatFrag, 0)
				// panicOn(err)
				// vv("got circuit to czar: %v", czarCkt)
				// cli.MyPeer.NewCircuitCh <- czarCkt // needed/desirable?
				if err == nil {
					nonCzarMembers = reply
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

				reply := &tube.ReliableMembershipList{}

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
				if err == nil {
					nonCzarMembers = reply
				}

				memberHeartBeatCh = time.After(memberHeartBeatDur)

			case <-czar.halt.ReqStop.Chan:
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
func detailsChanged(a, b *tube.PeerDetail) bool {
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

func newFunneler() (r *funneler) {
	newCliCh := make(chan *rpc.ConnHalt)
	clientDroppedCh := make(chan *rpc.ConnHalt, 1024)
	r = &funneler{
		newCliCh:        newCliCh,
		clientDroppedCh: clientDroppedCh,

		// add an empty clientConnHalt[0] to keep
		// aligned with clientConns which always has newCliCh at [0].
		clientConnHalt: []*rpc.ConnHalt{nil},
	}
	r.clientConns = append(r.clientConns, reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(newCliCh),
	})

	go func() {
		for {
			chosen, recv, _ := reflect.Select(r.clientConns)
			//vv("reflect.Select chosen='%v'", chosen) // seen 0
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
