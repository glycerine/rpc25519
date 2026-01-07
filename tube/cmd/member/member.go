package main

import (
	//"bufio"
	"context"
	"flag"
	"fmt"
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

	Members *tube.ReliableMembershipList `zid:"0"`
	heard   map[string]time.Time
	cliName string
	t0      time.Time
}

var declaredDeadDur = time.Second * 25

func (s *Czar) expireSilentNodes(skipLock bool) (changed bool) {
	now := time.Now()
	if !skipLock {
		s.mut.Lock()
		defer s.mut.Unlock()
	}

	for name := range s.Members.PeerNames.All() {
		if name == s.cliName {
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
			s.Members.PeerNames.Delkey(name)
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

	orig := s.Members.Vers.Version
	//vv("Ping called at cliName = '%v', since args = '%v'", s.cliName, args)
	det, ok := s.Members.PeerNames.Get2(args.Name)
	if !ok {
		//vv("args.Name('%v') is new, adding to PeerNames", args.Name)
		s.Members.PeerNames.Set(args.Name, args)
		s.Members.Vers.Version++
	} else {
		if detailsChanged(det, args) {
			//vv("args.Name('%v') details have changed, updating PeerNames", args.Name)
			s.Members.PeerNames.Set(args.Name, args)
			s.Members.Vers.Version++
		} else {
			//vv("args.Name '%v' already exists in PeerNames, det = '%v'", args.Name, det)
		}
	}
	*reply = *(s.Members)

	s.heard[args.Name] = time.Now()
	changed := s.expireSilentNodes(true) // true since mut is already locked.
	if changed {
		s.Members.Vers.Version++
	}
	if s.Members.Vers.Version != orig {
		vv("Czar.Ping: membership has changed, is now: {%v}", s.shortMemberSummary())
	}

	//vv("czar sees Czar.Ping(cliName='%v') called with args='%v', reply with current membership list, czar replies with ='%v'", s.cliName, args, reply)

	return nil
}

func (s *Czar) shortMemberSummary() (r string) {
	n := s.Members.PeerNames.Len()
	r = fmt.Sprintf("[%v members]{", n)
	i := 0
	for name := range s.Members.PeerNames.All() {
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
		Members: list,
		heard:   make(map[string]time.Time),
		t0:      time.Now(),
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

	leaderURL, leaderName, _, reallyLeader, _, err := cli.HelperFindLeader(cliCfg, "", false)
	panicOn(err)
	vv("got leaderName = '%v'; leaderURL = '%v'; reallyLeader='%v'", leaderName, leaderURL, reallyLeader)

	sess, err := cli.CreateNewSession(ctx, leaderURL)
	panicOn(err)
	//vv("got sess = '%v'", sess)

	keyCz := "czar"
	tableHermes := "hermes"
	var renewCzarLeaseCh <-chan time.Time

	leaseDurCz := time.Minute
	renewCzarLeaseDur := leaseDurCz / 2

	var cState czarState = unknownCzarState

	czar := NewCzar(cli)
	czar.cliName = cliName

	cli.Srv.RegisterName("Czar", czar)

	myDetail := cli.GetMyPeerDetail()
	//vv("myDetail = '%v' for cliName = '%v'", myDetail, cliName)

	//var czarURL string
	//var czarCkt *rpc.Circuit

	var rpcClientToCzar *rpc.Client
	var czarLeaseUntilTm time.Time

	var memberHeartBeatCh <-chan time.Time
	memberHeartBeatDur := time.Second * 10
	writeAttemptDur := time.Second * 5

	var expireCheckCh <-chan time.Time

	halt := idem.NewHalter()
	defer halt.Done.Close()

	var nonCzarMembers *tube.ReliableMembershipList

	// TODO: handle needing new session, maybe it times out?
	// should survive leader change, but needs checking.
	for {

		switch cState {
		case unknownCzarState:

			// find the czar. it might be me.
			// we try to write to the "czar" key with a lease.
			// first one there wins. everyone else reads the winner's URL.
			list := czar.Members
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
				czar.t0 = time.Now() // since we took over as czar
				vers := tube.RMVersionTuple{
					CzarLeaseEpoch: czarTkt.LeaseEpoch,
					Version:        0,
				}
				list.Vers = vers
				vv("err=nil on lease write. I am czar (cliName='%v'), send heartbeats to tube/raft to re-lease the hermes/czar key to maintain that status. vers = '%#v'", cliName, vers)
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

				vv("I am not czar, did not write to key: '%v'; czar.Members = '%v'", err, nonCzarMembers)
				// contact the czar and register ourselves.
			}

		case amCzar:
			select {
			case <-expireCheckCh:
				changed := czar.expireSilentNodes(false)
				if changed {
					vv("Czar check for heartbeats: membership changed, is now: {%v}", czar.shortMemberSummary())
				}
				expireCheckCh = time.After(5 * time.Second)

			case <-renewCzarLeaseCh:
				czar.mut.Lock()
				list := czar.Members
				bts2, err := list.MarshalMsg(nil)
				panicOn(err)
				czar.mut.Unlock()

				czarTkt, err := sess.Write(ctx, tube.Key(tableHermes), tube.Key(keyCz), tube.Val(bts2), writeAttemptDur, tube.ReliableMembershipListType, leaseDurCz)
				panicOn(err)
				vv("renewed czar lease, good until %v", nice(czarTkt.LeaseUntilTm))
				czarLeaseUntilTm = czarTkt.LeaseUntilTm

				renewCzarLeaseCh = time.After(renewCzarLeaseDur)
			case <-halt.ReqStop.Chan:
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

				// ?want? uses same serverBaseID so simnet can group same host simnodes.
				rpcClientToCzar, err = rpc.NewClient(cliName+"_pinger", &ccfg)
				panicOn(err)
				err = rpcClientToCzar.Start()
				if err != nil {
					vv("could not contact czar, err='%v' ... might have to wait out the lease...", err)
					rpcClientToCzar.Close()
					rpcClientToCzar = nil
					cState = unknownCzarState

					waitDur := czarLeaseUntilTm.Sub(time.Now()) + time.Second
					vv("waitDur= '%v' to wait out the current czar lease before trying again", waitDur)
					time.Sleep(waitDur)
					continue
				}
				// TODO: arrange for: defer rpcClientToCzar.Close()
				//halt.AddChild(rpcClientToCzar.halt) // unexported .halt

				//vv("about to rpcClientToCzar.Call(Czar.Ping, myDetail='%v')", myDetail)
				err = rpcClientToCzar.Call("Czar.Ping", myDetail, reply, nil)
				if err != nil {
					rpcClientToCzar.Close()
					rpcClientToCzar = nil
					cState = unknownCzarState
					continue
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
			case <-memberHeartBeatCh:

				reply := &tube.ReliableMembershipList{}

				err = rpcClientToCzar.Call("Czar.Ping", myDetail, reply, nil)
				//vv("member called to Czar.Ping, err='%v'", err)
				if err != nil {
					vv("connection refused to (old?) czar, transition to unknownCzarState and write/elect a new czar")
					rpcClientToCzar.Close()
					rpcClientToCzar = nil
					cState = unknownCzarState
					continue
				}
				//vv("member called to Czar.Ping, got reply='%v'", reply)
				if err == nil {
					czar.Members = reply
				}

				memberHeartBeatCh = time.After(memberHeartBeatDur)

			case <-halt.ReqStop.Chan:
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
