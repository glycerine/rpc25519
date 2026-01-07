package main

import (
	//"bufio"
	"context"
	"flag"
	"fmt"
	//"io"
	"os"
	"strings"
	//"path/filepath"
	//"sort"
	"time"

	rpc "github.com/glycerine/rpc25519"
	//"github.com/glycerine/ipaddr"
	"github.com/glycerine/idem"
	"github.com/glycerine/rpc25519/tube"
	//"github.com/glycerine/rpc25519/tube/art"
)

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
	vv("cliCfg = '%v'", cliCfg)
	cliName := cliCfg.MyName

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
	vv("got sess = '%v'", sess)

	keyCz := "czar"
	tableHermes := "hermes"
	var renewCzarLeaseCh <-chan time.Time

	leaseDurCz := time.Minute
	renewCzarLeaseDur := leaseDurCz / 2

	var cState czarState = unknownCzarState
	var vers tube.RMVersionTuple
	list := cli.NewReliableMembershipList()
	myDetail := cli.GetMyPeerDetail()

	var czarURL string
	var czarCkt *rpc.Circuit

	var memberHeartBeatCh <-chan time.Time
	memberHeartBeatDur := time.Second * 10

	halt := idem.NewHalter()
	defer halt.Done.Close()

	for {
		switch cState {
		case unknownCzarState:

			// find the czar. it might be me.
			// we try to write to the "czar" key with a lease.
			// first one there wins. everyone else reads the winner's URL.
			list.CzarName = cliName // if we win the write race, we are the czar.
			list.PeerNames.Set(cliName, myDetail)
			list.Vers = vers
			bts2, err := list.MarshalMsg(nil)
			panicOn(err)

			czarTkt, err := sess.Write(ctx, tube.Key(tableHermes), tube.Key(keyCz), tube.Val(bts2), 0, tube.ReliableMembershipListType, leaseDurCz)

			if err == nil {
				cState = amCzar
				vers = tube.RMVersionTuple{
					CzarLeaseEpoch: czarTkt.LeaseEpoch,
					Version:        0,
				}
				vv("err=nil on lease write. I am czar, send heartbeats to tube/raft to re-lease the hermes/czar key to maintain that status. vers = '%#v'", vers)
				renewCzarLeaseCh = time.After(renewCzarLeaseDur)
			} else {
				cState = notCzar
				if czarTkt.Vtype != tube.ReliableMembershipListType {
					panicf("why not tube.ReliableMembershipListType back? got '%v'", czarTkt.Vtype)
				}
				_, err = list.UnmarshalMsg(czarTkt.Val)
				panicOn(err)

				vv("I am not czar, did not write to key: '%v'; vers='%#v'; list='%v'; \n with list.CzarName='%v'", err, vers, list, list.CzarName)
				// contact the czar and register ourselves.
			}

		case amCzar:
			select {
			case <-renewCzarLeaseCh:
				list.Vers = vers
				bts2, err := list.MarshalMsg(nil)
				panicOn(err)

				czarTkt, err := sess.Write(ctx, tube.Key(tableHermes), tube.Key(keyCz), tube.Val(bts2), 0, tube.ReliableMembershipListType, leaseDurCz)
				panicOn(err)
				vv("renewed czar lease, good until %v", nice(czarTkt.LeaseUntilTm))
				renewCzarLeaseCh = time.After(renewCzarLeaseDur)
			case <-halt.ReqStop.Chan:
				return

				// case handle request to register new member, and update
				// the list that we periodically write to Tube/Raft.
			}

		case notCzar:
			if czarCkt == nil {
				czarDetail, ok := list.PeerNames.Get2(list.CzarName)
				if !ok {
					panicf("list with winning czar did not include czar itself?? list='%v'", list)
				}
				vv("will contact czar '%v' at URL: '%v'", list.CzarName, czarDetail.URL)
				czarURL = czarDetail.URL

				heartBeatFrag := cli.MyPeer.NewFragment()
				heartBeatFrag.FragOp = tube.ReliableMemberHeartBeatToCzar
				heartBeatFrag.FragSubject = "ReliableMemberHeartBeatToCzar"
				heartBeatFrag.SetUserArg("URL", myDetail.URL)

				czarCkt, _, _, err = cli.MyPeer.NewCircuitToPeerURL("member-to-czar", czarURL, heartBeatFrag, 0)
				panicOn(err)
				vv("got circuit to czar: %v", czarCkt)
				cli.MyPeer.NewCircuitCh <- czarCkt // needed/desirable?

				memberHeartBeatCh = time.After(memberHeartBeatDur)
			}
			select {
			case <-memberHeartBeatCh:
				heartBeatFrag := cli.MyPeer.NewFragment()
				heartBeatFrag.FragOp = tube.ReliableMemberHeartBeatToCzar
				heartBeatFrag.FragSubject = "ReliableMemberHeartBeatToCzar"
				heartBeatFrag.SetUserArg("URL", myDetail.URL)

				err = cli.SendOneWay(czarCkt, heartBeatFrag, 0, 0)

				const connRefused = "connect: connection refused"
				if strings.Contains(err.Error(), connRefused) {
					vv("connection refused to (old?) czar, transition to unknownCzarState and write/elect a new czar")
					czarCkt = nil
					cState = unknownCzarState
					continue
				}
				memberHeartBeatCh = time.After(memberHeartBeatDur)

			case <-halt.ReqStop.Chan:
				return
			}
		}
	}
}
