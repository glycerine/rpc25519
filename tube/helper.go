package tube

// helper for cmd/tubeadd, tuberm, tubels

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/glycerine/ipaddr"
	rpc "github.com/glycerine/rpc25519"
)

type set struct {
	nodes []string
}

// HelperFindLeader assists clients like
// tube, tubels, tuberm, tubeadd and tup
// with searching for the current leader
// at startup time.
//
// It connects to all nodes listed in cfg.Node2Addr, and
// inspects them looking for a leader using the
// GetPeerListFrom() call. For each circuit
// show in the inspection.CktAll returned, we
// also contact to them in turn to try extra hard to
// find a leader even if our static configuration
// loaded from disk into cfg.Node2Addr is
// unaware of them. node_4 for example in our
// example/local test rig is not listed
// the static configurations of the other
// nodes but can join and become leader.
//
// Note that the servers try to save these
// dynamically added nodes to state.Known to
// remember them even after a reboot.
func (node *TubeNode) HelperFindLeader(cfg *TubeConfig, contactName string, requireOnlyContact bool) (lastLeaderURL, lastLeaderName string, lastInsp *Inspection, reallyLeader bool, contacted []*Inspection, err error) {

	if node.name != cfg.MyName {
		panicf("must have consistent node.name(%v) == cfg.MyName(%v)", node.name, cfg.MyName)
	}

	// contact everyone, get their idea of who is leader
	leaders := make(map[string]*set)

	ctx := context.Background()

	selfSurelyNotLeader := false

	defer func() {
		// naw. leave nil if no leader.
		//if len(contacted) > 0 && insp == nil {
		//	insp = contacted[0]
		//}
		if lastLeaderName == node.name && selfSurelyNotLeader {
			// try not to mislead caller into thinking they
			// themselves are leader when they are not.
			lastLeaderName = ""
			lastLeaderURL = ""
			reallyLeader = false
		}
	}()

	var insps []*Inspection
	for remoteName, addr := range cfg.Node2Addr {
		url := FixAddrPrefix(addr)

		var err error
		var insp *Inspection
		var leaderURL, leaderName string
		if remoteName == node.name {
			// inspect self
			insp = node.Inspect()
			leaderName = insp.CurrentLeaderName
			leaderURL = insp.CurrentLeaderURL
			//vv("%v self inspection gave: leaderName = '%v'", node.name, leaderName)
			if leaderName == "" {
				selfSurelyNotLeader = true
				//vv("%v self insp says empty leaderName so setting selfSurelyNotLeader=true", node.name)
			}
		} else {
			ctx5sec, canc5 := context.WithTimeout(ctx, 5*time.Second)
			_, insp, leaderURL, leaderName, _, err = node.GetPeerListFrom(ctx5sec, url, remoteName)
			canc5()
			if err == nil && insp != nil {
				contacted = append(contacted, insp)
			}
			if leaderName == remoteName {
				//vv("did GetPeerListFrom for name = '%v'; got leaderName='%v'", name, leaderName)
			} else {
				// who we asked for (remoteName) and who
				// we were told was leader (leaderName)
				// are different, so we cannot really
				// trust their reports of leader identity--it
				// is just their last best guess--which
				// is not good enough for us here.
				if leaderName != "" {
					//vv("did GetPeerListFrom for name = '%v'; got leaderName='%v' -- but disallowing non-self reports, since they can be wrong!", name, leaderName) // did GetPeerListFrom for name = 'node_2'; got leaderName='node_0'
					leaderName = ""
				}
				continue
			}
		}
		if err != nil {
			//vv("skip '%v' b/c err = '%v'", name, err)
			continue
		}
		// INVAR: err == nil

		if leaderName != "" {
			// how can node_0: candidate leader = 'node_0'?
			// well, node_2 thinks that node_0 is the leader,
			// since it is configured as the default leader.
			// We need to only return leader we have gotten AE from,
			// or self who know are leader. Yeah maybe only
			// respect self-reports would be easier.
			if leaderName != remoteName {
				// only consider self-reports of leadership, not
				// reports from other nodes that may just have a
				// starting guess or 'hint' still.
				continue
			}
			if leaderName == node.name && selfSurelyNotLeader {
				continue // extra protection
			}
			//vv("%v: candidate leader = '%v', url = '%v", node.name, leaderName, leaderURL)
			insps = append(insps, insp)
			lastInsp = insp
			lastLeaderName = leaderName
			lastLeaderURL = leaderURL
			reallyLeader = true // else leaderName is empty string
			s := leaders[leaderName]
			if s == nil {
				leaders[leaderName] = &set{nodes: []string{remoteName}}
			} else {
				s.nodes = append(s.nodes, remoteName)
			}
		}
	}
	// put together a transitive set of known/connected nodes...
	xtra := make(map[string]string)
	for _, ins := range insps {
		both := mapUnion(ins.Known, ins.CktAllByName)
		for iname, url := range both {
			if iname == node.name {
				continue // skip self, already done above.
			}
			_, skip := cfg.Node2Addr[iname]
			if skip {
				// already contacted
				continue
			}
			surl, ok := xtra[iname]
			if ok {
				if surl == "pending" {
					xtra[iname] = url
				}
			} else {
				// avoid adding other clients/ourselves
				_, serviceName, _, _, err1 := rpc.ParsePeerURL(url)
				if err1 == nil && serviceName == TUBE_REPLICA {
					xtra[iname] = url
				}
			}
		}
	}

	for xname, url := range xtra {
		//vv("on xtra name='%v', url='%v'", name, url)
		if xname == node.name {
			continue // skip self
		}

		if url == "pending" {
			continue
		}
		//url = FixAddrPrefix(url)
		var insp *Inspection
		var leaderURL, leaderName string

		ctx5sec, canc5 := context.WithTimeout(ctx, 5*time.Second)
		_, insp, leaderURL, leaderName, _, err = node.GetPeerListFrom(ctx5sec, url, xname)
		//mc, insp, leaderURL, leaderName, _, err := node.GetPeerListFrom(ctx, url)
		canc5()
		if err == nil && insp != nil {
			contacted = append(contacted, insp)
		}

		if err != nil {
			continue
		}
		if leaderName != "" {
			lastLeaderName = leaderName
			lastLeaderURL = leaderURL
			reallyLeader = true
			lastInsp = insp
			pp("extra candidate leader = '%v', url = '%v", leaderName, leaderURL)
			s := leaders[leaderName]
			if s == nil {
				leaders[leaderName] = &set{nodes: []string{xname}}
			} else {
				s.nodes = append(s.nodes, xname)
			}
		}
	}

	if len(leaders) > 1 {
		if contactName == "" {
			errs := fmt.Sprintf("error: ugh. we see multiple leaders in our nodes\n     --not sure which one to talk to...\n")
			for lead, s := range leaders {
				for _, n := range s.nodes {
					errs += fmt.Sprintf("  '%v' sees leader '%v'\n", n, lead)
				}
			}
			err = fmt.Errorf("%v", errs)
			return
		}
	}
	if len(leaders) == 1 {
		if contactName == "" {
			if cfg.InitialLeaderName != "" &&
				cfg.InitialLeaderName != lastLeaderName {

				fmt.Printf("warning: ignoring default '%v' "+
					"because we see leader '%v'\n",
					cfg.InitialLeaderName, lastLeaderName)
			}
		} else {
			if lastLeaderName != contactName {
				if requireOnlyContact {
					fmt.Printf("abort: we see existing leader '%v'; conflicts with request -c %v\n", lastLeaderName, contactName)
					os.Exit(1)
				}
			}
		}
	} else {
		// INVAR: len(leaders) == 0
		if contactName == "" {
			if cfg.InitialLeaderName == "" {
				err = fmt.Errorf("error: no leaders found and no cfg.InitialLeaderName; use -c to contact a specific node.")
				//os.Exit(1)
				return
			} else {
				pp("based on cfg.InitialLeaderName we will try to contact '%v'", cfg.InitialLeaderName)
				lastLeaderName = cfg.InitialLeaderName
				addr := cfg.Node2Addr[lastLeaderName]
				lastLeaderURL = FixAddrPrefix(addr)
			}
		} else {
			lastLeaderName = contactName
			pp("based on -c we will try to contact '%v'", contactName)
			addr := cfg.Node2Addr[lastLeaderName]
			lastLeaderURL = FixAddrPrefix(addr)
		}
	}
	return
}

// from ":7000" -> "100.x.y.z:7000" for example; so
// we don't get stuck only on 127.0.0.1 loopback.
// hint might just be ":0" or ":7001" for example.
// for real network/prod clients, not simnet.
func GetExternalAddr(useQUIC bool, hint string) string {

	scheme := "tcp"
	if useQUIC {
		scheme = "udp"
	}

	// ipaddr.GetExternalIP() returns us an
	// external, non-loopback, IP address if
	// at all possible.
	myHost := ipaddr.GetExternalIP()

	hint1 := hint
	if !strings.Contains(hint, "://") {
		hint1 = scheme + "://" + hint
	}
	u, err := url.Parse(hint1)
	panicOn(err)
	port := u.Port()
	if port == "" || port == "0" {
		port = fmt.Sprintf("%v", ipaddr.GetAvailPort())
	}
	hn := u.Hostname()
	if hn != "" {
		myHost = hn // preserve specified IP
	}
	hostport := net.JoinHostPort(myHost, port)
	return hostport
}

func mapUnion(a, b map[string]string) (u map[string]string) {
	u = make(map[string]string)
	for k, v := range a {
		u[k] = v
	}
	for k, v := range b {
		u[k] = v
	}
	return
}
