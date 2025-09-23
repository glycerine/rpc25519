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
func (node *TubeNode) HelperFindLeader(cfg *TubeConfig, contactName string, requireOnlyContact bool) (lastLeaderURL, lastLeaderName string, lastInsp *Inspection, reallyLeader bool) {

	if node.name != cfg.MyName {
		panicf("must have consistent node.name(%v) == cfg.MyName(%v)", node.name, cfg.MyName)
	}

	// contact everyone, get their idea of who is leader
	leaders := make(map[string]*set)

	ctx := context.Background()

	selfSurelyNotLeader := false

	defer func() {
		if lastLeaderName == node.name && selfSurelyNotLeader {
			// try not to mislead caller into thinking they
			// themselves are leader when they are not.
			lastLeaderName = ""
			lastLeaderURL = ""
			reallyLeader = false
		}
	}()

	var insps []*Inspection
	for name, addr := range cfg.Node2Addr {
		url := FixAddrPrefix(addr)

		var err error
		var insp *Inspection
		var leaderURL, leaderName string
		if name == node.name {
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
			_, insp, leaderURL, leaderName, _, err = node.GetPeerListFrom(ctx5sec, url, name)
			canc5()
			if leaderName == name {
				//vv("did GetPeerListFrom for name = '%v'; got leaderName='%v'", name, leaderName)
			} else {
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
			if leaderName != name {
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
				leaders[leaderName] = &set{nodes: []string{name}}
			} else {
				s.nodes = append(s.nodes, name)
			}
		}
	}
	// put together a transitive set of known/connected nodes...
	xtra := make(map[string]string)
	for _, ins := range insps {
		for name, url := range ins.CktAllByName {
			if name == node.name {
				continue // skip self, already done above.
			}
			_, skip := cfg.Node2Addr[name]
			if skip {
				// already contacted
				continue
			}
			surl, ok := xtra[name]
			if ok {
				if surl == "pending" {
					xtra[name] = url
				}
			} else {
				// avoid adding other clients/ourselves
				_, serviceName, _, _, err1 := rpc.ParsePeerURL(url)
				if err1 == nil && serviceName == TUBE_REPLICA {
					xtra[name] = url
				}
			}
		}
	}

	for name, url := range xtra {
		//vv("on xtra name='%v', url='%v'", name, url)
		if name == node.name {
			continue // skip self
		}

		if url == "pending" {
			continue
		}
		//url = FixAddrPrefix(url)
		ctx5sec, canc5 := context.WithTimeout(ctx, 5*time.Second)
		_, insp, leaderURL, leaderName, _, err := node.GetPeerListFrom(ctx5sec, url, name)
		//mc, insp, leaderURL, leaderName, _, err := node.GetPeerListFrom(ctx, url)
		canc5()
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
				leaders[leaderName] = &set{nodes: []string{name}}
			} else {
				s.nodes = append(s.nodes, name)
			}
		}
	}

	if len(leaders) > 1 {
		if contactName == "" {
			fmt.Printf("ugh. we see multiple leaders in our nodes\n")
			fmt.Printf("     --not sure which one to talk to...\n")
			for lead, s := range leaders {
				for _, n := range s.nodes {
					fmt.Printf("  '%v' sees leader '%v'\n", n, lead)
				}
			}
			os.Exit(1)
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
				fmt.Printf("warning: no leaders found and no cfg.InitialLeaderName; use -c to contact a specific node.\n")
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
	hostport := net.JoinHostPort(myHost, port)
	//netAddr := u.Scheme + "://" + hostport
	//return netAddr
	return hostport
}
