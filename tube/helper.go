package tube

// helper for tubeadd, tuberm, tubels

import (
	"context"
	"fmt"
	"os"
	"time"

	rpc "github.com/glycerine/rpc25519"
)

type set struct {
	nodes []string
}

func (node *TubeNode) HelperFindLeader(cfg *TubeConfig, contactName string, requireOnlyContact bool) (lastLeaderURL, lastLeaderName string, lastInsp *Inspection) {

	// contact everyone, get their idea of who is leader
	leaders := make(map[string]*set)

	ctx := context.Background()

	var insps []*Inspection
	for name, addr := range cfg.Node2Addr {
		url := FixAddrPrefix(addr)

		ctx5sec, canc5 := context.WithTimeout(ctx, 5*time.Second)
		_, insp, leaderURL, leaderName, _, err := node.GetPeerListFrom(ctx5sec, url)
		//mc, insp, leaderURL, leaderName, _, err := node.GetPeerListFrom(ctx, url)
		canc5()
		if err != nil {
			//pp("skip '%v' b/c err = '%v'", leaderName, err)
			continue
		}
		if leaderName != "" {
			pp("candidate leader = '%v', url = '%v", leaderName, leaderURL)
			insps = append(insps, insp)
			lastInsp = insp
			lastLeaderName = leaderName
			lastLeaderURL = leaderURL
			s := leaders[leaderName]
			if s == nil {
				leaders[leaderName] = &set{nodes: []string{name}}
			} else {
				s.nodes = append(s.nodes, name)
			}
		}
	}
	// put together a transition set of known/connected nodes...
	xtra := make(map[string]string)
	for _, ins := range insps {
		for name, url := range ins.CktAll {
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
		if url == "pending" {
			continue
		}
		//url = FixAddrPrefix(url)
		ctx5sec, canc5 := context.WithTimeout(ctx, 5*time.Second)
		_, insp, leaderURL, leaderName, _, err := node.GetPeerListFrom(ctx5sec, url)
		//mc, insp, leaderURL, leaderName, _, err := node.GetPeerListFrom(ctx, url)
		canc5()
		if err != nil {
			continue
		}
		if leaderName != "" {
			lastLeaderName = leaderName
			lastLeaderURL = leaderURL
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
				fmt.Printf("no leaders found and no cfg.InitialLeaderName; use -c to contact a specific node.\n")
				os.Exit(1)
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
