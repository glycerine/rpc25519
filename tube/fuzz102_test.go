package tube

import (
	"context"
	"fmt"
	"strconv"

	"os"
	"runtime"
	"strings"
	//"strconv"
	"sync"

	"sync/atomic"
	"testing"

	"github.com/glycerine/idem"

	rpc "github.com/glycerine/rpc25519"
)

// longer runs than 101 fuzz_test.go, 1k steps;
// 5 node cluser; 5 users. avg 10 scenarios in ~ 60 seconds.
func Test102_longer_userFuzz(t *testing.T) {
	//return

	if !faketime {
		alwaysPrintf("Test102_longer_userFuzz only works under synctest.")
		return
	}
	fmt.Printf("pid = %v\n", os.Getpid())
	runtime.GOMAXPROCS(1)
	showBinaryVersion("tube.test")
	fmt.Println("Go version:", runtime.Version())

	defer func() {
		vv("test 101 wrapping up.")
	}()

	begScenario := 0
	endxScenario := 1

	// for batch runs from env var partition.
	beg := os.Getenv("SCEN0")
	batchN := os.Getenv("SCEN_BATCH")
	if beg != "" && batchN != "" {
		vv("SCEN0 = '%v' ; SCEN_BATCH = '%v'", beg, batchN)
		var err error
		begScenario, err = strconv.Atoi(beg)
		panicOn(err)
		n, err := strconv.Atoi(batchN)
		panicOn(err)
		endxScenario = begScenario + n
	}

	numScen := endxScenario - begScenario
	vv("numScen = %v", numScen)

	var curClus *TubeCluster
	defer func() {
		r := recover()
		if r != nil {
			err, ok := r.(error)
			if ok && strings.Contains(err.Error(), "deadlock: main bubble goroutine has exited but blocked goroutines remain") {
				vv("augmenting panic with dump of halter tree; try and detect where hung...")
				for i, node := range curClus.Nodes {
					fmt.Printf("++++++++ curClus.Node[%v].Halt.RootString():\n %v \n\n\n", i, node.Halt.RootString())
				}
				panic(r)
			}
		}
	}()

	alwaysPrintf("begScenario = %v; endxScenario = %v", begScenario, endxScenario)
	for scenario := begScenario; scenario < endxScenario; scenario++ {

		seedString := fmt.Sprintf("%v", scenario)
		uint64seed, seedBytes := parseSeedString(seedString)
		int64seed := int64(uint64seed)
		//runtime.ResetDsimSeed(seed)

		if int64seed != int64(scenario) {
			panicf("got %v, wanted same scenario number back %v", int64seed, scenario)
		}
		rng := newPRNG(seedBytes)
		rnd := rng.pseudoRandNonNegInt64Range

		steps := 1000 // 1000 ok. 20 ok. 15 ok for one run; but had "still have a ticket in waiting"
		numNodes := 5
		// numUsers of 20 ok at 200 steps, but 30 users is
		// too much for porcupine at even just 30 steps.
		numUsers := 5
		//numUsers := 5 // green at 5 (10 steps)
		//numUsers := 9 // 7=>258 events, 8=>310 events, 9=>329 events,10=>366
		//numUsers := 15 // inf err loop at 15 (10 steps)

		if numScen <= 100 || scenario%10 == 0 {
			alwaysPrintf("top of seed/scenario = %v ; steps = %v ; numNodes = %v ; numUsers = %v", scenario, steps, numNodes, numUsers)
		}

		// only if we get non-linz twice on same, do we panic;
		// there is occassional false alarm at end due to ?
		var tryOk bool
		var tryErr error
	tryloop:
		for try := 0; try < 2; try++ {
			onlyBubbled(t, func(t *testing.T) {

				// get a determinstic seed into rpc.globalPRNG
				// so that early NewCallID() calls are deterministic too.
				rpc.PrepareForSimnet(int64seed, try > 0)

				// 1% or less collision probability, to minimize
				// rejection sampling and get unique write values quickly.
				domain := steps * numUsers * 100
				domainSeen := &sync.Map{}
				domainLast := &atomic.Int64{}

				forceLeader := 0
				cfg := NewTubeConfigTest(numNodes, t.Name(), faketime)
				//cfg.NoLogCompaction = true
				// otherwise we can test very frequent compaction
				// (but still under memwal.go):
				cfg.CompactionThresholdBytes = 1

				// using sim.SimpleNewScenario(intSeed) will
				// be too late, since alot of setup (the raft cluster) with the
				// default seed will have already been done, making
				// reproducibility harder. So we set the seed from
				// the creation of the simnet.
				cfg.InitialSimnetScenario = int64seed

				c, leaderName, leadi, _ := setupTestClusterWithCustomConfig(cfg, t, numNodes, forceLeader, 101)

				curClus = c

				leaderURL := c.Nodes[leadi].URL
				defer c.Close()

				//const skipTrafficTrue = true
				//snap := s.clus.SimnetSnapshot(skipTrafficTrue)
				sim := c.Nodes[0].cfg.RpcCfg.GetSimnet()
				if sim == nil {
					panic("why could not get simnet?")
				}

				c.Cfg.prng = rng
				for i := range c.Nodes {
					c.Nodes[i].cfg.prng = rng
				}

				nemesis := &fuzzNemesis{
					rng:          rng,
					rnd:          rnd,
					clus:         c,
					damaged:      make(map[int]int),
					allowTrouble: make(chan bool, 1),
				}
				// unlock it
				nemesis.allowTrouble <- true

				// if we limit to 100sec, then we can only get
				// in around 500 steps (with 15 users, 3 node cluster).
				//ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
				//defer cancel()
				ctx := context.Background()

				atomicLastEventID := &atomic.Int64{}
				var users []*fuzzUser
				shEvents := &sharedEvents{}
				for userNum := 0; userNum < numUsers; userNum++ {
					user := &fuzzUser{
						atomicLastEventID: atomicLastEventID,
						shEvents:          shEvents,
						t:                 t,
						seed:              int64seed,
						name:              fmt.Sprintf("user%v", userNum),
						userid:            userNum,
						numNodes:          numNodes,
						rnd:               rnd,
						clus:              c,
						halt:              idem.NewHalterNamed("fuzzUser"),
						nemesis:           nemesis,
					}
					users = append(users, user)

					// try to never create a whole new TubeNode;
					// should just be able to restart its sessions and connections!
					// otherwise the names in the simnet dns explode/overlap.
					cliName := fmt.Sprintf("client101_%v", user.name)
					cliCfg := *c.Cfg
					cliCfg.MyName = cliName
					cliCfg.PeerServiceName = TUBE_CLIENT
					cli := NewTubeNode(cliName, &cliCfg)
					user.cli = cli
					err := cli.InitAndStart()
					panicOn(err)
					defer cli.Close()

					//vv("userNum:%v -> cli.name = '%v'", userNum, cli.name)

					user.Start(ctx, steps, leaderName, leaderURL, domain, domainSeen, domainLast)
				}

				for _, user := range users {
					<-user.halt.Done.Chan
				}
				// the history is shared, wait until all are done or else
				// we can read back a value before we get a chance to
				// record the write into the op history, and the linz
				// checker will false alarm on that.
				tryErr = users[0].linzCheck()
				if tryErr == nil {
					tryOk = true
				}

				if nemesis.calls == 0 {
					panic("nemesis was never called!")
				}

				//vv("makeTrouble calls total = %v", nemesis.calls)

			}) // end onlyBubbled
			if tryOk {
				break tryloop
			}
		} // end for try twice
		if !tryOk {
			panicf("problem on scenario %v twice we got non-linz: '%v'", scenario, tryErr)
		}
		//vv("end scenario for loop")
	}
}
