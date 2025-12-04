package tube

import (
	//"context"
	"fmt"
	//"os"
	"math"
	"runtime"
	"runtime/debug"
	"runtime/trace"
	"strconv"
	"sync"
	//"sync/atomic"
	"testing"
	"time"

	//"github.com/glycerine/idem"

	porc "github.com/anishathalye/porcupine"
	rpc "github.com/glycerine/rpc25519"
)

var _ = trace.Stop
var _ = debug.SetMemoryLimit
var _ = math.MaxInt64

type fuzzFault int

const (
	fuzz_NOOP fuzzFault = iota
	fuzz_PAUSE
	fuzz_CRASH
	fuzz_PARTITON
	fuzz_CLOCK_SKEW
	fuzz_MEMBER_ADD
	fuzz_MEMBER_REMOVE
	fuzz_MEMBER_RESTART
	fuzz_SWIZZLE_CLOG
	fuzz_ONE_WAY_FAULT
	fuzz_ONE_WAY_FAULT_PROBABALISTIC
	fuzz_ADD_CLIENT
	fuzz_PAUSE_CLIENT
	fuzz_RESTART_CLIENT
	fuzz_TERMINATE_CLIENT
	fuzz_MISORDERED_MESSAGE
	fuzz_DUPLICATED_MESSAGE
	fuzz_HEAL_NODE

	fuzz_LAST
)

func (f fuzzFault) String() string {
	switch f {
	case fuzz_NOOP:
		return "fuzz_NOOP"
	case fuzz_PAUSE:
		return "fuzz_PAUSE"
	case fuzz_CRASH:
		return "fuzz_CRASH"
	case fuzz_PARTITON:
		return "fuzz_PARTITON"
	case fuzz_CLOCK_SKEW:
		return "fuzz_CLOCK_SKEW"
	case fuzz_MEMBER_ADD:
		return "fuzz_MEMBER_ADD"
	case fuzz_MEMBER_REMOVE:
		return "fuzz_MEMBER_REMOVE"
	case fuzz_MEMBER_RESTART:
		return "fuzz_MEMBER_RESTART"
	case fuzz_SWIZZLE_CLOG:
		return "fuzz_SWIZZLE_CLOG"
	case fuzz_ONE_WAY_FAULT:
		return "fuzz_ONE_WAY_FAULT"
	case fuzz_ONE_WAY_FAULT_PROBABALISTIC:
		return "fuzz_ONE_WAY_FAULT_PROBABALISTIC"
	case fuzz_ADD_CLIENT:
		return "fuzz_ADD_CLIENT"
	case fuzz_PAUSE_CLIENT:
		return "fuzz_PAUSE_CLIENT"
	case fuzz_RESTART_CLIENT:
		return "fuzz_RESTART_CLIENT"
	case fuzz_TERMINATE_CLIENT:
		return "fuzz_TERMINATE_CLIENT"
	case fuzz_MISORDERED_MESSAGE:
		return "fuzz_MISORDERED_MESSAGE"
	case fuzz_DUPLICATED_MESSAGE:
		return "fuzz_DUPLICATED_MESSAGE"
	case fuzz_HEAL_NODE:
		return "fuzz_HEAL_NODE"
	case fuzz_LAST:
		return "fuzz_LAST"
	}
	panic(fmt.Sprintf("unknown fuzzFault: %v", int(f)))
}

// user tries to get read/write work done.
type fuzzUser struct {
	opsMut sync.Mutex
	ops    []porc.Operation
	rnd    func(nChoices int64) (r int64)

	clus *TubeCluster
	sess *Session
}

func (s *fuzzUser) Write(key string, writeMe int) {

	begtmWrite := time.Now()

	op := porc.Operation{
		ClientId: 0,
		Input:    registerInput{op: REGISTER_PUT, value: writeMe},
		Call:     begtmWrite.UnixNano(), // invocation timestamp
		Output:   writeMe,
		//Return:   endtmWrite.UnixNano(), // response timestamp
	}

	v := []byte(fmt.Sprintf("%v", writeMe))
	vv("about to write at node 0, v = writeMe = %v: '%v'", writeMe, string(v))

	// WRITE
	tktW, err := s.clus.Nodes[0].Write(bkg, "", Key(key), v, 0, nil)

	switch err {
	case ErrShutDown, rpc.ErrShutdown2,
		ErrTimeOut, rpc.ErrTimeout:
		vv("write failed: '%v'", err)
		return
	}
	panicOn(err)
	endtmWrite := time.Now()
	_ = tktW

	op.Return = endtmWrite.UnixNano() // response timestamp

	s.opsMut.Lock()
	s.ops = append(s.ops, op)
	s.opsMut.Unlock()

	vv("write ok. len ops now %v", len(s.ops))
}

func (s *fuzzUser) Read(key string, jnode int) {

	begtmRead := time.Now()

	vv("reading from jnode=%v", jnode)
	waitForDur := time.Second
	tkt, err := s.clus.Nodes[jnode].Read(bkg, "", Key(key), waitForDur, s.sess)
	vv("jnode=%v, back from Read(key='%v') -> tkt.Val:'%v' (tkt.Err='%v')", jnode, key, string(tkt.Val), tkt.Err)

	if tkt != nil && tkt.Err != nil && tkt.Err.Error() == "key not found" {
		return
	}

	switch err {
	case ErrShutDown, rpc.ErrShutdown2,
		ErrTimeOut, rpc.ErrTimeout:
		return
	}
	panicOn(err)
	endtmRead := time.Now()

	v2 := tkt.Val
	i2, err := strconv.Atoi(string(v2))
	panicOn(err)

	op := porc.Operation{
		ClientId: jnode,
		Input:    registerInput{op: REGISTER_GET},
		Call:     begtmRead.UnixNano(), // invocation timestamp
		Output:   i2,
		Return:   endtmRead.UnixNano(), // response timestamp
	}

	s.opsMut.Lock()
	s.ops = append(s.ops, op)
	s.opsMut.Unlock()

}

// nemesis injects faults, makes trouble for user.
type fuzzNemesis struct {
	rng     *prng
	rnd     func(nChoices int64) (r int64)
	clus    *TubeCluster
	clients *[]TubeNode

	// so we keep a majority of nodes healthy,
	// per Raft requirements, track who is damaged currently here.
	damagedSlc []int
	damaged    map[int]int
}

func (s *fuzzNemesis) makeTrouble() {

	beat := time.Second

	nn := len(s.clus.Nodes)
	quorum := nn/2 + 1
	numDamaged := len(s.damagedSlc)
	maxDamaged := nn - quorum

	healProb := 0.1
	noopProb := 0.3
	pr := s.rng.float64prob()
	isHeal := pr <= healProb
	if !isHeal {
		isNoop := pr <= healProb+noopProb
		if isNoop {
			vv("isNoop true")
			time.Sleep(beat)
			return
		}
	} else {
		vv("isHeal true")
	}

	var r fuzzFault
	var node int

	if isHeal {
		if numDamaged == 0 {
			r = fuzz_NOOP
		} else {
			r = fuzz_HEAL_NODE
			// pick a damaged node at random to heal.
			which := int(s.rnd(int64(numDamaged)))
			node = s.damagedSlc[which]

			// remove node from s.damaged and s.damagedSlc
			where := s.damaged[node]
			s.damagedSlc = append(s.damagedSlc[:where], s.damagedSlc[where+1:]...)
			delete(s.damaged, node)
		}
	} else {
		// not healing, damaging
		if numDamaged < maxDamaged {
			// any node can take damage
			node = int(s.rnd(int64(nn)))

			_, already := s.damaged[node]
			if !already {
				s.damaged[node] = len(s.damagedSlc)
				s.damagedSlc = append(s.damagedSlc, node)
			}
		} else {
			// only an already damaged node can take more damage.
			if numDamaged > 0 {
				// pick from one of the already damaged nodes
				which := int(s.rnd(int64(numDamaged)))
				node = s.damagedSlc[which]
			}
		}
		r = fuzzFault(s.rnd(int64(fuzz_HEAL_NODE)))
	}
	vv("node = %v; r = %v", node, r)
	switch r {
	case fuzz_NOOP:
	case fuzz_PAUSE:
	case fuzz_CRASH:
	case fuzz_PARTITON:
		s.clus.IsolateNode(node)

		//s.clus.DeafDrop(deaf, drop map[int]float64)
		//s.clus.AllHealthy(powerOnAnyOff, deliverDroppedSends bool)
		//s.clus.AllHealthyAndPowerOn(deliverDroppedSends bool)

	case fuzz_CLOCK_SKEW:
	case fuzz_MEMBER_ADD:
	case fuzz_MEMBER_REMOVE:
	case fuzz_MEMBER_RESTART:
	case fuzz_SWIZZLE_CLOG:
	case fuzz_ONE_WAY_FAULT:

		probDrop := 1.0
		probDeaf := 1.0
		if s.rnd(2) == 1 {
			s.clus.Nodes[node].DropSends(probDrop)
		} else {
			s.clus.Nodes[node].DeafToReads(probDeaf)
		}
	case fuzz_ONE_WAY_FAULT_PROBABALISTIC:

		if s.rnd(2) == 1 {
			probDrop := s.rng.float64prob()
			vv("probDrop send = %v on node = %v", probDrop, node)
			s.clus.Nodes[node].DropSends(probDrop)
			return
		}
		probDeaf := s.rng.float64prob()
		vv("probDeaf to read = %v on node = %v", probDeaf, node)
		s.clus.Nodes[node].DeafToReads(probDeaf)

	case fuzz_ADD_CLIENT:
	case fuzz_PAUSE_CLIENT:
	case fuzz_RESTART_CLIENT:

	case fuzz_TERMINATE_CLIENT:
	case fuzz_MISORDERED_MESSAGE:
	case fuzz_DUPLICATED_MESSAGE:

	case fuzz_HEAL_NODE:
		var deliverDroppedSends bool
		if s.rnd(2) == 1 {
			deliverDroppedSends = true
		}
		s.clus.AllHealthyAndPowerOn(deliverDroppedSends)
	}
	time.Sleep(beat)
}

func Test099_fuzz_testing_linz(t *testing.T) {

	return
	// need to implement API like tup to restart/timeout sessions
	// and retry as a client api. See tube/cmd/tup/tup.go

	runtime.GOMAXPROCS(1)

	// automatically available after 1.25
	// GOEXPERIMENT=synctest
	//
	// GODEBUG=asyncpreemptoff=1
	// how can we turn off sysmon at runtime?
	//
	// set with runtime.ResetDsimSeed(seed) below
	// GO_DSIM_SEED = 1

	defer func() {
		vv("test 099 wrapping up.")
	}()

	maxScenario := 1
	for scenario := 0; scenario < maxScenario; scenario++ {

		seedString := fmt.Sprintf("%v", scenario)

		seed, seedBytes := parseSeedString(seedString)
		if int(seed) != scenario {
			panicf("got %v, wanted same scenario number back %v", int(seed), scenario)
		}

		// if we were starting a new live Go process from
		// scratch we would want to do this:
		//os.Setenv("GO_DSIM_SEED", seedString)
		// but...
		// since we are not staring a new process,
		// we still try to control the
		// runtime's initialization with the seed; i.e.
		// we are already running here!
		runtime.ResetDsimSeed(seed)

		rng := newPRNG(seedBytes)
		rnd := rng.pseudoRandNonNegInt64Range

		var ops []porc.Operation

		onlyBubbled(t, func(t *testing.T) {

			steps := 20
			_ = steps
			numNodes := 3

			forceLeader := 0
			c, leader, leadi, maxterm := setupTestCluster(t, numNodes, forceLeader, 99)
			_, _, _ = leader, leadi, maxterm
			defer c.Close()

			user := &fuzzUser{
				rnd:  rnd,
				clus: c,
			}
			sess, err := c.Nodes[0].CreateNewSession(bkg, c.Nodes[0].URL)
			panicOn(err)
			user.sess = sess

			nemesis := &fuzzNemesis{
				rng:     rng,
				rnd:     rnd,
				clus:    c,
				damaged: make(map[int]int),
			}
			_ = user
			_ = nemesis

			key := "key10"

			jnode := int(rnd(int64(numNodes)))
			user.Read(key, jnode)

			writeMe := 10
			user.Write(key, writeMe)

			nemesis.makeTrouble()

			jnode2 := int(rnd(int64(numNodes)))
			user.Read(key, jnode2)

			ops = user.ops
		})

		linz := porc.CheckOperations(registerModel, ops)
		if !linz {
			writeToDiskNonLinz(t, ops)
			t.Fatalf("error: expected operations to be linearizable! seed='%v'; ops='%v'", seed, opsSlice(ops))
		}

		vv("len(ops)=%v passed linearizability checker.", len(ops))
	}
}

func Test199_dsim_seed_string_parsing(t *testing.T) {
	// GO_DSIM_SEED env variable is parsed
	// with the same logic as parseSeedString() code.

	seed, seedBytes := parseSeedString("0")
	if seed != 0 {
		panicf("expected seed of 0, got %v", seed)
	}
	for i, by := range seedBytes {
		if by != 0 {
			panicf("expected seedBytes of 0 at i = %v, got '%v'", i, string(by))
		}
	}

	seed, seedBytes = parseSeedString("1")
	if seed != 1 {
		panicf("expected seed of 1, got %v", seed)
	}
	//vv("on 1: seedBytes = '%#v'", seedBytes)
	for i, by := range seedBytes {
		if i == 0 {
			if by != 1 {
				panicf("expected seedBytes of 1 at i = %v, got '%v'", i, by)
			}
			continue
		}
		if by != 0 {
			panicf("expected seedBytes of 0 at i = %v, got '%v'", i, by)
		}
	}

	//vv("start max seed: 1<<64-1")
	seed, seedBytes = parseSeedString("18_446_744_073_709_551_615")
	if seed != 18_446_744_073_709_551_615 {
		panicf("expected seed of 18_446_744_073_709_551_615, got %v", seed)
	}
	//vv("good, got seed = %v as expected", seed)
	//vv("on max: seedBytes = '%#v'", seedBytes)
	for i, by := range seedBytes {
		if by != 255 {
			panicf("expected seedBytes of 255 at i = %v, got '%v'", i, by)
		}
		if i == 7 {
			break
		}
	}
}

func parseSeedString(simseed string) (simulationModeSeed uint64, seedBytes [32]byte) {

	var n, n2 uint64
	for _, ch := range []byte(simseed) {
		switch {
		case ch == '#' || ch == '/':
			break // comments terminate
		case ch < '0' || ch > '9':
			continue
		}
		//vv("ch='%v'; n = %v", ch, n)
		ch -= '0'
		n2 = n*10 + uint64(ch)
		if n2 > n {
			n = n2
		} else {
			break // no overflow
		}
	}
	simulationModeSeed = n
	for i := range 8 {
		// little endian fill
		//vv("from %v, fill at i = %v with %v", n, i, byte(n>>(i*8)))
		seedBytes[i] = byte(n >> (i * 8))
	}
	//println("simulationModeSeed from GO_DSIM_SEED=", simulationModeSeed)
	return
}

/*
// GOEXPERIMENT=synctest GOMAXPROCS=1 GODEBUG=asyncpreemptoff=1 GO_DSIM_SEED=1 go test -v -run 299 -count=1

	func Test299_ResetDsimSeed(t *testing.T) {
		//return

		// tried turning off garbage collection -- we still get non-determinism under
		// GODEBUG=asyncpreemptoff=1 GO_DSIM_SEED=1 GOEXPERIMENT=synctest go test -v -run 299_ResetDsim -trace=trace.out
		// GODEBUG=asyncpreemptoff=1,gctrace=1 GO_DSIM_SEED=1 GOEXPERIMENT=synctest go test -v -run 299_ResetDsim -trace=trace.out
		//
		//debug.SetMemoryLimit(math.MaxInt64)
		//debug.SetGCPercent(-1)
		//vv("turned off garbage collection. now.")

		onlyBubbled(t, func(t *testing.T) {
			// try to provoke races
			vv("begin 299")

			//trace.Start()
			//defer trace.Stop()

			runtime.ResetDsimSeed(1)

			N := uint64(100)

			ma := make(map[int]int)
			for k := range 10 {
				ma[k] = k
			}
			sam := make([][]int, 3)

			for i := range 3 {
				runtime.ResetDsimSeed(uint64(i))
				for k := range ma {
					sam[i] = append(sam[i], k)
				}
				vv("sam[%v] = '%#v'", i, sam[i])
			}

			ctx, task := trace.NewTask(bkg, "i_loop")
			defer task.End()

			for i := uint64(0); i < N; i++ {

				for j := range 10 { // _000 {
					seed := j % 3

					trace.Log(ctx, "i_j_iter", fmt.Sprintf("have i=%v; j=%v", i, j))

					trace.WithRegion(ctx, "one_map_iter", func() {

						runtime.ResetDsimSeed(uint64(seed))

						ii := 0
						for k := range ma {
							if k != sam[seed][ii] {
								// get timestamp since synctest controls clock.
								vv("disagree on seed=%v;  i = %v; ii=%v; k=%v but.. sam[i] = %v (at j=%v); runtime.JeaCounter() = %v", seed, i, ii, k, sam[seed][ii], j, runtime.JeaRandCallCounter())

								panicf("disagree on seed=%v;  i = %v; ii=%v; k=%v but sam[i] = %v (at j=%v)", seed, i, ii, k, sam[seed][ii], j)
							}
							ii++
						}
					})

				}
			}
		})
	}
*/
