package tube

import (
	"context"
	"fmt"

	"math"
	"os"
	"runtime"
	"runtime/debug"
	"runtime/trace"
	"strings"
	//"strconv"
	"sync"

	//"sync/atomic"
	"testing"
	"time"

	"github.com/glycerine/idem"

	porc "github.com/anishathalye/porcupine"
	rpc "github.com/glycerine/rpc25519"
)

var _ = runtime.GOMAXPROCS
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

type sharedOps struct {
	mut sync.Mutex
	ops []porc.Operation
}

// user tries to get read/write work done.
type fuzzUser struct {
	t      *testing.T
	seed   uint64
	name   string
	userid int

	shOps *sharedOps
	rnd   func(nChoices int64) (r int64)

	numNodes int
	clus     *TubeCluster
	sess     *Session

	cli  *TubeNode
	halt *idem.Halter
}

func (s *fuzzUser) linzCheck() {
	// Analysis
	// Expecting some ops
	s.shOps.mut.Lock()
	defer s.shOps.mut.Unlock()

	ops := s.shOps.ops
	if len(ops) == 0 {
		panicf("user %v: expected ops > 0, got 0", s.name)
	}

	linz := porc.CheckOperations(stringCasModel, ops)
	if !linz {
		writeToDiskNonLinzFuzz(s.t, s.name, ops)
		panicf("error: user %v: expected operations to be linearizable! seed='%v'; ops='%v'", s.name, s.seed, opsSlice(ops))
	}

	vv("user %v: len(ops)=%v passed linearizability checker.", s.name, len(ops))
}

func (s *fuzzUser) Start(ctx context.Context, steps int) {
	go func() {
		defer func() {
			s.halt.ReqStop.Close()
			s.halt.Done.Close()
		}()
		for step := range steps {
			vv("%v: fuzzUser.Start on step %v", s.name, step)
			select {
			case <-ctx.Done():
				return
			case <-s.halt.ReqStop.Chan:
				return
			default:
			}

			key := "key10"

			// in first step read, otherwise CAS from previous to next
			// unless swap fails, then re-read.
			var swapped bool
			var err error
			var oldVal Val
			var cur Val

			if step == 0 || !swapped {
				oldVal, err = s.Read(key)
				if err != nil && strings.Contains(err.Error(), "key not found") {
					if step == 0 {
						// allowed on first
					} else {
						panicf("on step %v, key not found!?!", step)
					}
				} else {
					panicOn(err)
				}
			}

			writeMeNewVal := Val([]byte(fmt.Sprintf("%v", s.rnd(100))))
			swapped, cur, err = s.CAS(key, oldVal, writeMeNewVal)
			if err != nil {
				vv("shutting down on err '%v'", err)
				return
			}
			if len(cur) == 0 {
				panic("why is cur value empty after first CAS?")
			}
			oldVal = cur
			if !swapped {
				vv("%v nice: CAS did not swap, on step %v!", s.name, step)
			}
			//nemesis.makeTrouble()
		}
	}()
}

const vtyp101 = "string"

func (s *fuzzUser) CAS(key string, oldVal, newVal Val) (swapped bool, curVal Val, err error) {

	begtmWrite := time.Now()

	out := &casOutput{
		unknown: true,
	}
	op := porc.Operation{
		ClientId: s.userid,
		Input:    &casInput{op: STRING_REGISTER_CAS, oldString: string(oldVal), newString: string(newVal)},
		Call:     begtmWrite.UnixNano(), // invocation timestamp

		// assume error/unknown happens, update below if it did not.
		Output: out,
		//Return:   endtmWrite.UnixNano(), // response timestamp
	}

	vv("about to write from cli sess, writeMe = '%v'", string(newVal))

	// WRITE
	var tktW *Ticket
	tktW, err = s.sess.CAS(bkg, fuzzTestTable, Key(key), oldVal, newVal, 0, vtyp101, 0, leaseAutoDelFalse, 0, 0)

	switch err {
	case ErrShutDown, rpc.ErrShutdown2,
		ErrTimeOut, rpc.ErrTimeout:
		vv("CAS write failed: '%v'", err)
		return
	}

	if err != nil && strings.Contains(err.Error(), rejectedWritePrefix) {
		// fair, fine to reject cas; the error forces us to deal with it,
		// but the occassional CAS reject is fine and working as expected.
		err = nil
		out.unknown = false
	} else {
		panicOn(err)
	}

	op.Return = time.Now().UnixNano() // response timestamp

	// skip adding to porcupine ops if the CAS failed to write.
	if tktW.CASwapped {
		vv("CAS write ok.")
		swapped = true
		if string(tktW.Val) != string(newVal) {
			panicf("why does tktW.Val('%v') != newVal('%v')", string(tktW.Val), string(newVal))
		}
		curVal = Val(append([]byte{}, newVal...))

	} else {
		swapped = false // for emphasis
		curVal = Val(append([]byte{}, tktW.CASRejectedBecauseCurVal...))
		vv("CAS write failed (did not write new value '%v'), we read back current value (%v) instead", string(newVal), string(curVal))
	}
	out.valueCur = string(curVal)
	out.swapped = swapped
	out.unknown = false

	s.shOps.mut.Lock()
	out.id = len(s.shOps.ops)
	s.shOps.ops = append(s.shOps.ops, op)
	vv("%v len ops now %v", s.name, len(s.shOps.ops))
	s.shOps.mut.Unlock()

	return
}

var fuzzTestTable = Key("table101")

func (s *fuzzUser) Read(key string) (val Val, err error) {

	begtmRead := time.Now()

	waitForDur := time.Second

	var tkt *Ticket
	tkt, err = s.sess.Read(bkg, fuzzTestTable, Key(key), waitForDur)
	if err == nil && tkt != nil && tkt.Err != nil {
		err = tkt.Err
	}
	if err != nil {
		vv("read from node/sess='%v', got err = '%v'", s.sess.SessionID, err)
		if err == ErrKeyNotFound || err.Error() == "key not found" {
			return
		}
		if err == rpc.ErrShutdown2 || err.Error() == "error shutdown" {
			return
		}
	}
	switch err {
	case ErrShutDown, rpc.ErrShutdown2,
		ErrTimeOut, rpc.ErrTimeout:
		// TODO: we could add an Unknown outcome Op, but I see no point atm.
		return
	}
	panicOn(err)
	if tkt == nil {
		panic("why is tkt nil?")
	}
	//vv("jnode=%v, back from Read(key='%v') -> tkt.Val:'%v' (tkt.Err='%v')", jnode, key, string(tkt.Val), tkt.Err)

	val = Val(append([]byte{}, tkt.Val...))

	endtmRead := time.Now()

	//v2 := tkt.Val
	//i2, err := strconv.Atoi(string(v2))
	//panicOn(err)

	out := &casOutput{
		valueCur: string(val),
	}
	op := porc.Operation{
		ClientId: s.userid,
		Input:    &casInput{op: STRING_REGISTER_GET},
		Call:     begtmRead.UnixNano(), // invocation timestamp
		//Output:   i2,
		Output: out,
		Return: endtmRead.UnixNano(), // response timestamp
	}

	s.shOps.mut.Lock()
	out.id = len(s.shOps.ops)
	s.shOps.ops = append(s.shOps.ops, op)
	s.shOps.mut.Unlock()

	return
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

		// remember this problem: now that we try to make NEW connections
		// after detecting node failures, those newly made
		// connections do not have the deaf/drop applied!
		// so ISOLATE host instead!
		// maybe we need a faul mode were all connections
		// from (or to) have the deaf/drop probs applied.
		// (Or not if we want to model faulty middle boxes? rare, skip for now)
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

func Test101_userFuzz(t *testing.T) {

	runtime.GOMAXPROCS(1)

	defer func() {
		vv("test 101 wrapping up.")
	}()

	maxScenario := 1
	for scenario := 0; scenario < maxScenario; scenario++ {

		seedString := fmt.Sprintf("%v", scenario)
		seed, seedBytes := parseSeedString(seedString)
		if int(seed) != scenario {
			panicf("got %v, wanted same scenario number back %v", int(seed), scenario)
		}
		rng := newPRNG(seedBytes)
		rnd := rng.pseudoRandNonNegInt64Range

		//var ops []porc.Operation

		onlyBubbled(t, func(t *testing.T) {

			steps := 20
			numNodes := 3
			numUsers := 3

			forceLeader := 0
			c, leaderName, leadi, _ := setupTestCluster(t, numNodes, forceLeader, 101)

			leaderURL := c.Nodes[leadi].URL
			defer c.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			var users []*fuzzUser
			shOps := &sharedOps{}
			for userNum := 0; userNum < numUsers; userNum++ {
				user := &fuzzUser{
					shOps:    shOps,
					t:        t,
					seed:     seed,
					name:     fmt.Sprintf("user%v", userNum),
					userid:   userNum,
					numNodes: numNodes,
					rnd:      rnd,
					clus:     c,
					halt:     idem.NewHalterNamed("fuzzUser"),
				}
				users = append(users, user)

				cliName := "client101_" + user.name
				cliCfg := *c.Cfg
				cliCfg.MyName = cliName
				cliCfg.PeerServiceName = TUBE_CLIENT
				cli := NewTubeNode(cliName, &cliCfg)
				err := cli.InitAndStart()
				panicOn(err)
				defer cli.Close()

				// request new session
				// seems like we want RPC semantics for this
				// and maybe for other calls?

				sess, err := cli.CreateNewSession(bkg, leaderName, leaderURL)
				panicOn(err)
				if sess.ctx == nil {
					panic(fmt.Sprintf("sess.ctx should be not nil"))
				}
				//vv("got sess = '%v'", sess) // not seen.
				user.sess = sess
				user.cli = cli

				// no nemesis initially.
				//nemesis := &fuzzNemesis{
				//	rng:     rng,
				//	rnd:     rnd,
				//	clus:    c,
				//	damaged: make(map[int]int),
				//}
				//_ = nemesis

				user.Start(ctx, steps)
			}

			for _, user := range users {
				<-user.halt.Done.Chan
			}
			// the history is shared, wait until all are done or else
			// we can read back a value before we get a chance to
			// record the write into the op history, and the linz
			// checker will false alarm on that.
			users[0].linzCheck()
		})

	}
}

func writeToDiskNonLinzFuzz(t *testing.T, user string, ops []porc.Operation) {

	res, info := porc.CheckOperationsVerbose(stringCasModel, ops, 0)
	if res != porc.Illegal {
		t.Fatalf("expected output %v, got output %v", porc.Illegal, res)
	}
	nm := fmt.Sprintf("red.nonlinz.%v.%03d.user_%v.html", t.Name(), 0, user)
	for i := 1; fileExists(nm) && i < 1000; i++ {
		nm = fmt.Sprintf("red.nonlinz.%v.%03d.user_%v.html", t.Name(), i, user)
	}
	vv("writing out non-linearizable ops history '%v'", nm)
	fd, err := os.Create(nm)
	panicOn(err)
	defer fd.Close()

	err = porc.Visualize(stringCasModel, info, fd)
	if err != nil {
		t.Fatalf("ops visualization failed")
	}
	t.Logf("wrote ops visualization to %s", fd.Name())
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

/* do not uncomment Test099, since it uses a custom
derivative of Go called Pont that implements the runtime.ResetDsimSeed(),
and this is not available in regular Go. Extend or copy Test101 instead.
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
*/

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

// string register with CAS (compare and swap)

type stringRegisterOp int

const (
	STRING_REGISTER_UNK stringRegisterOp = 0
	STRING_REGISTER_PUT stringRegisterOp = 1
	STRING_REGISTER_GET stringRegisterOp = 2
	STRING_REGISTER_CAS stringRegisterOp = 3
)

func (o stringRegisterOp) String() string {
	switch o {
	case STRING_REGISTER_PUT:
		return "STRING_REGISTER_PUT"
	case STRING_REGISTER_GET:
		return "STRING_REGISTER_GET"
	case STRING_REGISTER_CAS:
		return "STRING_REGISTER_CAS"
	}
	panic(fmt.Sprintf("unknown stringRegisterOp: %v", int(o)))
}

type casInput struct {
	op        stringRegisterOp
	oldString string
	newString string
}

type casOutput struct {
	id       int
	swapped  bool   // used for CAS
	notFound bool   // used for read
	unknown  bool   // used when operation times out
	valueCur string // for read/when cas rejects
}

func (o *casOutput) String() string {
	return fmt.Sprintf(`casOutput{
            id: %v
       swapped: %v
      notFound: %v
       unknown: %v
      valueCur: %v
}`, o.id, o.swapped, o.notFound,
		o.unknown,
		o.valueCur)
}

func (ri *casInput) String() string {
	if ri.op == STRING_REGISTER_GET {
		return fmt.Sprintf("casInput{op: %v}", ri.op)
	}
	return fmt.Sprintf("casInput{op: %v, oldString: '%v', newString: '%v'}", ri.op, ri.oldString, ri.newString)
}

// a sequential specification of a register, that holds a string
// and can CAS.
var stringCasModel = porc.Model{
	Init: func() interface{} {
		return "<empty>"
	},
	// step function: takes a state, input, and output, and returns whether it
	// was a legal operation, along with a new state. Must be a pure
	// function. Do not modify state, input, or output.
	Step: func(state, input, output interface{}) (legal bool, newState interface{}) {
		st := state.(string)
		inp := input.(*casInput)
		out := output.(*casOutput)

		switch inp.op {
		case STRING_REGISTER_GET:
			newState = st // state is unchanged by GET

			legal = (out.notFound && st == "<empty>") ||
				(!out.notFound && st == out.valueCur) ||
				out.unknown
			return

		case STRING_REGISTER_PUT:
			legal = true // always ok to execute a put
			newState = inp.newString
			return

		case STRING_REGISTER_CAS:

			if inp.oldString == "" {
				// treat empty string as absent/deleted/anything goes.
				// So this becomes just a PUT:
				legal = true
				newState = inp.newString
				return
			}

			// the default is that the state stays the same.
			newState = st

			if out.unknown {
				legal = true
			} else if inp.oldString == st && out.swapped {
				legal = true
			} else if inp.oldString != st && !out.swapped {
				legal = true
			} else {
				//vv("warning: legal is false in CAS because out.swapped = '%v', inp.oldString = '%v', inp.newString = '%v'; old state = '%v', newState = '%v'; out.valueCur = '%v'", out.swapped, inp.oldString, inp.newString, st, newState, out.valueCur)
			}

			if legal {
				if inp.oldString == st {
					newState = inp.newString
				}
			}
			return
		}
		return
	},
	DescribeOperation: func(input, output interface{}) string {
		inp := input.(*casInput)
		out := output.(*casOutput)

		switch inp.op {
		case STRING_REGISTER_GET:
			var r string
			if out.notFound {
				r = "<not found>"
			} else {
				r = fmt.Sprintf("'%v'", out.valueCur)
			}
			return fmt.Sprintf("get() -> %v", r)
		case STRING_REGISTER_PUT:
			return fmt.Sprintf("put('%v')", inp.newString)

		case STRING_REGISTER_CAS:

			if out.unknown {
				return fmt.Sprintf("CAS(unkown/timed-out: if '%v' -> '%v')", inp.oldString, inp.newString)
			}
			if out.swapped {
				return fmt.Sprintf("CAS(ok: '%v' ->'%v')", inp.oldString, inp.newString)
			}
			return fmt.Sprintf("CAS(rejected:old '%v' != cur '%v')", inp.oldString, out.valueCur)
		}
		panic(fmt.Sprintf("invalid inp.op! '%v'", int(inp.op)))
		return "<invalid>" // unreachable
	},
}
