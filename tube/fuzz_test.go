package tube

import (
	//"context"
	"fmt"
	//"os"
	"runtime"
	"strconv"
	"sync"
	//"sync/atomic"
	"testing"
	"time"

	//"github.com/glycerine/idem"

	porc "github.com/anishathalye/porcupine"
	rpc "github.com/glycerine/rpc25519"
)

type fuzzFault int

const (
	fuzz_NOOP fuzzFault = iota
	fuzz_PAUSE
	fuzz_CRASH
	fuzz_PARTITON
	fuzz_CLOCK_SKEW
	fuzz_MEMBER_ADD
	fuzz_MEMBER_REMOVE
	fuzz_SWIZZLE_CLOG
	fuzz_ONE_WAY_FAULT
	fuzz_ONE_WAY_FAULT_PROBABALISTIC
	fuzz_ADD_CLIENT
	fuzz_TERMINATE_CLIENT
	fuzz_MISORDERED_MESSAGE
	fuzz_DUPLICATED_MESSAGE

	fuzz_LAST
)

// func (s *TubeCluster) DeafDrop(deaf, drop map[int]float64)
// func (s *TubeCluster) IsolateNode(i int)
// func (s *TubeCluster) AllHealthy(powerOnAnyOff, deliverDroppedSends bool)
// func (s *TubeCluster) AllHealthyAndPowerOn(deliverDroppedSends bool)
// func (s *TubeNode) DropSends(probDrop float64)
// func (s *TubeNode) DeafToReads(probDeaf float64)

// mutator tries to get read/write work done.
type fuzzMutator struct {
	opsMut sync.Mutex
	ops    []porc.Operation
	rnd    func(nChoices int64) (r int64)

	clus *TubeCluster
}

func (s *fuzzMutator) Write(key string, writeMe int) {

	begtmWrite := time.Now()

	op := porc.Operation{
		ClientId: 0,
		Input:    registerInput{op: REGISTER_PUT, value: writeMe},
		Call:     begtmWrite.UnixNano(), // invocation timestamp
		Output:   writeMe,
		//Return:   endtmWrite.UnixNano(), // response timestamp
	}

	v := []byte(fmt.Sprintf("%v", writeMe))
	vv("about to write at writeMe=%v: '%v'", writeMe, string(v))

	// WRITE
	tktW, err := s.clus.Nodes[0].Write(bkg, "", Key(key), v, 0, nil)

	switch err {
	case ErrShutDown, rpc.ErrShutdown2,
		ErrTimeOut, rpc.ErrTimeout:
		return
	}
	panicOn(err)
	endtmWrite := time.Now()
	_ = tktW

	op.Return = endtmWrite.UnixNano() // response timestamp

	s.opsMut.Lock()
	s.ops = append(s.ops, op)
	s.opsMut.Unlock()

}

func (s *fuzzMutator) Read(key string, jnode int) {

	begtmRead := time.Now()

	tkt, err := s.clus.Nodes[jnode].Read(bkg, "", Key(key), 0, nil)
	//vv("i=%v, jnode=%v, back from Read", i, jnode)

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

// nemesis injects faults, makes trouble for mutator.
type fuzzNemesis struct {
	rnd  func(nChoices int64) (r int64)
	clus *TubeCluster
}

func Test099_fuzz_testing_linz(t *testing.T) {

	runtime.GOMAXPROCS(1)

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

		onlyBubbled(t, func() {

			steps := 20
			_ = steps
			numNodes := 3

			forceLeader := 0
			c, leader, leadi, maxterm := setupTestCluster(t, numNodes, forceLeader, 99)
			_, _, _ = leader, leadi, maxterm
			defer c.Close()

			mutator := &fuzzMutator{
				rnd:  rnd,
				clus: c,
			}
			nemesis := &fuzzNemesis{
				rnd:  rnd,
				clus: c,
			}
			_ = mutator
			_ = nemesis
		})

		linz := porc.CheckOperations(registerModel, ops)
		if !linz {
			writeToDiskNonLinz(t, ops)
			t.Fatalf("error: expected operations to be linearizable! ops='%v'", opsSlice(ops))
		}

		//vv("jnode=%v, i=%v passed linearizability checker.", jnode, i)
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
