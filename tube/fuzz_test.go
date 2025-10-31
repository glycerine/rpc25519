package tube

import (
	//"context"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/glycerine/idem"

	porc "github.com/anishathalye/porcupine"
	rpc "github.com/glycerine/rpc25519"
)

type fuzzFault int

const (
	fuzz_PAUSE fuzzFault = iota
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

// client tries to get work done.
type fuzzClient struct{}

// nemesis injects faults, makes trouble for client.
type fuzzNemesis struct{}

func Test099_fuzz_testing_linz(t *testing.T) {

	runtime.GOMAXPROCS(1)

	defer func() {
		vv("test 099 wrapping up.")
	}()

	maxScenario := 1
	for scenario := 0; scenario < maxScenario; scenario++ {

		seedString := fmt.Sprintf("%v", scenario)
		// if we were starting a new live Go process from
		// scratch we would want to do this:
		os.Setenv("GO_DSIM_SEED", seedString)

		seed, seedBytes := parseSeedString(seedString)
		if int(seed) != scenario {
			panicf("got %v, wanted same scenario number back %v", int(seed), scenario)
		}
		// Since we are not staring a new process,
		// can we still control the
		// runtime's initialization with the seed; i.e.
		// we are already running here!
		runtime.ResetDsimSeed(seed)

		rng := newPRNG(seedBytes)
		_ = rng

		return
	}
	onlyBubbled(t, func() {

		numNodes := 3

		forceLeader := 0
		c, leader, leadi, maxterm := setupTestCluster(t, numNodes, forceLeader, 99)
		_, _, _ = leader, leadi, maxterm
		defer c.Close()

		nodes := c.Nodes

		var v []byte
		N := 20
		fin := make(map[string]bool)
		var finmut sync.Mutex

		var ops []porc.Operation

		var opsMut sync.Mutex

		done := make(chan bool)
		wgcount := int64(numNodes * N)
		tHalt := idem.NewHalter()
		c.Halt.AddChild(tHalt)

		// establish a point for all writes to initially end,
		// that is a ways out, so we can add to ops before
		// it actually finishes. Trying to resolve the race
		// when readers get the new value before we've added
		// it to the ops log.
		farout := time.Now().Add(time.Hour)

		workerFunc := func(i, jnode int, endtmWrite time.Time) {
			defer func() {
				res := atomic.AddInt64(&wgcount, -1)
				if res == 0 {
					close(done)
				}
			}()

			//vv("i=%v, jnode=%v, about to Read", i, jnode)

			begtmRead := time.Now()
			tkt, err := nodes[jnode].Read(bkg, "", "a", 0, nil) // waiting here on deadlock/wg.Wait... so lost read? i=19, jnode=2, about to Read. last one.
			//vv("i=%v, jnode=%v, back from Read", i, jnode)
			finmut.Lock()
			delete(fin, fmt.Sprintf("i=%v:%v node", i, jnode))
			finmut.Unlock()

			switch err {
			case ErrShutDown, rpc.ErrShutdown2,
				ErrTimeOut, rpc.ErrTimeout:
				return
			}
			panicOn(err)
			endtmRead := time.Now()
			_ = endtmRead
			v2 := tkt.Val
			i2, err := strconv.Atoi(string(v2))
			panicOn(err)

			//endu := endtmRead.UnixNano()
			opsMut.Lock()
			ops = append(ops, porc.Operation{
				ClientId: jnode,
				Input:    registerInput{op: REGISTER_GET},
				Call:     begtmRead.UnixNano(), // invocation timestamp
				Output:   i2,
				Return:   endtmRead.UnixNano(), // response timestamp
			})
			oview := append([]porc.Operation{}, ops...)
			//oview := opsViewUpTo(bo, endtmWrite.UnixNano())
			//oview := bo // should be okay with farout.
			opsMut.Unlock()

			//if i2 != i {
			// 82 of these due to racing reads/writes, good!
			// so let's check for linearizability instead.
			// t.Fatalf("write a:'%v' to node 0, read back from node %v a different value: '%v'", i, jnode, i2)
			//vv("on jnode=%v, read i2=%v, after write of i=%v so checking linearizability...", jnode, i2, i)

			linz := porc.CheckOperations(registerModel, oview)
			if !linz {
				writeToDiskNonLinz(t, oview)
				t.Fatalf("error: expected operations to be linearizable! ops='%v'", opsSlice(ops))
			}

			vv("jnode=%v, i=%v passed linearizability checker.", jnode, i)
			//}
			//vv("099test done with read j = %v", j)

		} // end defn workerFunc

		//wg.Add(n * N)
		for i := range N {
			for j := range numNodes {
				fin[fmt.Sprintf("i=%v:%v node", i, j)] = true
			}
		}
		for i := range N {

			begtmWrite := time.Now()

			opsMut.Lock()
			pos := len(ops)
			ops = append(ops, porc.Operation{
				ClientId: 0,
				Input:    registerInput{op: REGISTER_PUT, value: i},
				Call:     begtmWrite.UnixNano(), // invocation timestamp
				Output:   i,
				// farout should be far enough out that
				// all reads should overlap it.
				Return: farout.UnixNano(),

				// later update to this when we have it:
				//Return:   endtmWrite.UnixNano(), // response timestamp
			})
			opsMut.Unlock()

			v = []byte(fmt.Sprintf("%v", i))
			vv("about to write at i=%v: '%v'", i, string(v))

			// WRITE
			tktW, err := nodes[0].Write(bkg, "", "a", v, 0, nil)

			switch err {
			case ErrShutDown, rpc.ErrShutdown2,
				ErrTimeOut, rpc.ErrTimeout:
				return
			}
			panicOn(err)
			endtmWrite := time.Now()
			_ = tktW

			opsMut.Lock()
			ops[pos].Return = endtmWrite.UnixNano() // response timestamp
			opsMut.Unlock()

			// Read all nodes in parallel
			// but give each their own copy of the
			// baseline ops at this point, they can append to their own.
			//bo := make([][]porc.Operation, n)
			//for j := range n {
			//	bo[j] = append([]porc.Operation{}, baseops...)
			//}

			for jnode := range numNodes {
				go workerFunc(i, jnode, endtmWrite)
			}
		}

		vv("099 end of i loop")

		// there should be no waiting tickets left now.
		// err.. with the goroutines going above, there can be reads still
		// in progress...
		//c.NoisyNothing(false, true)

		//time.Sleep(5 * time.Second)
		//time.Sleep(2 * time.Second)
		select {
		case <-done:
		case <-time.After(10 * time.Second):

			finmut.Lock()
			notfin := len(fin)
			vv("not finished: %v '%#v'", notfin, fin)
			finmut.Unlock()
			// porc_test.go:205 2025-06-07T01:42:15.333088000-05:00 not finished: 1 'map[string]bool{"i=11:2 node":true}'
			if notfin > 0 {
				vv("gonna panic bc not finished: %v '%#v'", notfin, fin)

				// there should be no waiting tickets left now.
				for j := range numNodes {
					pect := nodes[j].Inspect()
					nWaitL := len(pect.WaitingAtLeader)
					if got, want := nWaitL, 0; got != want {
						panic(fmt.Sprintf("should be nothing waitingL; we see '%v'", pect.WaitingAtLeader))
					}
					nWaitF := len(pect.WaitingAtFollow)
					if got, want := nWaitF, 0; got != want {
						panic(fmt.Sprintf("should be nothing waitingF; we see '%v'", pect.WaitingAtFollow))
					}
					for k, tkt := range pect.Tkthist {
						vv("[node %v] Tkthist[%v]='%v'", j, k, tkt)
					}
				}
				// dump the tube nodes with their waiting queues?
				// well, but from the above, we know they are empty!

				fmt.Printf("where is lost read? allstacks:\n %v \n", allstacks())
				vv("simnet = '%v'", c.SimnetSnapshot().LongString())

				time.Sleep(time.Second)
				panic("where is lost read?")
				// lost i=1, jnode=2
				// lost i=19, jnode=0 and jnode=2
			}
		}

		vv("099 good: nothing Waiting")
	})

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
