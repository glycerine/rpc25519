package tube

import (
	//"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/glycerine/idem"

	porc "github.com/anishathalye/porcupine"
	rpc "github.com/glycerine/rpc25519"
)

// What the 016 test does:
//
// 1. make an n=3 raft cluster.
//
// 2. for i in 0...19 do:
//
//	a) lock the ops log, append a new write starting
//	now and ending in an hour (the "far out" time). unlock.
//
//	b) WRITE from node 0 to the raft cluster
//
//	c) lock the ops log, correct the writer termination time
//	for the write we just did. unlock
//
//	d) for each of the n=3 nodes:
//	    start a background goroutine, in it:
//	        READ the latest value of the register;
//	           when the READ comes back:
//	        lock the ops log.
//	        append my read to the ops log.
//	        take a snapshot of the ops log.
//	        unlock the ops log.
//	        call the porcupine linz checker on the snapshot.
//
// Only practical to do this kind of "online" checking
// for a small number of operations at low concurrency.
// Usually you would write out a longer history and
// check it off-line.
func Test016_tube_parallel_linz(t *testing.T) {

	bubbleOrNot(t, func(t *testing.T) {

		defer func() {
			vv("test 016 wrapping up.")
		}()
		numNodes := 3
		/* old style, but now without any initial config in logs they just hang
		cfg := NewTubeConfigTest(n, t.Name(), globalUseSimnet)
		cfg.testNum = 16

		c := NewCluster(t.Name(), cfg)
		c.Start()
		defer c.Close()
		*/
		forceLeader := 0
		c, leader, leadi, maxterm := setupTestCluster(t, numNodes, forceLeader, 16)
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
			//vv("016test done with read j = %v", j)

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

		vv("016 end of i loop")

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
		vv("016 good: nothing Waiting")
	})
}

func eventViewUpTo(evs []porc.Event, maxev int) (r []porc.Event) {
	for i := range evs {
		if evs[i].Id <= maxev {
			r = append(r, evs[i])
		}
	}
	return
}

func opsViewUpTo(ops []porc.Operation, endtmWrite int64) (r []porc.Operation) {
	for i := range ops {
		if ops[i].Return <= endtmWrite {
			r = append(r, ops[i])
		}
	}
	return
}

type registerOp int

const (
	REGISTER_UNK registerOp = 0
	REGISTER_PUT registerOp = 1
	REGISTER_GET registerOp = 2
)

func (o registerOp) String() string {
	switch o {
	case REGISTER_PUT:
		return "REGISTER_PUT"
	case REGISTER_GET:
		return "REGISTER_GET"
	}
	panic(fmt.Sprintf("unknown registerOp: %v", int(o)))
}

type registerInput struct {
	op    registerOp // false = put, true = get
	value int
}

func (ri registerInput) String() string {
	if ri.op == REGISTER_GET {
		return fmt.Sprintf("registerInput{op: %v}", ri.op)
	}
	return fmt.Sprintf("registerInput{op: %v, value: %v}", ri.op, ri.value)
}

type opsSlice []porc.Operation

func opstring(e porc.Operation) string {
	return fmt.Sprintf(`porc.Operation{
    ClientId: %v,
       Input: %v,
      Output: %v,
      Call: %v,
    Return: %v,
}`, e.ClientId, e.Input, e.Output, niceu(e.Call), niceu(e.Return))
}

func evstring(e porc.Event) string {
	return fmt.Sprintf(`porc.Event{
    ClientId: %v,
        Kind: %v,
       Value: %v,
          Id: %v,
}`, e.ClientId, e.Kind, e.Value, e.Id)
}

type eventSlice []porc.Event

func niceu(u int64) string {
	return nice(time.Unix(0, u))
}

func (s opsSlice) String() (r string) {
	r = "opsSlice{\n"
	for i, e := range s {
		r += fmt.Sprintf("%02d: %v,\n", i, opstring(e))
	}
	r += "}"
	return
}

func (s eventSlice) String() (r string) {
	r = "eventSlice{\n"
	for i, e := range s {
		r += fmt.Sprintf("%02d: %v,\n", i, evstring(e))
	}
	r += "}"
	return
}

// a sequential specification of a register
var registerModel = porc.Model{
	Init: func() interface{} {
		return 0
	},
	// step function: takes a state, input, and output, and returns whether it
	// was a legal operation, along with a new state
	Step: func(state, input, output interface{}) (legal bool, newState interface{}) {
		regInput := input.(registerInput)

		switch regInput.op {
		case REGISTER_PUT:
			legal = true // always ok to execute a put
			newState = regInput.value

		case REGISTER_GET:
			newState = state // state is unchanged by GET

			if output == state {
				legal = true
			}
		}
		return
	},
	DescribeOperation: func(input, output interface{}) string {
		inp := input.(registerInput)
		switch inp.op {
		case REGISTER_GET:
			return fmt.Sprintf("get() -> '%d'", output.(int))
		case REGISTER_PUT:
			return fmt.Sprintf("put('%d')", inp.value)
		}
		panic(fmt.Sprintf("invalid inp.op! '%v'", int(inp.op)))
		return "<invalid>" // unreachable
	},
}

func writeToDiskNonLinz(t *testing.T, ops []porc.Operation) {

	res, info := porc.CheckOperationsVerbose(registerModel, ops, 0)
	if res != porc.Illegal {
		t.Fatalf("expected output %v, got output %v", porc.Illegal, res)
	}
	nm := fmt.Sprintf("red.nonlinz.%v.%03d.html", t.Name(), 0)
	for i := 1; fileExists(nm) && i < 1000; i++ {
		nm = fmt.Sprintf("red.nonlinz.%v.%03d.html", t.Name(), i)
	}
	vv("writing out non-linearizable ops history '%v'", nm)
	fd, err := os.Create(nm)
	panicOn(err)
	defer fd.Close()

	err = porc.Visualize(registerModel, info, fd)
	if err != nil {
		t.Fatalf("ops visualization failed")
	}
	t.Logf("wrote ops visualization to %s", fd.Name())
}

func writeToDiskNonLinzEvents(t *testing.T, evs []porc.Event) {

	res, info := porc.CheckEventsVerbose(registerModel, evs, 0)
	if res != porc.Illegal {
		t.Fatalf("expected output %v, got output %v", porc.Illegal, res)
	}

	nm := fmt.Sprintf("red.ev.nonlinz.%v.%03d.html", t.Name(), 0)
	for i := 1; fileExists(nm) && i < 1000; i++ {
		nm = fmt.Sprintf("red.ev.nonlinz.%v.%03d.html", t.Name(), i)
	}
	vv("writing out event non-linearizable history '%v'", nm)
	fd, err := os.Create(nm)
	panicOn(err)
	defer fd.Close()

	err = porc.Visualize(registerModel, info, fd)
	if err != nil {
		t.Fatalf("event visualization failed")
	}
	t.Logf("wrote event visualization to %s", fd.Name())
}
