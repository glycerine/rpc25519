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

	"sync/atomic"
	"testing"
	"time"

	"github.com/glycerine/idem"

	porc "github.com/glycerine/porcupine"
	//porc "github.com/anishathalye/porcupine"
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

type sharedEvents struct {
	mut sync.Mutex
	evs []porc.Event
}

// user tries to get read/write work done.
type fuzzUser struct {
	atomicLastEventID *atomic.Int64
	t                 *testing.T
	seed              uint64
	name              string
	userid            int
	edition           int // avoid simnet freak out on reuse of server name

	shEvents *sharedEvents
	rnd      func(nChoices int64) (r int64)

	numNodes int
	clus     *TubeCluster
	sess     *Session

	cli  *TubeNode
	halt *idem.Halter

	nemesis *fuzzNemesis
}

// eventId is what we match on, both calls and returns have the eventId.
// each new call attempt generates a new eventId, so eventId uniquely
// identifies the call attempt, and it should be named callAttemptId
// instead.
//
// A duplicated (or broadcast) call message can result in the delivery
// of the callAttemptId to multiple receivers and thus
// create multiple (return) events. Thus the callAttemptId is
// only unique during the call phase, and in the return phase
// it is no longer a unique identifier, but rather may correspond
// to multiple return events. However this function is one-way
// in the sense that each return event can be associated
// with only a single call attempt.
//
// But the callAttemptId identifies uniquely in the call map
// the set of destination return events that did observe it.
// So an attempted call that was never matched to a return
// event? is it present in the cliInfo.call map? there is
// nothing to match it with so... just accumulate
// stats for it... but do not enter into the call (or ret)
// maps.

type cliInfo struct {
	clientId int
	// out-bound edges: from these ini (Call) event
	// to the matching fin (Return) events.
	call map[int]map[int]int

	// in-bound edges: this event has incoming from
	// these fin (Return) events to the set of matching ini (Call) events.
	ret map[int]map[int]int
}

// from and to are eventId
func (s *cliInfo) addCall(fromIdx, toIdx, eventID int) {
	calls, ok := s.call[fromIdx]
	if !ok {
		calls = make(map[int]int)
		s.call[fromIdx] = calls
	}
	calls[toIdx] = eventID
}
func (s *cliInfo) addReturn(fromIdx, toIdx, eventID int) {
	rets, ok := s.ret[toIdx]
	if !ok {
		rets = make(map[int]int)
		s.ret[toIdx] = rets
	}
	rets[fromIdx] = eventID
}
func newCliInfo(clientID int) *cliInfo {
	return &cliInfo{
		clientId: clientID,
		call:     make(map[int]map[int]int), // fromIdx -> toIdx   -> eventID
		ret:      make(map[int]map[int]int), // toIdx   -> fromIdx -> eventID
	}
}

type eventStats struct {

	// callAttemptN (is partitioned into) =
	// oneToOneCallsN (only 1 receive) +
	// danglingCallsN (no recives; dropped message) +
	// dupReturnN (2 or more receives for same eventID; duplicated message) +
	// misDeliveredCallsN (received by wrong counter-party).
	//
	// but we cannot see misDelivered calls only looking
	// from the client side (so no actual counte for them).
	//
	// the dupReturnN we know about only if the client
	// receives a return event for the same eventID twice or more.
	// We hope for none, but check for them as a sanity check
	// (is the data really messed up?) anyway -- since the
	// receiver can re-try sending a reply it thought was lost.
	//
	// there could also be erroneous returns without a corresponding
	// call; we sanity check for those.
	countAllEvents int

	callAttemptN     int // call attempts
	oneToOneCallsN   int
	oneToOneCallsPct float64

	danglingCallsN   int
	danglingCallsPct float64

	dupReturnN   int
	dupReturnPct float64

	returnWithoutCallN   int
	returnWithoutCallPct float64

	numPut     int
	numPut11   int
	numPutFail int

	numGet     int
	numGet11   int
	numGetFail int

	numCAS       int
	numCAS11     int
	numCASFail   int
	numCASwapped int
}

type perClientEventStats struct {
	eventStats
	clientID int
	cliInfo  *cliInfo
}

func newPerClientEventStats(clientID int) *perClientEventStats {
	return &perClientEventStats{
		clientID: clientID,
		cliInfo:  newCliInfo(clientID),
	}
}

type totalEventStats struct {
	eventStats
	perCli map[int]*perClientEventStats
}

func (s *totalEventStats) getPerClientStats(clientID int) *perClientEventStats {
	c, ok := s.perCli[clientID]
	if ok {
		return c
	}
	c = newPerClientEventStats(clientID)
	s.perCli[clientID] = c
	return c
}

func newTotalEventStats(countAllEvents int) *totalEventStats {
	s := &totalEventStats{
		perCli: make(map[int]*perClientEventStats),
	}
	s.countAllEvents = countAllEvents
	return s
}

func (s *perClientEventStats) String() (r string) {
	return fmt.Sprintf(`
  =======   client %v event stats   ========
%v`, s.clientID, s.eventStats.String())
}

func (s *totalEventStats) String() (r string) {
	return fmt.Sprintf(`
  =======   total event stats   ========
%v`, s.eventStats.String())
}

func (s *eventStats) String() (r string) {
	r = fmt.Sprintf(`
          callAttemptN: %v
        countAllEvents: %v

     [ok call] oneToOneCallsPct: %0.3f (%v)

 [failed call] danglingCallsPct: %0.3f (%v)

        dupReturnPct: %0.3f (%v)
returnWithoutCallPct: %0.3f (%v)

  -- categorizing attempted calls --
	numPut: %v  [dangling: %v, one-to-one: %v]
	numGet: %v  [dangling: %v, one-to-one: %v]
	numCAS: %v  [dangling: %v, one-to-one: %v, swapped: %v (%0.2f %%)]
`,
		s.callAttemptN,
		s.countAllEvents,

		s.oneToOneCallsPct,
		s.oneToOneCallsN,

		s.danglingCallsPct,
		s.danglingCallsN,

		s.dupReturnPct,
		s.dupReturnN,

		s.returnWithoutCallPct,
		s.returnWithoutCallN,

		s.numPut, s.numPutFail, s.numPut11,
		s.numGet, s.numGetFail, s.numGet11,
		s.numCAS, s.numCASFail, s.numCAS11, s.numCASwapped,
		100*float64(s.numCASwapped)/float64(s.numCAS11),
	)
	return
}

func basicEventStats(evs []porc.Event) (tot *totalEventStats) {
	tot = newTotalEventStats(len(evs))

	// eventID -> index of CallEvent in evs.
	// This we assert must be 1:1 since if
	// the client re-tries a call, it must (and
	// does) assign it a new EventID when
	// is appends the new CallEvent to its
	// unique index in evs.
	event2caller := make(map[int]int)

	// eventID -> ReturnEvent list of indexes into evs
	event2return := make(map[int]map[int]struct{})

	// track all of the returns to check for any missing a call.
	unmatchedReturns := make(map[int]int) // index in evs -> eventID

	for i, e := range evs {
		eventID := e.Id
		if e.Kind == porc.CallEvent {
			// sanity assert there is no prior event2caller entry.
			prior, already := event2caller[eventID]
			if already {
				panicf("sanity check failed: for eventID(%v) have prior(%v)", eventID, prior)
			}
			event2caller[eventID] = i
			continue
		}
		// e == evs[i] is a porc.ReturnEvent
		unmatchedReturns[i] = eventID
		returnList, ok := event2return[eventID]
		if !ok {
			returnList = make(map[int]struct{})
			event2return[eventID] = returnList
		}
		// sanity check
		_, already := returnList[i]
		if already {
			panicf("internal assumptions not holding?! why already in returnList? do we need a counter instead of set here?")
		}
		returnList[i] = struct{}{}
	}
	// now do matching on eventID
	for i, e := range evs {
		eventID := e.Id
		clientID := e.ClientId
		per := tot.getPerClientStats(clientID)
		per.countAllEvents++
		info := per.cliInfo

		if e.Kind == porc.CallEvent {
			per.callAttemptN++
			tot.callAttemptN++

			// count call type
			inp, isInput := e.Value.(*casInput)
			if !isInput {
				panicf("why not input? e.Value='%#v'", e.Value)
			}
			var failCount, perFailCount *int
			var okCount, perOkCount *int
			var isCAS bool
			switch inp.op {
			case STRING_REGISTER_UNK:
				panic("should be no STRING_REGISTER_UNK")
			case STRING_REGISTER_PUT:
				tot.numPut++
				per.numPut++
				failCount = &(tot.numPutFail)
				okCount = &(tot.numPut11)
				perFailCount = &(per.numPutFail)
				perOkCount = &(per.numPut11)
			case STRING_REGISTER_GET:
				tot.numGet++
				per.numGet++
				failCount = &(tot.numGetFail)
				okCount = &(tot.numGet11)
				perFailCount = &(per.numGetFail)
				perOkCount = &(per.numGet11)
			case STRING_REGISTER_CAS:
				tot.numCAS++
				per.numCAS++
				failCount = &(tot.numCASFail)
				okCount = &(tot.numCAS11)
				perFailCount = &(per.numCASFail)
				perOkCount = &(per.numCAS11)
				isCAS = true
			}

			returnList, ok := event2return[eventID]
			if !ok {
				// no returns/replies for this event, it is dangling.
				per.danglingCallsN++
				tot.danglingCallsN++
				(*failCount)++
				(*perFailCount)++
				continue
			}
			nReturn := len(returnList)
			switch nReturn {
			case 0:
				// nil map, should have skipped above on !ok
				panic("why have a returnList at all?")
			case 1:
				// single call, single return/response.
				per.oneToOneCallsN++
				tot.oneToOneCallsN++
				(*okCount)++
				(*perOkCount)++
				if isCAS {
					for j := range returnList {
						out, ok := evs[j].Value.(*casOutput)
						if !ok {
							panicf("j=%v should have *casOutput, not %#v", j, evs[j].Value)
						}
						if out.swapped {
							tot.numCASwapped++
							per.numCASwapped++
						}
					}
				}
			default:
				vv("eventID %v has multiple returns:", eventID)
				for j := range returnList {
					vv("dup returns: j=%v  for eventID %v", j, eventID)
				}
				per.dupReturnN++
				tot.dupReturnN++
			}
			for j := range returnList {
				info.addCall(i, j, eventID)
				info.addReturn(i, j, eventID)
				delete(unmatchedReturns, j)
			}
		}
	}

	// compute stats per each client, and overall in tot.
	for _, per := range tot.perCli { // map[int]*perClientEventStats
		base := float64(per.callAttemptN)
		per.oneToOneCallsPct = float64(per.oneToOneCallsN) / base
		per.danglingCallsPct = float64(per.danglingCallsN) / base
		per.dupReturnPct = float64(per.dupReturnN) / base
	}
	n := float64(tot.callAttemptN)
	tot.oneToOneCallsPct = float64(tot.oneToOneCallsN) / n
	tot.danglingCallsPct = float64(tot.danglingCallsN) / n
	tot.dupReturnPct = float64(tot.dupReturnN) / n

	tot.returnWithoutCallN = len(unmatchedReturns)
	tot.returnWithoutCallPct = float64(tot.returnWithoutCallN) / n
	for _, eventID := range unmatchedReturns {
		i := event2caller[eventID] // callEvent index for this eventID
		clientID := evs[i].ClientId
		per := tot.getPerClientStats(clientID)
		per.returnWithoutCallN++
	}
	for _, per := range tot.perCli {
		per.returnWithoutCallPct = float64(per.returnWithoutCallN) / float64(per.callAttemptN)
	}

	return
}

func (s *fuzzUser) linzCheck() {
	// Analysis
	// Expecting some events
	s.shEvents.mut.Lock()
	defer s.shEvents.mut.Unlock()

	evs := s.shEvents.evs
	if len(evs) == 0 {
		panicf("user %v: expected evs > 0, got 0", s.name)
	}

	totalStats := basicEventStats(evs)
	alwaysPrintf("linzCheck basic event stats:\n %v", totalStats)

	for clientID, per := range totalStats.perCli {
		alwaysPrintf("clientID %v stats: \n %v", clientID, per)
	}

	// filter out unmatched gets?

	//vv("linzCheck: about to porc.CheckEvents on %v evs", len(evs))
	linz := porc.CheckEvents(stringCasModel, evs)
	if !linz {
		alwaysPrintf("error: user %v: expected operations to be linearizable! seed='%v'; evs='%v'", s.name, s.seed, eventSlice(evs))
		writeToDiskNonLinzFuzzEvents(s.t, s.name, evs)
		panicf("error: user %v: expected operations to be linearizable! seed='%v'", s.name, s.seed)
	}

	vv("user %v: len(evs)=%v passed linearizability checker.", s.name, len(evs))

	// nothing much to see really.
	//writeToDiskOkEvents(s.t, s.name, evs)
}

func (s *fuzzUser) Start(startCtx context.Context, steps int, leaderName, leaderURL string) {
	go func() {
		defer func() {
			s.halt.ReqStop.Close()
			s.halt.Done.Close()
		}()
		var prevCanc context.CancelFunc = func() {}
		defer func() {
			prevCanc()
		}()
		for step := range steps {
			prevCanc()
			stepCtx, canc := context.WithTimeout(startCtx, time.Second*10)
			prevCanc = canc
			if false && step%10 == 0 {
				vv("%v: fuzzUser.Start on step %v", s.name, step)
			}
			select {
			case <-stepCtx.Done():
				return
			case <-startCtx.Done():
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
				if err != nil {
					errs := err.Error()
					switch {
					case strings.Contains(errs, "key not found"):
						if step == 0 {
							// allowed on first
							err = nil
						} else {
							panicf("%v key not found on step %v!?!", s.name, step)
						}
					case strings.Contains(errs, "error shutdown"):
						// nemesis probably shut down the node
						vv("%v: try again on shutdown error", s.name)

						// do we need to reconnect to try again?
						continue
					case strings.Contains(errs, "context deadline exceeded"):
						// message loss due to nemesis likely
						vv("%v: try again on timeout", s.name)

						// do we need to reconnect to try again?
						continue
					}
				}
				panicOn(err)
			}

			writeMeNewVal := Val([]byte(fmt.Sprintf("%v", s.rnd(100))))
			swapped, cur, err = s.CAS(stepCtx, key, oldVal, writeMeNewVal)

			if err != nil {
				errs := err.Error()
				if err == ErrNeedNewSession ||
					strings.Contains(errs, "need new session") {

					snap1 := s.clus.SimnetSnapshot(true)
					if snap1.HealthSummary() == "SimnetSnapshot{all HEALTHY}" {
						panic("snap0 shows healthy, why did we need a new session then?")
					}
					vv("prompted for new session, snap1 = '%v'", snap1) // .LongString()) // not seen.

					sess := s.newSession(startCtx, leaderName, leaderURL)
					if sess == nil {
						alwaysPrintf("%v ugh, cannot get newSession with this client", s.name)
						time.Sleep(time.Second)
					}
					vv("%v good, got new session", s.name)
					continue
				}

				switch {
				case strings.Contains(errs, "error shutdown"):
					// nemesis probably shut down the node
					vv("%v: try again on shutdown error", s.name)

					// do we need to reconnect to try again?
					continue
				case strings.Contains(errs, "context deadline exceeded"):
					// message loss due to nemesis likely
					vv("%v: try again on timeout", s.name)

					// do we need to reconnect to try again?
					continue
				}
				vv("shutting down on err (at step %v) '%v'; stepCtx.Err()='%v'; startCtx.Err()='%v'", step, err, stepCtx.Err(), startCtx.Err()) // stepCtx not cancelled. hmm... startCtx also not canceled!
				return
			}

			if len(cur) == 0 {
				panicf("why is cur value empty after first CAS? oldVal='%v', writeMeNewVal='%v', swapped='%v', err='%v'", string(oldVal), string(writeMeNewVal), swapped, err)
			}
			oldVal = cur
			if !swapped {
				//vv("%v nice: CAS did not swap, on step %v!", s.name, step)
			}

			s.nemesis.makeTrouble()
		}
	}()
}

const vtyp101 = "string"

var ErrNeedNewSession = fmt.Errorf("need new session")

func (s *fuzzUser) CAS(ctxCAS context.Context, key string, oldVal, newVal Val) (swapped bool, curVal Val, err error) {

	var eventID int

	oldValStr := string(oldVal)
	newValStr := string(newVal)
	casAttempts := 0
	var tktW *Ticket
	for {
		eventID = int(s.atomicLastEventID.Add(1))
		casAttempts++
		callEvent := porc.Event{
			ClientId: s.userid,
			//Input:    &casInput{op: STRING_REGISTER_CAS, oldString: string(oldVal), newString: string(newVal)},
			Kind:  porc.CallEvent,
			Value: &casInput{op: STRING_REGISTER_CAS, oldString: oldValStr, newString: newValStr},
			Id:    eventID,
			//Call:  begtmWrite.UnixNano(), // invocation timestamp

			// assume error/unknown happens, update below if it did not.
			//Output: out,
			//Return:   endtmWrite.UnixNano(), // response timestamp
		}

		s.shEvents.mut.Lock()
		s.shEvents.evs = append(s.shEvents.evs, callEvent)
		s.shEvents.mut.Unlock()

		//vv("about to write from cli sess, writeMe = '%v'", string(newVal)) // seen lots

		// CAS (write)
		ctx5, canc5 := context.WithTimeout(ctxCAS, 5*time.Second)

		tktW, err = s.sess.CAS(ctx5, fuzzTestTable, Key(key), oldVal, newVal, 0, vtyp101, 0, leaseAutoDelFalse, 0, 0)
		//vv("%v just after sess.CAS, ctx5.Err()='%v', s.sess.ctx.Err()='%v'; while err='%v'", s.name, ctx5.Err(), s.sess.ctx.Err(), err) // ctx5.Err()==nil. while err='context canceled'; s.sess.ctx.Err()='context canceled';
		canc5()

		switch err {
		case ErrShutDown, rpc.ErrShutdown2,
			ErrTimeOut, rpc.ErrTimeout:
			vv("CAS write failed: '%v'", err) // not seen

			return
		}

		if err != nil {
			errs := err.Error()
			switch {
			case strings.Contains(errs, rejectedWritePrefix):
				// fair, fine to reject cas; the error forces us to deal with it,
				// but the occassional CAS reject is fine and working as expected.
				err = nil
				//vv("%v: rejectedWritePrefix seen", s.name) // seen.
				break
			case strings.Contains(errs, "context canceled"):
				vv("%v sees context canceled for s.sess.CAS() ", s.name)
				//err = ErrNeedNewSession
				return
			case strings.Contains(errs, "context deadline exceeded"):

				// have to try again...
				// Ugh: cannot just return, as then we will try
				// again with a different write value
				// and that means our session caching will be off!
				// try again locally.
				if casAttempts > 3 {
					vv("%v: about to return ErrNeedNewSession", s.name) // not seen!
					err = ErrNeedNewSession
					return
				}
				s.sess.SessionSerial-- // try again with same serial.

				vv("%v retry with same serial.", s.name) // seen lots
				continue
			}
		}
		panicOn(err)
		break
	}

	if tktW.DupDetected {
		vv("%v tkt.DupDetected true!", s.name)
	}

	if tktW.CASwapped {
		//vv("%v: CAS write ok on tktW = '%v'; tktW.Val='%v'", s.name, tktW.Desc, string(tktW.Val))
		swapped = true
		if string(tktW.Val) != string(newVal) {
			panicf("why does tktW.Val('%v') != newVal('%v')", string(tktW.Val), string(newVal))
		}
		curVal = Val(append([]byte{}, newVal...))

	} else {
		swapped = false // for emphasis
		curVal = Val(append([]byte{}, tktW.CASRejectedBecauseCurVal...))
		//vv("CAS write failed (did not write new value '%v'), we read back current value ('%v') instead", string(newVal), string(curVal))
	}
	out := &casOutput{
		op:       STRING_REGISTER_CAS,
		id:       eventID,
		valueCur: string(curVal),
		swapped:  swapped,

		oldString: oldValStr,
		newString: newValStr,
	}
	resultEvent := porc.Event{
		ClientId: s.userid,
		Kind:     porc.ReturnEvent,
		Id:       int(eventID),
		Value:    out,
		// no timestamp, because, from the porcupine docs:
		// "returns are only relatively ordered and do not
		// have absolute timestamps"
	}

	s.shEvents.mut.Lock()
	s.shEvents.evs = append(s.shEvents.evs, resultEvent)
	//vv("%v added returnEvent: len shEvents.evs now %v", s.name, len(s.shEvents.evs)) // not seen
	s.shEvents.mut.Unlock()

	return
}

var fuzzTestTable = Key("table101")

func (s *fuzzUser) Read(key string) (val Val, err error) {

	waitForDur := time.Second

	var tkt *Ticket

	eventID := int(s.atomicLastEventID.Add(1))
	callEvent := porc.Event{
		ClientId: s.userid,
		Id:       eventID,
		Kind:     porc.CallEvent,
		Value:    &casInput{op: STRING_REGISTER_GET},
	}

	if false {
		s.shEvents.mut.Lock()
		s.shEvents.evs = append(s.shEvents.evs, callEvent)
		s.shEvents.mut.Unlock()
	}

	// with message loss under nemesis, must be able to
	// timeout and retry
	ctx5, canc5 := context.WithTimeout(bkg, 5*time.Second)
	tkt, err = s.sess.Read(ctx5, fuzzTestTable, Key(key), waitForDur)
	canc5()
	if err == nil && tkt != nil && tkt.Err != nil {
		err = tkt.Err
	}
	if err != nil {
		vv("read from node/sess='%v', got err = '%v'", s.sess.SessionID, err)
		if err == ErrKeyNotFound || err.Error() == "key not found" {
			if false {
				returnEvent := porc.Event{
					ClientId: s.userid,
					Id:       eventID, // must match call event
					Kind:     porc.ReturnEvent,
					Value: &casOutput{
						op:       STRING_REGISTER_GET,
						id:       eventID,
						notFound: true,
					},
				}

				s.shEvents.mut.Lock()
				s.shEvents.evs = append(s.shEvents.evs, returnEvent)
				s.shEvents.mut.Unlock()
			}
			return
		}
		if err == rpc.ErrShutdown2 || err.Error() == "error shutdown" {
			return
		}
		errs := err.Error()
		if strings.Contains(errs, "context deadline exceeded") {
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

	returnEvent := porc.Event{
		ClientId: s.userid,
		Id:       eventID, // must match call event
		Kind:     porc.ReturnEvent,
		Value: &casOutput{
			op:       STRING_REGISTER_GET,
			id:       eventID,
			valueCur: string(val),
		},
	}

	s.shEvents.mut.Lock()
	s.shEvents.evs = append(s.shEvents.evs, callEvent)
	s.shEvents.evs = append(s.shEvents.evs, returnEvent)
	s.shEvents.mut.Unlock()

	return
}

// nemesis injects faults, makes trouble for user.
type fuzzNemesis struct {
	//mut sync.Mutex

	allowTrouble chan bool

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
	<-s.allowTrouble // a channel lock that synctest can durably block on.
	//s.mut.Lock() // why deadlocked?
	wantSleep := true
	//beat := time.Second

	defer func() {
		//s.mut.Unlock()
		s.allowTrouble <- true
		// we can deadlock under synctest if we don't unlock
		// before sleeping...
		if wantSleep {
			//time.Sleep(beat)
		}
	}()

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
			//vv("isNoop true")
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
				if numDamaged > 1 {
					// pick from one of the already damaged nodes
					which := int(s.rnd(int64(numDamaged)))
					node = s.damagedSlc[which]
				} else {
					node = s.damagedSlc[0]
				}
			}
		}
		r = fuzzFault(s.rnd(int64(fuzz_HEAL_NODE)))
	}

	implemented := false
	switch r {
	case fuzz_NOOP:
	case fuzz_PAUSE:
	case fuzz_CRASH:
	case fuzz_PARTITON:
		implemented = true

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
		implemented = true

		probDrop := 1.0
		probDeaf := 1.0
		if s.rnd(2) == 1 {
			vv("about to s.clus.Nodes[node].DropSends(probDrop)")
			s.clus.Nodes[node].DropSends(probDrop)
			vv("back from s.clus.Nodes[node].DropSends(probDrop)")
		} else {
			s.clus.Nodes[node].DeafToReads(probDeaf)
		}
	case fuzz_ONE_WAY_FAULT_PROBABALISTIC:
		implemented = true

		if s.rnd(2) == 1 {
			probDrop := s.rng.float64prob()
			vv("probDrop send = %v on node = %v", probDrop, node)
			s.clus.Nodes[node].DropSends(probDrop)
			vv("back from DropSends setting probDrop send = %v on node = %v", probDrop, node)
			wantSleep = false
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
		implemented = true

		var deliverDroppedSends bool
		if s.rnd(2) == 1 {
			deliverDroppedSends = true
		}
		s.clus.AllHealthyAndPowerOn(deliverDroppedSends)
	}
	if implemented {
		vv("makeTrouble at node = %v; r = %v; implemented = %v", node, r, implemented)
	}
}

func Test101_userFuzz(t *testing.T) {
	//return
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

			steps := 2
			numNodes := 3
			// numUsers of 20 ok at 200 steps, but 30 users is
			// too much for porcupine at even just 30 steps.
			numUsers := 2

			forceLeader := 0
			c, leaderName, leadi, _ := setupTestCluster(t, numNodes, forceLeader, 101)

			leaderURL := c.Nodes[leadi].URL
			defer c.Close()

			nemesis := &fuzzNemesis{
				rng:          rng,
				rnd:          rnd,
				clus:         c,
				damaged:      make(map[int]int),
				allowTrouble: make(chan bool, 1),
			}
			// unlock it
			nemesis.allowTrouble <- true

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
			defer cancel()

			atomicLastEventID := &atomic.Int64{}
			var users []*fuzzUser
			shEvents := &sharedEvents{}
			for userNum := 0; userNum < numUsers; userNum++ {
				user := &fuzzUser{
					atomicLastEventID: atomicLastEventID,
					shEvents:          shEvents,
					t:                 t,
					seed:              seed,
					name:              fmt.Sprintf("user%v", userNum),
					userid:            userNum,
					numNodes:          numNodes,
					rnd:               rnd,
					clus:              c,
					halt:              idem.NewHalterNamed("fuzzUser"),
					nemesis:           nemesis,
				}
				users = append(users, user)

			retryCli:
				cliName := fmt.Sprintf("client101_%v_%v", user.name, user.edition)
				cliCfg := *c.Cfg
				cliCfg.MyName = cliName
				cliCfg.PeerServiceName = TUBE_CLIENT
				cli := NewTubeNode(cliName, &cliCfg)
				user.cli = cli
				err := cli.InitAndStart()
				panicOn(err)
				defer cli.Close()

				// request new session
				// seems like we want RPC semantics for this
				// and maybe for other calls?

				sess := user.newSession(ctx, leaderName, leaderURL)
				if sess == nil {
					goto retryCli
				}

				user.cli = cli

				user.Start(ctx, steps, leaderName, leaderURL)
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

func (user *fuzzUser) newSession(ctx context.Context, leaderName, leaderURL string) *Session {
retry:
	ctx5, canc5 := context.WithTimeout(ctx, time.Second*10)
	sess, redirect, err := user.cli.CreateNewSession(ctx5, leaderName, leaderURL)
	canc5()
	if err != nil {
		errs := err.Error()
		if strings.Contains(errs, "context cancelled") {
			if redirect != nil {
				leaderName = redirect.LeaderName
				leaderURL = redirect.LeaderURL
			}
			alwaysPrintf("%v: err back from CreateNewSession, goto retry; err='%v'", user.name, errs) // not seen.
			goto retry
		}
	}
	if err != nil {
		alwaysPrintf("%v, CreateNewSession err = '%v'", user.name, err) // not seen.
		errs := err.Error()
		if strings.Contains(errs, "no leader known to me") ||
			strings.Contains(errs, "I am not leader") {

			if redirect != nil {
				leaderName = redirect.LeaderName
				leaderURL = redirect.LeaderURL
			}
			//goto retry // insufficient. we infinite loop.
			user.cli.Close()
			time.Sleep(time.Second)

			//snap0 := c.SimnetSnapshot(true)
			//vv("could not find leader. snap0 = '%v'", snap0) // .LongString())
			user.edition++
			return nil // get a new client, not just a new session
		}
		if strings.Contains(errs, "time-out waiting for call to complete") {
			// e.g. ExternalGetCircuitToLeader error: myPeer.NewCircuitToPeerURL to leaderURL 'simnet://srv_node_0/tube-replica/kmSYTOFyUDLsOFW5cNPsvxS9uSFv' (netAddr='simnet://srv_node_0') (onlyPossibleAddr='') gave err = 'error requesting CallPeerStartCircuit from remote: 'time-out waiting for call to complete'; netAddr='simnet://srv_node_0'; remoteAddr='simnet://srv_node_0'';
		}
		panic(err)
	}
	panicOn(err)
	if sess.ctx == nil {
		panic("sess.ctx should be not nil")
	}
	// ctx5 is already canceled, must replace.
	sess.ctx = ctx
	if errC := sess.ctx.Err(); errC != nil {
		panicf("sess.ctx.Err() should be not already be canceled!: errC='%v'", errC) // gotcha!
	}
	vv("%v got new sess", user.name) // , sess)
	user.sess = sess
	return sess
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

func writeToDiskNonLinzFuzzEvents(t *testing.T, user string, evs []porc.Event) {

	res, info := porc.CheckEventsVerbose(stringCasModel, evs, 0)
	if res != porc.Illegal {
		t.Fatalf("expected output %v, got output %v", porc.Illegal, res)
	}
	nm := fmt.Sprintf("red.nonlinz.%v.%03d.user_%v.html", t.Name(), 0, user)
	for i := 1; fileExists(nm) && i < 1000; i++ {
		nm = fmt.Sprintf("red.nonlinz.%v.%03d.user_%v.html", t.Name(), i, user)
	}
	vv("writing out non-linearizable evs history '%v'", nm)
	fd, err := os.Create(nm)
	panicOn(err)
	defer fd.Close()

	err = porc.Visualize(stringCasModel, info, fd)
	if err != nil {
		t.Fatalf("evs visualization failed")
	}
	t.Logf("wrote evs visualization to %s", fd.Name())
}

func writeToDiskOkEvents(t *testing.T, user string, evs []porc.Event) {

	res, info := porc.CheckEventsVerbose(stringCasModel, evs, 0)
	if res == porc.Illegal {
		t.Fatalf("expected output %v, got output %v", porc.Ok, res)
	}
	nm := fmt.Sprintf("green.linz.%v.%03d.user_%v.html", t.Name(), 0, user)
	for i := 1; fileExists(nm) && i < 1000; i++ {
		nm = fmt.Sprintf("green.linz.%v.%03d.user_%v.html", t.Name(), i, user)
	}
	vv("writing out linearizable evs history '%v'", nm)
	fd, err := os.Create(nm)
	panicOn(err)
	defer fd.Close()

	err = porc.Visualize(stringCasModel, info, fd)
	if err != nil {
		t.Fatalf("evs visualization failed")
	}
	t.Logf("wrote evs visualization to %s", fd.Name())
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

			ops = user.evs
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
	id        int
	op        stringRegisterOp
	oldString string
	newString string
}

type casOutput struct {
	id       int
	op       stringRegisterOp
	swapped  bool   // used for CAS
	notFound bool   // used for read
	valueCur string // for read/when cas rejects

	oldString string
	newString string
}

func (o *casOutput) String() string {
	var xtra string
	if o.op == STRING_REGISTER_CAS {
		xtra = fmt.Sprintf(`       swapped: %v,
  // -- from input --
	oldString: %q,
	newString: %q,
`,
			o.swapped,
			o.oldString,
			o.newString,
		)
	}
	//STRING_REGISTER_GET

	return fmt.Sprintf(`casOutput{
            id: %v,
            op: %v,
      valueCur: %q,
      notFound: %v,
%v}`, o.id,
		o.op,
		o.valueCur,
		o.notFound,
		xtra,
	)
}

func (ri *casInput) String() string {
	if ri.op == STRING_REGISTER_GET {
		return fmt.Sprintf("casInput{op: %v}", ri.op)
	}
	return fmt.Sprintf(`casInput{op: %v, oldString: %q, newString: %q}`, ri.op, ri.oldString, ri.newString)
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
				(!out.notFound && st == out.valueCur)
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

			if inp.oldString == st && out.swapped {
				legal = true
			} else if inp.oldString != st && !out.swapped {
				legal = true
			} else {
				//vv("warning: legal is false in CAS because out.swapped = '%v', inp.oldString = '%v', inp.newString = '%v'; old state = '%v', newState = '%v'; out.valueCur = '%v'", out.swapped, inp.oldString, inp.newString, st, newState, out.valueCur)
			}

			if legal { // does not seem to make a difference?? at least when green 101
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

			if out.swapped {
				return fmt.Sprintf("CAS(ok: '%v' ->'%v')", inp.oldString, inp.newString)
			}
			return fmt.Sprintf("CAS(rejected:old '%v' != cur '%v')", inp.oldString, out.valueCur)
		}
		panic(fmt.Sprintf("invalid inp.op! '%v'", int(inp.op)))
		return "<invalid>" // unreachable
	},
}
