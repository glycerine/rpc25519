package tube

import (
	"context"
	"fmt"

	"math"
	"math/big"
	"os"
	"runtime"
	"runtime/debug"
	"runtime/trace"
	"sort"
	"strings"
	//"strconv"
	"sync"

	"sync/atomic"
	"testing"
	"time"

	"github.com/glycerine/idem"
	rb "github.com/glycerine/rbtree"

	porc "github.com/glycerine/porcupine"
	//porc "github.com/anishathalye/porcupine"
	rpc "github.com/glycerine/rpc25519"
)

// 2026 Feb 15: a note on why we use
// github.com/glycerine/porcupine (v.1.2.4-jea)
// rather than github.com/anishathalye/porcupine (v1.1.0)
// as of this writing
//
// Per https://github.com/anishathalye/porcupine/issues/42
// there are two problems with anishathalye/porcupine (v1.1.0)
// for my use in fault tolerance testing:
//
// a) it rejects event histories where some calls do not have returns.
// Since our network partition/dropped packets robustness testing
// involves lost replies, porcupine will spuriously reject
// such histories on their face. glycerine/porcupine (v1.2.4-jea) filters out
// unmatched calls (with commit 2122eb2) from the history.
//
// b) filtering alone though will itself create non-linearizability
// if done alone, because those writes may have succeeded at
// the raft database server side, and it may have simply been
// the reply indicating success that got lost. So instead,
// below, we also tackle this by adding phantom
// porc.ReturnEvent(s) only when an unmatched write/cas
// CallEvent is observed to have actually succeeded in this fuzz test.
//
// We know a put/write/CAS actually succeeded because we write a unique
// value on every attempt, and so if that value
// is ever observed later in the history, then we know there was a successful
// write but a lost reply. We insert a phantom reply just
// before the first (earliest) observation of the written value.

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
	// out-bound CallEvent edges:
	// fromIdx -> toIdx -> eventID
	call map[int]map[int]int

	// in-bound ReturnEvent edges:
	// toIdx -> fromIdx -> eventID
	ret map[int]map[int]int
}

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
	perCli   map[int]*perClientEventStats
	dangling map[int]bool
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

func newTotalEventStats() *totalEventStats {
	s := &totalEventStats{
		perCli:   make(map[int]*perClientEventStats),
		dangling: make(map[int]bool),
	}
	return s
}

func (s *perClientEventStats) String() (r string) {
	return fmt.Sprintf(`
  =======   client %v event stats   ========
%v`, s.clientID, s.eventStats.String())
}

// show both total and include per-client specific.
func (s *totalEventStats) String() (r string) {
	r = fmt.Sprintf(`
  =======   total event stats   ========
%v`, s.eventStats.String())

	// sort so display order of per client is consistent
	var ids []int
	for clientID := range s.perCli {
		ids = append(ids, clientID)
	}
	sort.Ints(ids)
	for _, clientID := range ids {
		per := s.perCli[clientID]
		r += fmt.Sprintf("clientID %v stats: \n %v", clientID, per)
	}
	return
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
	tot = newTotalEventStats()

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
	} // end for range over evs

	// now do matching on eventID
	for i, e := range evs {
		eventID := e.Id
		clientID := e.ClientId
		per := tot.getPerClientStats(clientID)
		per.countAllEvents++
		tot.countAllEvents++

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
				tot.dangling[i] = true
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
	for _, per := range tot.perCli { // map[clientID]*perClientEventStats
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

func (s *fuzzUser) addPhantomsForUnmatchedCalls(evs []porc.Event, tot *totalEventStats) []porc.Event {
	if tot == nil {
		tot = basicEventStats(evs)
	}
	event2caller := make(map[int]int)
	// eventID -> ReturnEvent list of indexes into evs
	event2return := make(map[int]map[int]struct{})

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

	// now do matching on eventID, filling in seenVal in outputs
	// for all events.
	writeList := make(map[string]*intSet) // written string -> all the Call event indexes that attempted to write it.

	writValMin := make(map[string]int) // written string -> index written at (min)
	writValMax := make(map[string]int) // written string -> index written at (max)
	seenVal := make(map[string]int)    // written string -> index observed at (min)
	for i, e := range evs {
		//eventID := e.Id

		if e.Kind == porc.CallEvent {
			// extract written value for un-matched calls.
			inp, isInput := e.Value.(*casInput)
			if !isInput {
				panicf("why not casInput? e.Value='%#v'", e.Value)
			}
			switch inp.op {
			case STRING_REGISTER_UNK:
				panic("should be no STRING_REGISTER_UNK")
			case STRING_REGISTER_PUT:
				k, prior := writValMin[inp.newString]
				if !prior || i < k {
					// keep the smallest index observed.
					writValMin[inp.newString] = i
				}
				k, prior = writValMax[inp.newString]
				if !prior || i > k {
					// keep the largest index observed.
					writValMax[inp.newString] = i
				}
				wl, ok := writeList[inp.newString]
				if !ok {
					wl = &intSet{}
					writeList[inp.newString] = wl
				}
				wl.slc = append(wl.slc, i)
			case STRING_REGISTER_GET:
			case STRING_REGISTER_CAS:
				// so on retry at the same session
				// we _want_ to write the
				// exact same value again; so allow that
				// but find the minimum (first possible successful
				// write) index.
				k, prior := writValMin[inp.newString]
				if !prior || i < k {
					// keep the smallest index observed.
					writValMin[inp.newString] = i
				}
				k, prior = writValMax[inp.newString]
				if !prior || i > k {
					// keep the largest index observed.
					writValMax[inp.newString] = i
				}
				wl, ok := writeList[inp.newString]
				if !ok {
					wl = &intSet{}
					writeList[inp.newString] = wl
				}
				wl.slc = append(wl.slc, i)
			}
		} else {
			// INVAR: e.Kind == porc.ReturnEvent
			out, isOutput := e.Value.(*casOutput)
			if !isOutput {
				panicf("why not casOuput? e.Value='%#v'", e.Value)
			}
			switch out.op {
			case STRING_REGISTER_UNK:
				panic("should be no STRING_REGISTER_UNK")
			case STRING_REGISTER_PUT:
				// not using puts currently, so not sure
				// if there output will have anything relevant;
				// probably not.
			case STRING_REGISTER_GET:
				k, prior := seenVal[out.valueCur]
				if !prior || i < k {
					// keep the smallest index observed.
					seenVal[out.valueCur] = i
				}
			case STRING_REGISTER_CAS:
				k, prior := seenVal[out.oldString]
				if !prior || i < k {
					// keep the smallest index observed.
					seenVal[out.oldString] = i
				}
			}
		}
	}

	// leave evs untouched so that we do not mess up
	// our indexing as we insert into the tree. keys
	// are big.Rat so original will be on whole numbers
	// while phantoms will be have fractional keys.

	hist := newHistoryTree(evs)
	tree := hist.tree
	delWrite := func(d int, why string) {
		//vv("delWrite d=%v (%v) which was: %v", d, why, evs[d].String()) // seen lots with the fallthrough and retry at same session serial number.
		hist.del(d)

		// delete the corresponding ReturnEvent if any
		returnList, ok := event2return[evs[d].Id]
		if !ok {
			return
		}
		for r := range returnList {
			vv("delWrite corresponding ret=%v (%v) which was: %v", r, why, evs[r].String())
			hist.del(r)
		}
		/*		call, ok := event2caller[evs[d].Id]
				if !ok {
					panicf("where is call for write d=%v", d)
				}
				vv("delWrite corresponding call=%v (%v) which was: %v", call, why, evs[call])
				hist.del(call)
		*/
	}

topLoop:
	for it := tree.Min(); !it.Limit(); it = it.Next() {
		elem := it.Item().(*eventHistoryElem)
		if elem.phantom {
			continue // only add these, don't analyze them; skip over.
		}
		e := elem.pev
		//i := elem.origi

		eventID := e.Id
		clientID := e.ClientId

		if e.Kind != porc.CallEvent {
			continue
		}
		// INVAR: only CallEvents are being handled now.

		// For un-answered CallEvents (those without
		// a corresponding ReturnEvent) that actually did
		// succeed (because we observed their unique
		// written values in the history), we insert
		// a phantom ReturnEvent to let porcupine not choke.
		inp := e.Value.(*casInput)

		_, ok := event2return[eventID]
		if ok {
			// this call has a response, no need for a phantom one.
			continue
		}
		// no returns/replies for this event, it needs a phantom
		// if observed (hidden but successful write)

		var newString, oldString string
		switch inp.op {
		case STRING_REGISTER_UNK:
			panic("should be no STRING_REGISTER_UNK")
		case STRING_REGISTER_PUT:
			newString = inp.newString
		case STRING_REGISTER_GET:
			// unmatched reads can just be deleted.
			// In fact this helps an unpatched porcupine
			// not spuriously reject a history for not
			// having a matched return. i.e. if using
			// anishathalye/porcupine instead of glycerine/porcupine
			// where I fixed that issue by filtering
			// out unmatched calls first.

			// TODO: delete i without messing up anything else?

			// Really we have no idea what should be observed, and no
			// way to know what observed value would be
			// linearizable, so we cannot insert a phantom value
			// without breaking the checker.

			// For now we just let the glycerine/phantom checker.go
			// fix filter out un-matched calls after adding phantom returns.

			continue // skip the read, move on to next unmatched write.

		case STRING_REGISTER_CAS:
			newString = inp.newString
			oldString = inp.oldString
		}
		if newString == "" {
			continue
		}
		// o is the first (minimal) observed index.
		o, ok := seenVal[newString]
		if !ok {
			continue
		}
		// value can be observed more than once so o != i is viable.

		// w is the earliest (smallest) write index.
		// we might have multiple retries attempting the same write.
		// which one of them succeeded? all we know is that the
		// write should linearize between the first and the last
		// attempt at writing if the value is actually observed
		// at some point.
		w0, ok := writValMin[newString]
		if !ok {
			panicf("internal logic error: we should have newString='%v' in writValMin", newString)
		}
		w1, ok := writValMax[newString]
		if !ok {
			panicf("internal logic error: we should have newString='%v' in writValMax", newString)
		}
		if w1 != w0 {
			// pick an actual w0 between [w0, w1]

			// what are all the attempts at writing?

			//ci := tot.getPerClientStats(clientID).cliInfo

			wl := writeList[newString]
			sort.Ints(wl.slc)
			// if we saw a later CAS that got a reply AND
			// the swap happened, then we know it was that
			// attempt that worked, and we do not want a phantom.

			//vv("at i = %v, wl.slc = '%#v'", elem.origi, wl.slc)
			remain := intSlice2set(wl.slc)
			for q, k := range wl.slc {
				if k >= o {
					// we know the write had to come before the observation.
					// wl.slc is sorted, we can delete k and all after too
					why := fmt.Sprintf("past o=%v; w0=%v; w1=%v", o, w0, w1)
					for qq := q; qq < len(wl.slc); qq++ {
						kk := wl.slc[qq]
						delWrite(kk, why)
						delete(remain, kk)
					}
					if len(remain) == 0 {
						continue topLoop
					}
					break // done looking at all of wl.slc
				}
				eventID_ := evs[k].Id
				returnList, ok := event2return[eventID_]
				if !ok {
					continue
				}
				for iRet := range returnList {
					out := evs[iRet].Value.(*casOutput)
					if !out.swapped {
						continue
					}
					//swappedAt = iRet
					// this one succeeded. We can
					// delete the others if they are dangling;
					// they had better be!
					why := fmt.Sprintf("fine/swapped at %v", iRet)
					for k2 := range wl.slc {
						if k2 != k {
							if tot.dangling[k2] {
								delWrite(k2, why)
								delete(remain, k2)
							} else {
								panicf("how can we have more than one successful CAS on this eventID_ %v ? k=%v and k2=%v", eventID_, k, k2)
							}
						}
					}
					// since we found a successfully swapped CAS,
					// we don't want to inject a phantom for this write.
					continue topLoop
				}
			}
			if len(remain) == 0 {
				continue topLoop
			}
			// all of remain are dangling or not-swapped;
			// none completed that we know of.

			// also we need to be after the oldString value was written!
			old, ok := writValMin[oldString]
			if !ok {
				// never seen so any will work
			} else {
				var delme []int
				for r := range remain {
					if r <= old {
						delme = append(delme, r)
					}
				}
				why := "must be after writValMin[oldString]"
				for _, d := range delme {
					delWrite(d, why)
					delete(remain, d)
				}
			}
			nr := len(remain)
			if nr == 0 {
				continue topLoop
			}
			if nr == 1 {
				for r := range remain {
					w0 = r
				}
			} else {
				// INVAR: any of the dangling remain can be used;
				// pick the smallest one, and delete the others
				// since they will dangle anyway.

				slc := set2intSliceSorted(remain)
				why := "picked smallest remain for w0"
				for si, r := range slc {
					if si == 0 {
						w0 = r
					} else {
						delWrite(r, why)
					}
				}
			}
		}
		if w0 >= o {
			alwaysPrintf("evs='%v'", eventSlice(evs))
			panicf("invalid history: cannot observe(o=%v) before unique(!) value is written at earliest (w0=%v). has uniqueness been violated? value newString='%v'", o, w0, newString)
		}
		// this can certainly happen when i is a retry after a failed write
		// but someone else sees the actually successful write.
		//if i > o {
		//	panicf("invalid history: how can we write a unique value *after* it has been observed already?? i(%v) should not be > o(%v)", i, o)
		//}

		// INVAR: w0 < o

		// seen, so insert the phantom successful ReturnEvent (just) before o.
		var out *casOutput

		switch inp.op {
		case STRING_REGISTER_UNK:
			panic("should be no STRING_REGISTER_UNK")
		case STRING_REGISTER_PUT:
			out = &casOutput{
				op:        STRING_REGISTER_PUT,
				id:        eventID,
				valueCur:  inp.newString,
				newString: inp.newString,
				phantom:   true,
			}
		case STRING_REGISTER_GET:
			panic("GET should have been ignored above")
			// do not insert, no idea what the value should be.
		case STRING_REGISTER_CAS:
			out = &casOutput{
				op:        STRING_REGISTER_CAS,
				id:        eventID,
				valueCur:  inp.newString, // because we know the CAS actually wrote.
				swapped:   true,
				oldString: inp.oldString,
				newString: inp.newString,
				phantom:   true,
			}
		}

		phantom := &porc.Event{
			Ts:       evs[o].Ts - 1,
			ClientId: clientID,
			Kind:     porc.ReturnEvent,
			Id:       eventID,
			Value:    out,
		}

		//vv("inserting phantom at o, where o(%v) > w0(%v): '%v'", o, w0, phantom)
		hist.insertBefore(o, phantom)

	} // end for over all nodes in tree

	// copy into new slice.
	evs2 := make([]porc.Event, hist.tree.Len())
	i := 0
	for it := tree.Min(); !it.Limit(); it = it.Next() {
		evs2[i] = *(it.Item().(*eventHistoryElem).pev)
		i++
	}
	return evs2
}

type eventHistoryElem struct {
	key     *big.Rat
	pev     *porc.Event
	origi   int
	phantom bool
}
type historyTree struct {
	tree *rb.Tree
}

func set2intSliceSorted(m map[int]bool) (slc []int) {
	for k := range m {
		slc = append(slc, k)
	}
	sort.Ints(slc)
	return
}
func intSlice2set(slc []int) (m map[int]bool) {
	m = make(map[int]bool)
	for _, i := range slc {
		m[i] = true
	}
	return
}

func newHistoryTree(evs []porc.Event) (s *historyTree) {
	s = &historyTree{
		tree: rb.NewTree(func(a, b rb.Item) int {
			av := a.(*eventHistoryElem)
			bv := b.(*eventHistoryElem)
			if av == bv {
				return 0
			}
			return av.key.Cmp(bv.key)
		}),
	}
	for i := range evs {
		elem := &eventHistoryElem{
			// we never have to insert before the first event,
			// so we can start at 0 and still keep all keys >= 0.
			key:   big.NewRat(int64(i), 1),
			pev:   &(evs[i]),
			origi: i,
		}
		s.tree.Insert(elem)
	}
	return
}

var bigZero = big.NewRat(0, 1)

func (s *historyTree) del(d int) {
	//vv("del d=%v", d)
	dkey := big.NewRat(int64(d), 1)
	query := &eventHistoryElem{
		key: dkey,
	}
	s.tree.DeleteWithKey(query)
}
func (s *historyTree) insertBefore(w int, addme *porc.Event) {
	wkey := big.NewRat(int64(w), 1)
	query := &eventHistoryElem{
		key: wkey,
	}
	it, exact := s.tree.FindGE_isEqual(query)
	if !exact {
		panicf("could not find w = %v in tree", w)
	}
	ahead := it.Item().(*eventHistoryElem)
	var key *big.Rat
	if it.Min() {
		panic("currently we cannot insert before the 0-th element in the original event history")
	} else {
		prev := it.Prev().Item().(*eventHistoryElem)
		key = keyBetween(prev.key, ahead.key)
	}
	add := &eventHistoryElem{
		key:     key,
		pev:     addme,
		phantom: true,
	}
	added := s.tree.Insert(add)
	if !added {
		panicf("why was not added to tree key '%v'?", key)
	}
}

// to insert between two existing keys
func keyBetween(a, b *big.Rat) *big.Rat {
	sum := new(big.Rat).Add(a, b)
	return sum.Quo(sum, big.NewRat(2, 1))
}

func (s *fuzzUser) linzCheck() {
	// Analysis
	// Expecting some events
	s.shEvents.mut.Lock()
	defer s.shEvents.mut.Unlock()

	evs := s.shEvents.evs

	totalStats := basicEventStats(evs)
	//alwaysPrintf("linzCheck basic event stats (pre-phantom):\n %v", totalStats)

	//alwaysPrintf("debug print all events: evs =\n %v", eventSlice(evs))

	// calls that may or may not have succeeded can mess up
	// the linearization check. write/cas only unique values,
	// and if an uncertain (unmatched CallEvent) is observed
	// later then we know it must have succeeded after all; so
	// insert a phantom ReturnEvent just prior to the first
	// observation of that unique value later in the
	// event sequence. Note: we cannot support deletes
	// under this regime. We might turn them negative or
	// invent something else later, but for now, no deletion.
	s.shEvents.evs = s.addPhantomsForUnmatchedCalls(s.shEvents.evs, totalStats)
	evs = s.shEvents.evs

	//totalStats2 := basicEventStats(evs)
	//alwaysPrintf("linzCheck basic event stats (post-phantom):\n %v", totalStats2)

	if len(evs) == 0 {
		panicf("user %v: expected evs > 0, got 0", s.name)
	}

	//alwaysPrintf(" wal:\n")
	//err := s.clus.Nodes[0].DumpRaftWAL()
	//panicOn(err)

	s.events2obsThenCheck(evs, totalStats)
	return
	/*
	   //vv("linzCheck: about to porc.CheckEvents on %v evs", len(evs))
	   linz := porc.CheckEvents(stringCasModel, evs)

	   	if !linz {
	   		alwaysPrintf("not linz! wal:\n")
	   		dump := "test101fuzz_test.wal.dump"
	   		fd, err := os.Create(dump)
	   		panicOn(err)
	   		err = s.clus.Nodes[0].DumpRaftWAL(fd)
	   		fd.Close()
	   		panicOn(err)

	   		alwaysPrintf("error: user %v: expected operations to be linearizable! seed='%v'; evs='%v'", s.name, s.seed, eventSlice(evs))
	   		writeToDiskNonLinzFuzzEvents(s.t, s.name, evs)
	   		panicf("error: user %v: expected operations to be linearizable! seed='%v'", s.name, s.seed)
	   	}

	   vv("user %v: len(evs)=%v passed linearizability checker.", s.name, len(evs))

	   // nothing much to see really.
	   writeToDiskOkEvents(s.t, s.name, evs)
	*/
}

func (s *fuzzUser) events2obsThenCheck(evs []porc.Event, totalStats *totalEventStats) {

	// we assume phantom insertion and dup/unmatched return deletion
	// has already been done by now.
	event2caller := make(map[int]int)
	event2return := make(map[int]int)
	for i, e := range evs {
		eventID := e.Id
		if e.Kind == porc.CallEvent {
			// sanity assert there is no prior event2caller entry.
			prior, already := event2caller[eventID]
			if already {
				panicf("sanity check failed: for CallEvent eventID(%v) have prior(%v)", eventID, prior)
			}
			event2caller[eventID] = i
			continue
		}
		// e == evs[i] is a porc.ReturnEvent
		prior, already := event2return[eventID]
		if already {
			panicf("sanity check failed: ReturnEvent eventID(%v) have prior(%v)", eventID, prior)
		}
		event2return[eventID] = i
	}
	var ops []porc.Operation
	for i, e := range evs {
		eventID := e.Id
		if e.Kind == porc.CallEvent {
			r, ok := event2return[eventID]
			if !ok {
				// meh. skip it.
				continue
				//alwaysPrintf(" evs='%v'", eventSlice(evs))
				alwaysPrintf("i=%v; call('%v') call without return. should have just been deleted?", i, e.String())
				panic("left danging call???")
				continue
			}
			ret := evs[r]
			op := porc.Operation{
				ClientId: e.ClientId,
				Input:    e.Value,
				Call:     e.Ts,
				Output:   ret.Value,
				Return:   ret.Ts,
			}
			ops = append(ops, op)
		}
	}
	//vv("linzCheck: about to porc.CheckEvents on %v evs", len(evs))
	linz := porc.CheckOperations(stringCasModel, ops)
	if !linz {

		for i := range s.clus.Nodes {
			dump := fmt.Sprintf("test101fuzz_test.wal.dump.%v.red", i)
			alwaysPrintf("not linz! wal dumped to '%v'\n", dump)
			fd, err := os.Create(dump)
			panicOn(err)
			err = s.clus.Nodes[i].DumpRaftWAL(fd)
			fd.Close()
			panicOn(err)
		}
		n := len(s.clus.Nodes)
		for i := range n {
			for j := i + 1; j < n; j++ {
				vv(" ------- first diff of log i=%v vs j=%v:\n %v \n\n", i, j, firstDiffLogs(s.clus.Nodes[i].wal, s.clus.Nodes[j].wal))
			}
		}
		alwaysPrintf("error: user %v: expected operations to be linearizable! seed='%v'; ops='%v'", s.name, s.seed, opsSlice(ops)) // eventSlice(evs))
		alwaysPrintf("and the raw events before we inserted phantoms and assembled into ops: '%v'", eventSlice(evs))               // totalStats.eventStats.String())
		writeToDiskNonLinzFuzz(s.t, s.name, ops)
		panicf("error: user %v: expected operations to be linearizable! seed='%v'", s.name, s.seed)
	}

	for i := range s.clus.Nodes {
		dump := fmt.Sprintf("test101fuzz_test.wal.dump.%v.green", i)
		alwaysPrintf("yes linz, and still wal dumped to '%v'\n", dump)
		fd, err := os.Create(dump)
		panicOn(err)
		err = s.clus.Nodes[i].DumpRaftWAL(fd)
		fd.Close()
		panicOn(err)
	}
	vv("user %v: len(ops)=%v passed linearizability checker.", s.name, len(ops))

	// nothing much to see really.
	writeToDiskOkOperations(s.t, s.name, ops) // not written yet.
}

func (s *fuzzUser) Start(startCtx context.Context, steps int, leaderName, leaderURL string, domain int, domainSeen *sync.Map, domainLast *atomic.Int64) {

	go func() {
		defer func() {
			s.halt.ReqStop.Close()
			s.halt.Done.Close()
		}()
		var prevCanc context.CancelFunc = func() {}
		defer func() {
			prevCanc()
		}()

		// get initial session and check that startCtx is still valid.
		restart := func() (ok bool) {
			select {
			case <-startCtx.Done():
				vv("restart false: startCtx.Done has closed")
				return false
			case <-s.halt.ReqStop.Chan:
				vv("restart false: s.halt.ReqStop.Chan closed")
				return false
			default:
			}

			_, _, err := s.newSession(startCtx, leaderName, leaderURL)
			if err == nil {
				return true
			}
			//time.Sleep(time.Millisecond * 100)
			if cerr := startCtx.Err(); cerr != nil {
				//alwaysPrintf("%v startCtx cancelled: returning", s.name)
				return false
			}

			ctx5, canc5 := context.WithTimeout(startCtx, time.Second*5)
			err2 := restartFullHelper(ctx5, s.name, s.cli, &s.sess, s.halt)
			canc5()
			if err2 != nil {
				// this is bad. bailout needed.
				vv("restart false: restartFullHelper err2 = '%v'", err2)
				return false
			}
			return true
		} // end definition of restart()

		// set s.sess to a fresh session. s.sess==nil here;
		// we have no Session as of yet.
		if !restart() { // in Start(), before the step-loop.
			// startCtx was cancelled
			return
		}
		for step := range steps {
			prevCanc()
			stepCtx, canc := context.WithTimeout(startCtx, time.Second*10)
			prevCanc = canc
			if step%100 == 0 {
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
			var redirect *LeaderRedirect

			if step == 0 || !swapped {
				oldVal, redirect, err = s.Read(key)
				//time.Sleep(time.Millisecond * 500)
				_ = redirect
				if err != nil {
					errs := err.Error()
					switch {
					case strings.Contains(errs, "key not found"):
						if step < 3 {
							// allowed on first 3
							err = nil
						} else {
							// seen on step 1 too. users 5, nodes 3, steps 5000.
							continue
							//panicf("%v key not found on step %v!?!", s.name, step)
						}
						// used to just default: into restart. but try
						// bringing back more retries without that (slow) recovery,
						// to get more testing coverage.

					case strings.Contains(errs, "error shutdown"):
						// nemesis probably shut down the node
						vv("%v: try again on shutdown error", s.name)

						// do we need to reconnect to try again?
						continue
					case strings.Contains(errs, "context deadline exceeded"):
						// message loss due to nemesis likely
						//vv("%v: try again on timeout", s.name)

						// do we need to reconnect to try again?
						continue

						// restart works really well, just not as good a test.
					default:
						// all other errors, just restart from scratch.
						if !restart() { // in step loop inside Start()
							return
						}
						continue
					}
				} // else err == nil
			} // end if step == 0 || !swapped

			// draw unique values from domain, and only
			// accept unique new write/cas values every time,
			// so we can insert phantom ReturnEvents for
			// calls that did not get an actual return.
			// check in domainSeen to make sure they are unique.
			var proposedVal int
			for {
				proposedVal = int(s.rnd(int64(domain)))
				_, loaded := domainSeen.LoadOrStore(proposedVal, true)
				if !loaded {
					// we stored proposedVal, rather than reading ("loading") it.
					// proposedVal is uniquely ours now, across all client users.
					break
				}
			}
			proposedVal = int(domainLast.Add(3)) // sequential instead, easier debugging. ugh. Add(1) masks the blast-from-the-past bug.
			writeMeNewVal := Val([]byte(fmt.Sprintf("%v", proposedVal)))
			swapped, cur, err = s.CAS(stepCtx, key, oldVal, writeMeNewVal)

			// "need new session" every time.
			//vv("s.CAS err -> %v", err)

			if err != nil {
				// restart() was extensively tested and will
				// recover, but at the cost of making
				// a whole new session and losing the current session.
				//
				// So doing an immediate restart() does not
				// test our session handling code,
				// which is actually important code for providing
				// linearizability. (Chapter 6 of Raft dissertation).
				// So first try just keep going...
				if err == ErrNeedNewSession {
					if !restart() {
						return
					}
				}
				continue
			}

			if len(cur) == 0 {
				panicf("why is cur value empty after first CAS? oldVal='%v', writeMeNewVal='%v', swapped='%v', err='%v'", string(oldVal), string(writeMeNewVal), swapped, err)
			}
			oldVal = cur
			if swapped {
				//time.Sleep(time.Millisecond * 3000)
			} else {
				//vv("%v nice: CAS did not swap, on step %v!", s.name, step)
			}
			//time.Sleep(time.Millisecond * 500)

			// leaved commented out b/c racy to call me() externally.
			//vv("%v calling makeT", s.cli.me())
			s.nemesis.makeTrouble(s.name)
		}
	}()
}

const vtyp101 = "string"

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
			Ts:       time.Now().UnixNano(),
			ClientId: s.userid,
			//Input:    &casInput{op: STRING_REGISTER_CAS, oldString: string(oldVal), newString: string(newVal)},
			Kind: porc.CallEvent,
			Value: &casInput{
				op:        STRING_REGISTER_CAS,
				oldString: oldValStr,
				newString: newValStr,
				sessSN:    s.sess.SessionSerial + 1,
				sessID:    s.sess.SessionID,
			},
			Id: eventID,
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

		if tktW != nil {
			if tktW.DupDetected {
				vv("%v s.sess.CAS() got a DupDetected back! tkt.LogIndex='%v'; tkt.Term='%v'; tkt='%v'", s.name, tktW.LogIndex, tktW.Term, tktW) // not seen, red 101 non-linz.
			}
		}

		switch err {
		case ErrShutDown, rpc.ErrShutdown2,
			ErrTimeOut, rpc.ErrTimeout:
			vv("CAS write failed: '%v'", err) // not seen

			return
		}

		if err != nil {
			//vv("%v do we get here? err= '%v'", s.name, err) // yes, seen a bunch; all saying "context canceled"
			errs := err.Error()
			switch {
			case strings.Contains(errs, rejectedWritePrefix):
				// fair, fine to reject cas; the error forces us to deal with it,
				// but the occassional CAS reject is fine and working as expected.
				err = nil
				//vv("%v: rejectedWritePrefix seen", s.name) // seen.
				break

			case strings.Contains(errs, "error: I am not leader"):
				time.Sleep(time.Second)
				return // TODO: can we keep session live across leader change.

			case strings.Contains(errs, "context canceled"): // every time?!?! why?
				//vv("%v sees context canceled for s.sess.CAS() ", s.name) // seen ever time
				//err = ErrNeedNewSession
				//return
				fallthrough
			case strings.Contains(errs, "context deadline exceeded"):

				// have to try again...
				// Ugh: cannot just return, as then we will try
				// again with a different write value
				// and that means our session caching will be off!
				// try again locally.
				if casAttempts > 10 {
					//vv("%v: about to return ErrNeedNewSession", s.name) // seen.
					err = ErrNeedNewSession
					return
				}
				s.sess.SessionSerial-- // try again with same serial.

				//vv("%v retry with same serial.", s.name) // used to be seen lots, not seen after restart() changeover. Seen a bunch with the fallthrough just above.
				continue
			case err == ErrNeedNewSession:
				vv("%v err == ErrNeedNewSession back from s.sess.CAS", s.name)
				return

			case strings.Contains(errs, "Must call CreateNewSession first"):
				return
			}
		}
		if err != nil {
			return
		}
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
		Ts:       time.Now().UnixNano(),
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

func (s *fuzzUser) Read(key string) (val Val, redir *LeaderRedirect, err error) {

	waitForDur := time.Second

	var tkt *Ticket

	eventID := int(s.atomicLastEventID.Add(1))
	callEvent := porc.Event{
		Ts:       time.Now().UnixNano(),
		ClientId: s.userid,
		Id:       eventID,
		Kind:     porc.CallEvent,
		Value:    &casInput{op: STRING_REGISTER_GET},
	}

	//if false {
	if true {
		s.shEvents.mut.Lock()
		s.shEvents.evs = append(s.shEvents.evs, callEvent)
		s.shEvents.mut.Unlock()
	}

	// with message loss under nemesis, must be able to
	// timeout and retry
	ctx5, canc5 := context.WithTimeout(bkg, 5*time.Second)
	tkt, err = s.sess.Read(ctx5, fuzzTestTable, Key(key), waitForDur)
	canc5()
	if tkt != nil {
		if tkt.DupDetected {
			vv("%v s.sess.Read() got a DupDetected back! tkt.LogIndex='%v'; tkt.Term='%v'; tkt='%v'", s.name, tkt.LogIndex, tkt.Term, tkt) // not seen, 101 red non-linz.
		}
	}

	if err != nil && ((err == ErrShutDown || err == rpc.ErrShutdown2) ||
		strings.Contains(err.Error(), "shutdown")) {
		return
	}
	if err == nil && tkt != nil && tkt.Err != nil {
		err = tkt.Err
	}
	if err != nil {
		if err == ErrKeyNotFound || err.Error() == "key not found" {
			// if false {
			// 	returnEvent := porc.Event{
			//      Ts:       time.Now.UnixNano(),
			// 		ClientId: s.userid,
			// 		Id:       eventID, // must match call event
			// 		Kind:     porc.ReturnEvent,
			// 		Value: &casOutput{
			// 			op:       STRING_REGISTER_GET,
			// 			id:       eventID,
			// 			notFound: true,
			// 		},
			// 	}
			//
			// 	s.shEvents.mut.Lock()
			// 	s.shEvents.evs = append(s.shEvents.evs, returnEvent)
			// 	s.shEvents.mut.Unlock()
			// }
			return
		}
		//vv("read from node/sess='%v', got err = '%v'", s.sess.SessionID, err)
		if err == rpc.ErrShutdown2 || err.Error() == "error shutdown" {
			return
		}
		errs := err.Error()
		if strings.Contains(errs, "context deadline exceeded") {
			//vv("%v fuzzUser.Read returning on err: context deadline exceeded", s.name)
			return
		}

		// e.g. error: I am not leader. I ('node_0') think leader is 'node_2'
		if strings.Contains(errs, "error: I am not leader.") {
			redir = &LeaderRedirect{
				LeaderID:   tkt.LeaderID,
				LeaderName: tkt.LeaderName,
				LeaderURL:  tkt.LeaderURL,
			}
			return
		}

		// "Must call CreateNewSession first"

		return
		// panic: node_2 leader error: clock mis-behavior detected and client must re-submit ticket to preserve sequential consistency. Ticket.SessionID='evpvvaOdaoCuW30JNWFJ_ZcgK960' (SessionSerial='16'); tkt.SessionLastKnownIndex(61) > s.state.LastApplied(57)
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
		Ts:       time.Now().UnixNano(),
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
	//s.shEvents.evs = append(s.shEvents.evs, callEvent)
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

	calls int
}

func (s *fuzzNemesis) makeTrouble(caller string) {
	<-s.allowTrouble // a channel lock that synctest can durably block on.
	//s.mut.Lock() // why deadlocked?
	wantSleep := true
	//beat := time.Second

	s.calls++

	defer func() {
		// diagnostic, if need be:
		//const skipTrafficTrue = true
		//snap := s.clus.SimnetSnapshot(skipTrafficTrue)
		//vv("%v after makeTrouble call %v, snap = '%v'", caller, s.calls, snap.ShortString()) // .LongString())

		//s.mut.Unlock()
		s.allowTrouble <- true
		// we can deadlock under synctest if we don't unlock
		// before sleeping...
		if wantSleep {
			//time.Sleep(beat)
		}

	}()

	//vv("makeTrouble running, calls = %v", s.calls)

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
		// maybe we need a fault mode were all connections
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
	showBinaryVersion("tube.test")
	fmt.Println("Go version:", runtime.Version())

	defer func() {
		vv("test 101 wrapping up.")
	}()

	// [0, 10) 48.4s runtime with 20 users, 100 steps, 3 nodes.
	// seed 0, 7 nodes, 20 users, 1000 steps:
	// 9.88s, 10458 ops passed linearizability checker.
	begScenario := 20_000
	endxScenario := 20_200
	//endxScenario := 10_000
	//endxScenario := 20_000
	alwaysPrintf("begScenario = %v; endxScenario = %v", begScenario, endxScenario)
	for scenario := begScenario; scenario < endxScenario; scenario++ {

		seedString := fmt.Sprintf("%v", scenario)
		seed, seedBytes := parseSeedString(seedString)

		//runtime.ResetDsimSeed(seed)

		if int(seed) != scenario {
			panicf("got %v, wanted same scenario number back %v", int(seed), scenario)
		}
		rng := newPRNG(seedBytes)
		rnd := rng.pseudoRandNonNegInt64Range

		// seed 0 numbers: 3 node cluster. GOMAXPROCS(1). (without makeTrouble though!)
		// 10 users,  5 steps =>  44 ops.
		// 10 users, 10 steps =>  99 ops.
		// 10 users, 20 steps => 209 ops.
		// 10 users, 30 steps => 319 ops.
		// 10 users, 40 steps => 429 ops.
		// 10 users, 50 steps => 539 ops. (12.8s runtime)
		// 15 users, 100 steps => 1584 ops. (4.6s with prints quiet)
		// 20 users, 100 steps => 2079 ops. (5.1s)
		//  5 users, 1000 steps=> 2975 ops. (14.6s)
		// 15 users, 10_000 steps => 159984 ops. (17 minutes; 1054s on rog).
		//
		// seed 0, 5 nodes in cluster
		// 20 users, 1000 steps => 10458 ops. (10.71s with prints quiet)
		// same, but ten seeds 0-9: green, 88 seconds.
		steps := 20 // 1000 ok. 20 ok. 15 ok for one run; but had "still have a ticket in waiting"
		numNodes := 3
		// numUsers of 20 ok at 200 steps, but 30 users is
		// too much for porcupine at even just 30 steps.
		numUsers := 3
		//numUsers := 5 // green at 5 (10 steps)
		//numUsers := 9 // 7=>258 events, 8=>310 events, 9=>329 events,10=>366
		//numUsers := 15 // inf err loop at 15 (10 steps)

		alwaysPrintf("top of seed/scenario = %v ; steps = %v ; numNodes = %v ; numUsers = %v", scenario, steps, numNodes, numUsers)

		onlyBubbled(t, func(t *testing.T) {

			// 1% or less collision probability, to minimize
			// rejection sampling and get unique write values quickly.
			domain := steps * numUsers * 100
			domainSeen := &sync.Map{}
			domainLast := &atomic.Int64{}

			forceLeader := 0
			cfg := NewTubeConfigTest(numNodes, t.Name(), faketime)
			cfg.NoLogCompaction = true

			c, leaderName, leadi, _ := setupTestClusterWithCustomConfig(cfg, t, numNodes, forceLeader, 101)

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
			users[0].linzCheck()

			if nemesis.calls == 0 {
				panic("nemesis was never called!")
			}
			vv("makeTrouble calls total = %v", nemesis.calls)
		})

	}
}

func (s *fuzzUser) newSession(ctx context.Context, leaderName, leaderURL string) (sess *Session, redir *LeaderRedirect, err error) {

	//vv("%v top of newSession", s.name)
	if ctx.Err() != nil {
		//vv("%v newSession: submitted call ctx already cancelled! '%v'", s.name, ctx.Err()) // not seen
		return nil, nil, ctx.Err()
	}

	defer func() {
		vv("%v end of newSession; err = '%v'", s.name, err)
		if err == nil && sess.ctx.Err() != nil {
			panicf("problem: newSession err==nil, but sess.ctx already done! Err()='%v'", sess.ctx.Err()) // context cancelled.
		}
	}()
	if s.sess != nil {
		s.sess.Close()
	}

	// top ctx cancelled?
	if cerr := ctx.Err(); cerr != nil {
		return nil, nil, cerr
	}

	ctx5, canc5 := context.WithTimeout(ctx, time.Second*5)
	sess, redir, err = s.cli.CreateNewSession(ctx5, leaderName, leaderURL)
	canc5()
	if err == nil {
		vv("%v got err=nil and new sess %p back from CreateNewSession(); redir='%v'; called with leaderName='%v'", s.name, sess, redir, leaderName)
		s.sess = sess
	}
	return
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
	vv("writing out non-linearizable evs history '%v' len %v", nm, len(evs))
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
	vv("writing out linearizable evs history '%v', len %v", nm, len(evs))
	fd, err := os.Create(nm)
	panicOn(err)
	defer fd.Close()

	err = porc.Visualize(stringCasModel, info, fd)
	if err != nil {
		t.Fatalf("evs visualization failed")
	}
	t.Logf("wrote evs visualization to %s", fd.Name())
}

func writeToDiskOkOperations(t *testing.T, user string, ops []porc.Operation) {

	res, info := porc.CheckOperationsVerbose(stringCasModel, ops, 0)
	if res == porc.Illegal {
		t.Fatalf("expected output %v, got output %v", porc.Ok, res)
	}
	nm := fmt.Sprintf("green.linz.%v.%03d.user_%v.html", t.Name(), 0, user)
	for i := 1; fileExists(nm) && i < 1000; i++ {
		nm = fmt.Sprintf("green.linz.%v.%03d.user_%v.html", t.Name(), i, user)
	}
	vv("writing out linearizable ops history '%v', len %v", nm, len(ops))
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

	sessSN int64
	sessID string
}

type casOutput struct {
	id       int
	op       stringRegisterOp
	swapped  bool   // used for CAS
	notFound bool   // used for read
	valueCur string // for read/when cas rejects

	oldString string
	newString string

	phantom bool
	sessSN  int64
}

func (o *casOutput) String() string {
	var xtra, phant string
	if o.phantom {
		phant = `
      phantom: true
`
	}
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
%v%v}`, o.id,
		o.op,
		o.valueCur,
		o.notFound,
		xtra,
		phant,
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
		inp := input.(*casInput) // nil?
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

type intSet struct {
	slc []int
}

// restartFullHelper was extracted and
// generalized from czar.go. What it does:
//
// a) close old session, if any in *pSess.
// b) find the leader using HelperFindLeader(), repeatedly if need be.
// c) create a new session with the leader.
//
// the only errors are when we are shutting down
// from ctx or halt; if restartFullHelper
// returns an error, the test might be done.
func restartFullHelper(ctx context.Context, name string, cli *TubeNode, pSess **Session, halt *idem.Halter) (err error) {

fullRestart:
	for {
		select {
		case <-halt.ReqStop.Chan:
			vv("%v: halt requested (at restartFull top). exiting.", name)
			return ErrShutDown
		case <-ctx.Done():
			vv("%v: ctx Done requested (at restartFull top). exiting.", name)
			return ctx.Err()
		default:
		}

		if (*pSess) != nil {
			ctx2, canc := context.WithTimeout(ctx, time.Second*2)
			err = cli.CloseSession(ctx2, (*pSess))
			canc()
			if err != nil {
				vv("%v: closing prior session err='%v'", name, err)
			}
		}

		const requireOnlyContact = false

		for k := 0; ; k++ {
			//vv("%v: find leader loop k = %v", name, k)
			//vv("%v: cliCfg.Node2Addr = '%#v'", name, cliCfg.Node2Addr)
			leaderURL, leaderName, _, reallyLeader, _, err := cli.HelperFindLeader(ctx, &cli.cfg, "", requireOnlyContact, KEEP_CKT_ONLY_IF_LEADER) // KEEP_CKT_UP) // KEEP_CKT_ONLY_IF_LEADER)
			//vv("%v: helper said: leaderURL = '%v'; reallyLeader=%v; err='%v'", name, leaderURL, reallyLeader, err)
			panicOn(err)
			if !reallyLeader {
				vv("%v: arg. we see not really leader? why?", name)
				cli.closeAutoClientSockets()
				continue fullRestart
			}
			// should have updated our notion of leader, else on leader change we can be stuck
			// see peerListReplyHandler() tube.go:13234
			insp := cli.Inspect()
			if insp.CurrentLeaderName != "" &&
				insp.CurrentLeaderName != leaderName {
				// seen! not sure what to do...
				//panicf("why was insp.CurrentLeaderName(%v) != leaderName(%v) back from helper?", insp.CurrentLeaderName, leaderName)
			}

			ctx5, canc := context.WithTimeout(ctx, time.Second*5)

			(*pSess), _, err = cli.CreateNewSession(ctx5, leaderName, leaderURL)
			canc()
			//panicOn(err) // panic: hmm. no leader known to me (node 'node_0')
			if err == nil {
				vv("%v: got sess = '%v'", name, *pSess)
				return nil
			}
			alwaysPrintf("%v: got err from CreateNewSession, sleep 1 sec and try again: '%v'", name, err)
			time.Sleep(time.Second)
		}
	} // end for ever
}
