package main

import (
	"context"
	"fmt"
	mathrand2 "math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

var _ = fmt.Printf

// Given a minimum election timeout T, the Raft
// leader election timeouts must be uniformly
// random within (T, 2*T] for the dissertation
// analysis of Chapter 9 to apply. Since
// Ongaro shows this gives fast leader elections,
// we undertake to meet his pre-requisite condition.
func randomElectionTimeoutDur(T time.Duration) time.Duration {
	// compute T + a random fraction of T
	draw := mathrand2.N(T) // in [0, T)
	draw++                 // make sure it is positive, in (0, T]
	//INVAR: draw > 0
	frac := float64(draw%1e8) / 1e8
	r := time.Duration(float64(T) * frac)
	dur := T + r // somewhere in (T, 2T]
	return dur
}

var barrierMut sync.Mutex
var waitCond *sync.Cond = sync.NewCond(&barrierMut)
var barrierUpTime time.Time
var barrierDownTime time.Time
var numWaitingAtBarrier atomic.Int64
var barrierUp atomic.Bool

func waitInBubble() {
	synctest.Wait()
}

func waitInBubble2() {

	barrierMut.Lock()
	if barrierUpTime.After(barrierDownTime) && barrierUpTime == time.Now() {
		waitCond.Wait() // unlock barrierMut and suspend until Broadcast
	} else {
		// we are first
		barrierUpTime = time.Now()
		vv("first to arrive set barrierUpTime = '%v' and about to synctest.Wait()", barrierUpTime)
		synctest.Wait()
		barrierDownTime = time.Now()
		vv("after synctest.Wait, first to arrive set barrierDownTime = '%v' and about to synctest.Wait()", barrierDownTime)

		waitCond.Broadcast()
		barrierMut.Unlock()
	}
}

func waitUntilBarrierDown(who string) (alwaysNilChan chan struct{}) {
	barrierMut.Lock()
	isUp := barrierDownTime.Before(barrierUpTime)
	if isUp {
		// barrier   up when: downTime < upTime
		// barrier down when: downTime >= upTime
		tot := numWaitingAtBarrier.Add(1)
		vv("%v goro about to block, total blocked: %v", who, tot)
		waitCond.Wait() // unlock barrierMut and suspend until Broadcast
		return
	}
	vv("barrier was down, %v checked", who)
	barrierMut.Unlock()
	return // always returns a nil channel
}

// we want that after raising the barrier,
// all worker goro are either blocked on a select or
// inside the barrier.
func raiseBarrier() {
	barrierMut.Lock()
	barrierUpTime = time.Now()
	if barrierDownTime.Equal(barrierUpTime) {
		time.Sleep(time.Nanosecond)
		barrierUpTime = time.Now()
	}
	// INVAR: barrierUpTime > barrierDownTime
	vv("barrierUpTime = %v", barrierUpTime)
	barrierUp.Store(true)
	//synctest.Wait() // maybe?
	barrierMut.Unlock()
}
func releaseBarrier() {
	barrierMut.Lock()
	barrierDownTime = time.Now()

	vv("num waiting = %v", numWaitingAtBarrier.Load())

	//vv("after synctest.Wait, first to arrive set barrierDownTime = '%v' and about to synctest.Wait()", barrierDownTime)

	numWaitingAtBarrier.Store(0) // avoid race by clearing before unlock.

	waitCond.Broadcast()
	barrierMut.Unlock()
}

func Test_Are_We_Last(t *testing.T) {
	synctest.Run(func() {
		//for range 100_000 {

		//var rwmut sync.RWMutex
		//vv("got lock? %v", rwmut.TryLock())
		shutdown := make(chan struct{})

		for range 1 {
			tick := time.Second
			N := 3
			awake := make(chan time.Time)
			for i := range N {
				me := "producer"
				go func(i int) {
					// producer workers.
					for {
						//dur := randomElectionTimeoutDur(T)
						time.Sleep(tick)
						select {
						case awake <- time.Now():
							vv("producer %v produced", i)
						case <-shutdown:
						case <-waitUntilBarrierDown(me):
						}
					}
				}(i)
			}

			// service some reads on awake chan, to verify this gets blocked too
			go func() {
				// consumer worker.
				me := "consumer"
				for {
					//dur := randomElectionTimeoutDur(T)
					time.Sleep(tick)
					select {
					case <-awake:
						vv("consumed")
					case <-shutdown:
					case <-waitUntilBarrierDown(me):
					}
				}
			}()

			// let other goro go for a bit, 10 ticks worth of work.
			time.Sleep(tick * 10)

			vv("about to raise barrier, num waiting %v", numWaitingAtBarrier.Load())
			raiseBarrier()
			// let all the goro get blocked

			vv("after raising barrier")

			//time.Sleep(tick)

			// usually what we see:
			// one worker will be randomly be blocked on the select with
			// the nil channel and now partners. The other
			// three will be inside the barrier.
			vv("num waiting = %v, vs producers N = %v +1 consumer = %v", numWaitingAtBarrier.Load(), N, N+1)

			vv("scheduler about to synctest.Wait")
			synctest.Wait()

			vv("num waiting after synctest.Wait= %v", numWaitingAtBarrier.Load())

			vv("1) ideally, scheduler has time frozen, can make atomic state changes")

			vv("2) ideally, scheduler has time frozen, can make atomic state changes")

			vv("3) ideally, scheduler has time frozen, can make atomic state changes")

			vv("scheduler about to advance time by one tick, which might fire alarms")
			time.Sleep(tick)

			vv("scheduler after one tick")

			// if we deadlock, and do not see any consumer/producer
			// prints, this is good. meaning: no other goro made progress
			// nor could run... let's even do a bunch of ticks...verify
			// that we see no consumer/producer prints, but that
			// the clock goes forward.

			time.Sleep(tick * 60)

			vv("scheduler after 60 ticks")

			select {}

			releaseBarrier()
			vv("after releaseBarrier")

			vv("num waiting after synctest.Wait + Sleep = %v", numWaitingAtBarrier.Load())

			// let other goro go for a bit
			time.Sleep(tick)

			vv("about to raise barrier, num waiting %v", numWaitingAtBarrier.Load())
			raiseBarrier()

			vv("scheduler about to synctest.Wait")
			synctest.Wait()

			//vv("I think we're alone now. There doesn't seem to be anyone about...")

			//time.Sleep(tick)

			vv("scheduler's last print") // seen, good

			// without the raiseBarrier,
			// does not panic, because the workers are active.
			select {}

			/*
				//panic("look at the stack trace") // did not tell us if blocked.

				// If only I am awake, then the receive should default:
				select {

				// Ah, but this channel receive will also wake them up again! we need a lock... or something. mutexes are excluded from bubble analysis...
				case when := <-awake:

					panic(fmt.Sprintf("no other goro should have been awake! awake='%v'; now='%v'", when, time.Now()))
				default:
					//vv("good: nobody sent to us, as they were asleep")
				}
				vv("about to Unlock") // not seen?!?
				//rwmut.Unlock()        // let all other goro into select...
				close(shutdown)
				vv("past shutdown")
			*/
		}
	})
}

/*
	barrierMut.Lock()
	if barrierUpTime.After(barrierDownTime) && barrierUpTime == time.Now() {
		waitCond.Wait() // unlock barrierMut and suspend until Broadcast
	} else {
		// we are first
		barrierUpTime = time.Now()
		vv("first to arrive set barrierUpTime = '%v' and about to synctest.Wait()", barrierUpTime)
		synctest.Wait()
		barrierDownTime = time.Now()
		vv("after synctest.Wait, first to arrive set barrierDownTime = '%v' and about to synctest.Wait()", barrierDownTime)

		waitCond.Broadcast()
		barrierMut.Unlock()
	}
*/
/*
-*- mode: compilation; default-directory: "~/trash/blah/" -*-
Compilation started at Wed May  7 02:25:26

GOTRACEBACK=all GOEXPERIMENT=synctest go test -v -run Are_We_Last
=== RUN   Test_Are_We_Last
panic: nobody other goro should have been awake!

goroutine 7 [running, synctest group 6]:
blah.Test_Are_We_Last.func1()
	/Users/jaten/trash/blah/ctx_test.go:46 +0x125
created by internal/synctest.Run in goroutine 6
	/usr/local/go/src/runtime/synctest.go:178 +0x10d

goroutine 1 [chan receive]:
testing.(*T).Run(0xc0000036c0, {0x2f23a32?, 0xc000111b30?}, 0x3014510)
	/usr/local/go/src/testing/testing.go:1859 +0x431
testing.runTests.func1(0xc0000036c0)
	/usr/local/go/src/testing/testing.go:2279 +0x37
testing.tRunner(0xc0000036c0, 0xc000111c70)
	/usr/local/go/src/testing/testing.go:1792 +0xf4
testing.runTests(0xc00000e018, {0x3105f40, 0x2, 0x2}, {0x310f800?, 0x7?, 0x310e680?})
	/usr/local/go/src/testing/testing.go:2277 +0x4b4
testing.(*M).Run(0xc00007c280)
	/usr/local/go/src/testing/testing.go:2142 +0x64a
main.main()
	_testmain.go:47 +0x9b

goroutine 6 [synctest.Run, synctest group 6]:
internal/synctest.Run(0x30145a0)
	/usr/local/go/src/runtime/synctest.go:191 +0x18c
testing/synctest.Run(...)
	/usr/local/go/src/testing/synctest/synctest.go:38
blah.Test_Are_We_Last(0xc000003880?)
	/Users/jaten/trash/blah/ctx_test.go:29 +0x1b
testing.tRunner(0xc000003880, 0x3014510)
	/usr/local/go/src/testing/testing.go:1792 +0xf4
created by testing.(*T).Run in goroutine 1
	/usr/local/go/src/testing/testing.go:1851 +0x413

goroutine 8 [chan send (synctest), synctest group 6]:
blah.Test_Are_We_Last.func1.1(0x0)
	/Users/jaten/trash/blah/ctx_test.go:38 +0x34
created by blah.Test_Are_We_Last.func1 in goroutine 7
	/Users/jaten/trash/blah/ctx_test.go:35 +0x45

goroutine 9 [chan send (synctest), synctest group 6]:
blah.Test_Are_We_Last.func1.1(0x1)
	/Users/jaten/trash/blah/ctx_test.go:38 +0x34
created by blah.Test_Are_We_Last.func1 in goroutine 7
	/Users/jaten/trash/blah/ctx_test.go:35 +0x45

// ... same for 1000 goroutines....

goroutine 1006 [chan send (synctest), synctest group 6]:
blah.Test_Are_We_Last.func1.1(0x3e6)
	/Users/jaten/trash/blah/ctx_test.go:38 +0x34
created by blah.Test_Are_We_Last.func1 in goroutine 7
	/Users/jaten/trash/blah/ctx_test.go:35 +0x45

goroutine 1007 [chan send (synctest), synctest group 6]:
blah.Test_Are_We_Last.func1.1(0x3e7)
	/Users/jaten/trash/blah/ctx_test.go:38 +0x34
created by blah.Test_Are_We_Last.func1 in goroutine 7
	/Users/jaten/trash/blah/ctx_test.go:35 +0x45
exit status 2
FAIL	blah	6.735s

Compilation exited abnormally with code 1 at Wed May  7 02:25:33

*/

func TestWithTimeout(t *testing.T) {
	synctest.Run(func() {
		const timeout = 5 * time.Second
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		vv("before sleep 4.999999999 seconds")
		// Wait just less than the timeout.
		time.Sleep(timeout - time.Nanosecond)
		vv("after sleep 4.999999999")
		//synctest.Wait()
		vv("after Wait")

		if err := ctx.Err(); err != nil {
			t.Fatalf("before timeout, ctx.Err() = %v; want nil", err)
		}

		synctest.Wait()

		// Wait the rest of the way until the timeout.
		vv("before sleep 1ns")
		time.Sleep(time.Nanosecond)
		vv("after sleep 1ns")
		synctest.Wait()
		vv("after 2nd Wait")
		if err := ctx.Err(); err != context.DeadlineExceeded {
			t.Fatalf("after timeout, ctx.Err() = %v; want DeadlineExceeded", err)
		}
	})
}

/*
	// Does time ONLY advance when all goro are blocked?
	// It can it advance in some other manner? Is
	// this guaranteed?
	//
	// Let's assume for a moment that time can ONLY advance when
	// all goro are blocked? The adjective durably
	// was not applied to the blog post statement,
	// "Time advances in the bubble when all goroutines are blocked."
	// so let's further assume that he meant _durably_ blocked.
	//
	// jea: since we were blocked, other goro can
	// run until blocked in the background; in
	// fact, time won't advance until they are
	// blocked. So once we get to here, we
	// know:
	// 1) all other goroutines were blocked
	// until a nanosecond before now.
	// 2) We are now awake, but also any
	// other goro that is due to wake at
	// this moment is also now awake as well.

	// Still under those assumptions, now ask:
	// What does the synctest.Wait() do for us?
	synctest.Wait()
	// okay, we now know that all _other_ goroutines
	// that woke up when we did just now
	// have also finished their business and are
	// now durably blocked. We are the only
	// one that has any more business to do
	// at this time point. So this is useful in
	// that we can now presume to "go last" in
	// this nanosecond, with the assurance that
	// we won't miss a send we were supposed
	// to get at this point in time. During
	// this nanosecond, the order of who does
	// what is undefined, and lots of other
	// goro could be doing stuff before the
	// synctest.Wait(). But, now, after the
	// synctest.Wait, we know they are all done,
	// and we are the only one who will
	// operate until the next time we sleep.
	// In effect, we have frozen time just
	// at the end of this nanosecond. We
	// can adjust our queues, match sends and reads,
	// set new timers, process expired timers,
	// change node state to partitioned, etc,
	// with the full knowledge that we are
	// not operating concurrently with anyone
	// else, but just after all of them have
	// done any business they are going to
	// do at the top of this nanosecond.
	// We are now "at the bottom" of the nanosecond.

	// What if we did the wait, then slept?
	// After the wait, we also know that
	// everyone else is durably blocked.
	// So in fact, we don't need to sleep
	// ourselves to know that nobody else
	// will be doing anything concurrently.
	// It is guaranteed that we are the last,
	// no matter what time it is. In fact,
	// we might want to have other goroutines
	// do stuff for us now, respond to us,
	// and take any actions they are going
	// to take, get blocked on timers or
	// reads or sends. They can do so, operating
	// independently and concurrently with us,
	// until the next sleep or synctest.Wait(),
	// *IF* we want to be operating with them
	// concurrently (MUCH more non-deterministic!)
	// then this is fine. BUT if we want the
	// determinism guarantee that we are
	// "going last" and have seen everyone
	// else's poker hand before placing our bets,
	// then we will want to do the Sleep then
	// Wait approach.
*/
