package main

import (
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"fmt"
	"io"
	"os"
	"path"
	"reflect"
	"runtime"
	"runtime/debug"
	"strconv"

	"4d63.com/tz"
)

var barrierMut sync.Mutex
var waitCond *sync.Cond = sync.NewCond(&barrierMut)

var numWaitingAtBarrier atomic.Int64
var barrierUp = make(chan struct{})

// logical clocks
var tm int64
var barrierUpTime int64
var barrierDownTime int64

// N = 1 seems to correctly deadlock
// N = 2 hangs, time.Sleep() does not return. Why?
// The synctest.Wait does not get to run...
// My guess is that it is not handling the cond.Wait properly? just a guess.
var N int64 = 2 // number of producers

var clocks = make([]int64, N+1) // logical clocks for consumer and producers

func highestClock() (maxLC int64) {
	for _, lc := range clocks {
		maxLC = max(maxLC, lc)
	}
	return
}
func lowestClock() (minLC int64) {
	minLC = 1 << 62
	for _, lc := range clocks {
		minLC = min(minLC, lc)
	}
	return
}
func advanceLC() {
	for i := range clocks {
		clocks[i] = tm + 1
	}
}

func waitUntilBarrierDown(who string, i int64) {
	barrierMut.Lock()
	tot := numWaitingAtBarrier.Add(1)
	vv("%v goro about to block, total blocked: %v", who, tot)
	waitCond.Wait() // some may be block here, fine, good. should let synctest.Wait proceed.
	// we have the lock!
	tot = numWaitingAtBarrier.Add(-1)
	vv("%v awoke! tot waiting now %v", who, tot)
	barrierMut.Unlock()
}

func raiseBarrier() {
	barrierMut.Lock()

	// closed channel means barrier up
	ch := make(chan struct{})
	close(ch)
	barrierUp = ch

	barrierUpTime = tm + 1
	vv("barrierUpTime = %v", barrierUpTime)
	barrierMut.Unlock()
}

func releaseBarrier(moreTime int64) {
	barrierMut.Lock()
	barrierDownTime = tm + 1

	nwait := numWaitingAtBarrier.Load()
	vv("num waiting at barrier = %v", nwait)

	barrierUp = nil
	advanceLC()
	tm += moreTime
	waitCond.Broadcast()
	vv("after waitCond.Broadcast()")
	barrierMut.Unlock()
}

func Test_why_does_synctest_block_forever_in_synctest_Wait_hmm(t *testing.T) {

	synctest.Run(func() {

		shutdown := make(chan struct{})

		awake := make(chan time.Time)
		for i := range N {
			me := "producer"
			go func(i int64) {
				// producer workers.
				for {
					//time.Sleep(tick)
					clocks[i]++
					if clocks[i] >= tm {
						waitUntilBarrierDown(me, i)
					}

					select { // some may be blocked here, fine, good. synctest.Wait should proceed.
					case awake <- time.Now():
						vv("producer %v produced, lc = %v", i, clocks[i])
					case <-shutdown:
					case <-barrierUp:
						waitUntilBarrierDown(me, i)
						//vv("producer %v saw gBarrierDown closed", i)
					}
				}
			}(i)
		}

		// service some reads on awake chan, to verify this gets blocked too
		go func() {
			// consumer worker.
			me := "consumer"
			for {
				clocks[N]++
				if clocks[N] >= tm {
					waitUntilBarrierDown(me, N)
				}
				select {
				case <-awake:
					vv("the consumer consumed, lc = %v", clocks[N])
				case <-shutdown:
				case <-barrierUp:
					waitUntilBarrierDown(me, N)
				}
			}
		}()

		// let other goro go for a bit
		tm += 2
		vv("tm now %v", tm)

		minLC := lowestClock()
		if minLC < tm {
			vv("minLC = %v <  tm = %v", minLC, tm)
			//time.Sleep(10 * time.Millisecond) // hung here???
		}
		// not seen:
		vv("scheduler about to synctest.Wait()")

		// let all the goro get blocked
		synctest.Wait() // 1 here, is never released. Why?

		vv("about to raise barrier, num waiting %v", numWaitingAtBarrier.Load())
		raiseBarrier()

		vv("after raising barrier")

		vv("num waiting = %v, vs producers N = %v +1 consumer = %v", numWaitingAtBarrier.Load(), N, N+1)

		vv("num waiting after synctest.Wait= %v", numWaitingAtBarrier.Load())

		vv("1) ideally, scheduler has time frozen, can make atomic state changes")

		vv("2) ideally, scheduler has time frozen, can make atomic state changes")

		vv("3) ideally, scheduler has time frozen, can make atomic state changes")

		vv("scheduler about to advance time by 10 ticks, which might fire alarms")
		tm += 10

		vv("scheduler after 10 ticks") // seen

		tm += 60

		vv("scheduler after 60 ticks")

		releaseBarrier(60)

		vv("after releaseBarrier")
		vv("num waiting after synctest.Wait + Sleep = %v", numWaitingAtBarrier.Load())

		tm += 10

		vv("2nd time, about to raise barrier, num waiting %v", numWaitingAtBarrier.Load())

		raiseBarrier()

		vv("raised the barrier")

		vv("scheduler's last print")

		releaseBarrier(60)
		close(shutdown)

	})
}

/*
Scenario A) with time.Sleep(10 * time.Millisecond) uncommented:

-*- mode: compilation; default-directory: "~/rpc25519/play/" -*-
Compilation started at Wed May  7 07:33:28

GOTRACEBACK=all GOEXPERIMENT=synctest go test -v -run why_does
=== RUN   Test_why_does_synctest_block_forever_in_synctest_Wait_hmm

buggy_test.go:137 2000-01-01 00:00:00 +0000 UTC tm now 2

buggy_test.go:141 2000-01-01 00:00:00 +0000 UTC minLC = 0 <  tm = 2

buggy_test.go:127 2000-01-01 00:00:00 +0000 UTC the consumer consumed, lc = 1

buggy_test.go:51 2000-01-01 00:00:00 +0000 UTC consumer goro about to block, total blocked: 1

buggy_test.go:106 2000-01-01 00:00:00 +0000 UTC producer 0 produced, lc = 1

buggy_test.go:51 2000-01-01 00:00:00 +0000 UTC producer goro about to block, total blocked: 2
SIGQUIT: quit
PC=0x7ff80f8cc5d6 m=0 sigcode=0

goroutine 0 gp=0x60b99a0 m=0 mp=0x60ba880 [idle]:
runtime.pthread_cond_wait(0x60badd8, 0x60bad98)
	/usr/local/go/src/runtime/sys_darwin.go:547 +0x2e fp=0x7ff7bb1849d0 sp=0x7ff7bb1849a8 pc=0x5dd5b8e
runtime.semasleep(0xffffffffffffffff)
	/usr/local/go/src/runtime/os_darwin.go:72 +0xae fp=0x7ff7bb184a28 sp=0x7ff7bb1849d0 pc=0x5db1aee
runtime.notesleep(0x60ba9c0)
	/usr/local/go/src/runtime/lock_sema.go:62 +0x7d fp=0x7ff7bb184a58 sp=0x7ff7bb184a28 pc=0x5d8c01d
runtime.mPark(...)
	/usr/local/go/src/runtime/proc.go:1887
runtime.stopm()
	/usr/local/go/src/runtime/proc.go:2907 +0x8c fp=0x7ff7bb184a88 sp=0x7ff7bb184a58 pc=0x5dbc32c
runtime.findRunnable()
	/usr/local/go/src/runtime/proc.go:3644 +0xdcf fp=0x7ff7bb184c00 sp=0x7ff7bb184a88 pc=0x5dbde2f
runtime.schedule()
	/usr/local/go/src/runtime/proc.go:4017 +0xb1 fp=0x7ff7bb184c38 sp=0x7ff7bb184c00 pc=0x5dbef31
runtime.park_m(0xc000003dc0)
	/usr/local/go/src/runtime/proc.go:4141 +0x285 fp=0x7ff7bb184c98 sp=0x7ff7bb184c38 pc=0x5dbf3e5
runtime.mcall()
	/usr/local/go/src/runtime/asm_amd64.s:459 +0x4e fp=0x7ff7bb184cb0 sp=0x7ff7bb184c98 pc=0x5df090e

goroutine 1 gp=0xc000002380 m=nil [chan receive]:
runtime.gopark(0x3000000be7598fe?, 0x3000000061250d0?, 0xd0?, 0x50?, 0x611c108?)
	/usr/local/go/src/runtime/proc.go:435 +0xce fp=0xc000111998 sp=0xc000111978 pc=0x5dea76e
runtime.chanrecv(0xc00007a310, 0xc000111a7f, 0x1)
	/usr/local/go/src/runtime/chan.go:664 +0x445 fp=0xc000111a10 sp=0xc000111998 pc=0x5d86e05
runtime.chanrecv1(0x60b95c0?, 0x5fc3320?)
	/usr/local/go/src/runtime/chan.go:506 +0x12 fp=0xc000111a38 sp=0xc000111a10 pc=0x5d869b2
testing.(*T).Run(0xc0000036c0, {0x5edaf2c?, 0xc000111b30?}, 0x5fbf348)
	/usr/local/go/src/testing/testing.go:1859 +0x431 fp=0xc000111b10 sp=0xc000111a38 pc=0x5e50ff1
testing.runTests.func1(0xc0000036c0)
	/usr/local/go/src/testing/testing.go:2279 +0x37 fp=0xc000111b50 sp=0xc000111b10 pc=0x5e532f7
testing.tRunner(0xc0000036c0, 0xc000111c70)
	/usr/local/go/src/testing/testing.go:1792 +0xf4 fp=0xc000111ba0 sp=0xc000111b50 pc=0x5e500d4
testing.runTests(0xc00000e018, {0x60afd70, 0x1, 0x1}, {0x60ba880?, 0x7?, 0x60b9700?})
	/usr/local/go/src/testing/testing.go:2277 +0x4b4 fp=0xc000111ca0 sp=0xc000111ba0 pc=0x5e531d4
testing.(*M).Run(0xc00007c280)
	/usr/local/go/src/testing/testing.go:2142 +0x64a fp=0xc000111ed0 sp=0xc000111ca0 pc=0x5e51b8a
main.main()
	_testmain.go:45 +0x9b fp=0xc000111f50 sp=0xc000111ed0 pc=0x5eb55fb
runtime.main()
	/usr/local/go/src/runtime/proc.go:283 +0x28b fp=0xc000111fe0 sp=0xc000111f50 pc=0x5db770b
runtime.goexit({})
	/usr/local/go/src/runtime/asm_amd64.s:1700 +0x1 fp=0xc000111fe8 sp=0xc000111fe0 pc=0x5df27e1

goroutine 2 gp=0xc0000028c0 m=nil [force gc (idle)]:
runtime.gopark(0x0?, 0x0?, 0x0?, 0x0?, 0x0?)
	/usr/local/go/src/runtime/proc.go:435 +0xce fp=0xc000052fa8 sp=0xc000052f88 pc=0x5dea76e
runtime.goparkunlock(...)
	/usr/local/go/src/runtime/proc.go:441
runtime.forcegchelper()
	/usr/local/go/src/runtime/proc.go:348 +0xb3 fp=0xc000052fe0 sp=0xc000052fa8 pc=0x5db7a53
runtime.goexit({})
	/usr/local/go/src/runtime/asm_amd64.s:1700 +0x1 fp=0xc000052fe8 sp=0xc000052fe0 pc=0x5df27e1
created by runtime.init.7 in goroutine 1
	/usr/local/go/src/runtime/proc.go:336 +0x1a

goroutine 3 gp=0xc000002e00 m=nil [GC sweep wait]:
runtime.gopark(0x0?, 0x0?, 0x0?, 0x0?, 0x0?)
	/usr/local/go/src/runtime/proc.go:435 +0xce fp=0xc000053780 sp=0xc000053760 pc=0x5dea76e
runtime.goparkunlock(...)
	/usr/local/go/src/runtime/proc.go:441
runtime.bgsweep(0xc00006a000)
	/usr/local/go/src/runtime/mgcsweep.go:276 +0x94 fp=0xc0000537c8 sp=0xc000053780 pc=0x5da0e74
runtime.gcenable.gowrap1()
	/usr/local/go/src/runtime/mgc.go:204 +0x25 fp=0xc0000537e0 sp=0xc0000537c8 pc=0x5d95385
runtime.goexit({})
	/usr/local/go/src/runtime/asm_amd64.s:1700 +0x1 fp=0xc0000537e8 sp=0xc0000537e0 pc=0x5df27e1
created by runtime.gcenable in goroutine 1
	/usr/local/go/src/runtime/mgc.go:204 +0x66

goroutine 4 gp=0xc000002fc0 m=nil [GC scavenge wait]:
runtime.gopark(0xc00006a000?, 0x5f091d0?, 0x1?, 0x0?, 0xc000002fc0?)
	/usr/local/go/src/runtime/proc.go:435 +0xce fp=0xc000053f78 sp=0xc000053f58 pc=0x5dea76e
runtime.goparkunlock(...)
	/usr/local/go/src/runtime/proc.go:441
runtime.(*scavengerState).park(0x60b9780)
	/usr/local/go/src/runtime/mgcscavenge.go:425 +0x49 fp=0xc000053fa8 sp=0xc000053f78 pc=0x5d9e909
runtime.bgscavenge(0xc00006a000)
	/usr/local/go/src/runtime/mgcscavenge.go:653 +0x3c fp=0xc000053fc8 sp=0xc000053fa8 pc=0x5d9ee7c
runtime.gcenable.gowrap2()
	/usr/local/go/src/runtime/mgc.go:205 +0x25 fp=0xc000053fe0 sp=0xc000053fc8 pc=0x5d95325
runtime.goexit({})
	/usr/local/go/src/runtime/asm_amd64.s:1700 +0x1 fp=0xc000053fe8 sp=0xc000053fe0 pc=0x5df27e1
created by runtime.gcenable in goroutine 1
	/usr/local/go/src/runtime/mgc.go:205 +0xa5

goroutine 5 gp=0xc000003500 m=nil [finalizer wait]:
runtime.gopark(0x60da340?, 0x490013?, 0x78?, 0x26?, 0x5d8d99e?)
	/usr/local/go/src/runtime/proc.go:435 +0xce fp=0xc000052630 sp=0xc000052610 pc=0x5dea76e
runtime.runfinq()
	/usr/local/go/src/runtime/mfinal.go:196 +0x107 fp=0xc0000527e0 sp=0xc000052630 pc=0x5d94347
runtime.goexit({})
	/usr/local/go/src/runtime/asm_amd64.s:1700 +0x1 fp=0xc0000527e8 sp=0xc0000527e0 pc=0x5df27e1
created by runtime.createfing in goroutine 1
	/usr/local/go/src/runtime/mfinal.go:166 +0x3d

goroutine 6 gp=0xc000003a40 m=nil [synctest.Run, synctest group 6]:
runtime.gopark(0xc000054730?, 0x5dd5739?, 0x1a?, 0x8c?, 0x24?)
	/usr/local/go/src/runtime/proc.go:435 +0xce fp=0xc0000546e0 sp=0xc0000546c0 pc=0x5dea76e
internal/synctest.Run(0x5fbf3d8)
	/usr/local/go/src/runtime/synctest.go:191 +0x18c fp=0xc000054758 sp=0xc0000546e0 pc=0x5dee28c
testing/synctest.Run(...)
	/usr/local/go/src/testing/synctest/synctest.go:38
github.com/glycerine/rpc25519/play.Test_why_does_synctest_block_forever_in_synctest_Wait_hmm(0xc000003880?)
	/Users/jaten/rpc25519/play/buggy_test.go:88 +0x1b fp=0xc000054770 sp=0xc000054758 pc=0x5eb49bb
testing.tRunner(0xc000003880, 0x5fbf348)
	/usr/local/go/src/testing/testing.go:1792 +0xf4 fp=0xc0000547c0 sp=0xc000054770 pc=0x5e500d4
testing.(*T).Run.gowrap1()
	/usr/local/go/src/testing/testing.go:1851 +0x25 fp=0xc0000547e0 sp=0xc0000547c0 pc=0x5e51165
runtime.goexit({})
	/usr/local/go/src/runtime/asm_amd64.s:1700 +0x1 fp=0xc0000547e8 sp=0xc0000547e0 pc=0x5df27e1
created by testing.(*T).Run in goroutine 1
	/usr/local/go/src/testing/testing.go:1851 +0x413

goroutine 7 gp=0xc000003c00 m=nil [sleep, synctest group 6]:
runtime.gopark(0xc00006e240?, 0xd234ccf52db9680?, 0x0?, 0x0?, 0x0?)
	/usr/local/go/src/runtime/proc.go:435 +0xce fp=0xc000104e88 sp=0xc000104e68 pc=0x5dea76e
time.Sleep(0x989680)
	/usr/local/go/src/runtime/time.go:336 +0x145 fp=0xc000104ee0 sp=0xc000104e88 pc=0x5deed65
github.com/glycerine/rpc25519/play.Test_why_does_synctest_block_forever_in_synctest_Wait_hmm.func1()
	/Users/jaten/rpc25519/play/buggy_test.go:142 +0x2b5 fp=0xc000104fe0 sp=0xc000104ee0 pc=0x5eb4c95
runtime.goexit({})
	/usr/local/go/src/runtime/asm_amd64.s:1700 +0x1 fp=0xc000104fe8 sp=0xc000104fe0 pc=0x5df27e1
created by internal/synctest.Run in goroutine 6
	/usr/local/go/src/runtime/synctest.go:178 +0x10d

goroutine 8 gp=0xc000003dc0 m=nil [sync.Cond.Wait, synctest group 6]:
runtime.gopark(0x611c108?, 0x1d?, 0x10?, 0x0?, 0xc00001a220?)
	/usr/local/go/src/runtime/proc.go:435 +0xce fp=0xc000100de8 sp=0xc000100dc8 pc=0x5dea76e
runtime.goparkunlock(...)
	/usr/local/go/src/runtime/proc.go:441
sync.runtime_notifyListWait(0x60b11b0, 0x1)
	/usr/local/go/src/runtime/sema.go:597 +0x159 fp=0xc000100e38 sp=0xc000100de8 pc=0x5debdd9
sync.(*Cond).Wait(0x5ed8c28?)
	/usr/local/go/src/sync/cond.go:71 +0x85 fp=0xc000100e70 sp=0xc000100e38 pc=0x5df6e45
github.com/glycerine/rpc25519/play.waitUntilBarrierDown({0x5eccb07, 0x8}, 0xc000100f98?)
	/Users/jaten/rpc25519/play/buggy_test.go:52 +0xf1 fp=0xc000100ef0 sp=0xc000100e70 pc=0x5eb4611
github.com/glycerine/rpc25519/play.Test_why_does_synctest_block_forever_in_synctest_Wait_hmm.func1.1(0x0)
	/Users/jaten/rpc25519/play/buggy_test.go:101 +0xb4 fp=0xc000100fc8 sp=0xc000100ef0 pc=0x5eb5374
github.com/glycerine/rpc25519/play.Test_why_does_synctest_block_forever_in_synctest_Wait_hmm.func1.gowrap1()
	/Users/jaten/rpc25519/play/buggy_test.go:113 +0x24 fp=0xc000100fe0 sp=0xc000100fc8 pc=0x5eb5284
runtime.goexit({})
	/usr/local/go/src/runtime/asm_amd64.s:1700 +0x1 fp=0xc000100fe8 sp=0xc000100fe0 pc=0x5df27e1
created by github.com/glycerine/rpc25519/play.Test_why_does_synctest_block_forever_in_synctest_Wait_hmm.func1 in goroutine 7
	/Users/jaten/rpc25519/play/buggy_test.go:95 +0x65

goroutine 9 gp=0xc000230000 m=nil [select, synctest group 6]:
runtime.gopark(0xc000055f50?, 0x3?, 0x0?, 0x0?, 0xc000055f2a?)
	/usr/local/go/src/runtime/proc.go:435 +0xce fp=0xc000055db8 sp=0xc000055d98 pc=0x5dea76e
runtime.selectgo(0xc000055f50, 0xc000055f24, 0x0?, 0x1, 0x0?, 0x1)
	/usr/local/go/src/runtime/select.go:351 +0x837 fp=0xc000055ef0 sp=0xc000055db8 pc=0x5dc9dd7
github.com/glycerine/rpc25519/play.Test_why_does_synctest_block_forever_in_synctest_Wait_hmm.func1.1(0x1)
	/Users/jaten/rpc25519/play/buggy_test.go:104 +0x137 fp=0xc000055fc8 sp=0xc000055ef0 pc=0x5eb53f7
github.com/glycerine/rpc25519/play.Test_why_does_synctest_block_forever_in_synctest_Wait_hmm.func1.gowrap1()
	/Users/jaten/rpc25519/play/buggy_test.go:113 +0x24 fp=0xc000055fe0 sp=0xc000055fc8 pc=0x5eb5284
runtime.goexit({})
	/usr/local/go/src/runtime/asm_amd64.s:1700 +0x1 fp=0xc000055fe8 sp=0xc000055fe0 pc=0x5df27e1
created by github.com/glycerine/rpc25519/play.Test_why_does_synctest_block_forever_in_synctest_Wait_hmm.func1 in goroutine 7
	/Users/jaten/rpc25519/play/buggy_test.go:95 +0x65

goroutine 10 gp=0xc0002301c0 m=nil [sync.Cond.Wait, synctest group 6]:
runtime.gopark(0x611c108?, 0x1d?, 0x10?, 0x0?, 0xc00001a1e0?)
	/usr/local/go/src/runtime/proc.go:435 +0xce fp=0xc000105e38 sp=0xc000105e18 pc=0x5dea76e
runtime.goparkunlock(...)
	/usr/local/go/src/runtime/proc.go:441
sync.runtime_notifyListWait(0x60b11b0, 0x0)
	/usr/local/go/src/runtime/sema.go:597 +0x159 fp=0xc000105e88 sp=0xc000105e38 pc=0x5debdd9
sync.(*Cond).Wait(0x5ed8c28?)
	/usr/local/go/src/sync/cond.go:71 +0x85 fp=0xc000105ec0 sp=0xc000105e88 pc=0x5df6e45
github.com/glycerine/rpc25519/play.waitUntilBarrierDown({0x5eccb0f, 0x8}, 0xc000105fc0?)
	/Users/jaten/rpc25519/play/buggy_test.go:52 +0xf1 fp=0xc000105f40 sp=0xc000105ec0 pc=0x5eb4611
github.com/glycerine/rpc25519/play.Test_why_does_synctest_block_forever_in_synctest_Wait_hmm.func1.2()
	/Users/jaten/rpc25519/play/buggy_test.go:123 +0x9d fp=0xc000105fe0 sp=0xc000105f40 pc=0x5eb513d
runtime.goexit({})
	/usr/local/go/src/runtime/asm_amd64.s:1700 +0x1 fp=0xc000105fe8 sp=0xc000105fe0 pc=0x5df27e1
created by github.com/glycerine/rpc25519/play.Test_why_does_synctest_block_forever_in_synctest_Wait_hmm.func1 in goroutine 7
	/Users/jaten/rpc25519/play/buggy_test.go:117 +0x188

rax    0x104
rbx    0x16
rcx    0x7ff7bb184898
rdx    0x200
rdi    0x60badd8
rsi    0x20100000300
rbp    0x7ff7bb184930
rsp    0x7ff7bb184898
r8     0x0
r9     0xa0
r10    0x0
r11    0x246
r12    0x0
r13    0x20100000300
r14    0x7ff852ee1e80
r15    0x200
rip    0x7ff80f8cc5d6
rflags 0x247
cs     0x7
fs     0x0
gs     0x0
exit status 2
FAIL	github.com/glycerine/rpc25519/play	8.634s

Compilation exited abnormally with code 1 at Wed May  7 07:33:37

*/

/*
Scenario B: // comment out time.Sleep(10 * time.Millisecond)
             Why doesn't the synctest.Wait() return?

-*- mode: compilation; default-directory: "~/rpc25519/play/" -*-
Compilation started at Wed May  7 07:37:47

GOTRACEBACK=all GOEXPERIMENT=synctest go test -v -run why_does
=== RUN   Test_why_does_synctest_block_forever_in_synctest_Wait_hmm

buggy_test.go:139 2000-01-01 00:00:00 +0000 UTC tm now 2

buggy_test.go:143 2000-01-01 00:00:00 +0000 UTC minLC = 0 <  tm = 2

buggy_test.go:147 2000-01-01 00:00:00 +0000 UTC scheduler about to synctest.Wait()

buggy_test.go:129 2000-01-01 00:00:00 +0000 UTC the consumer consumed, lc = 1

buggy_test.go:52 2000-01-01 00:00:00 +0000 UTC consumer goro about to block, total blocked: 1

buggy_test.go:108 2000-01-01 00:00:00 +0000 UTC producer 0 produced, lc = 1

buggy_test.go:52 2000-01-01 00:00:00 +0000 UTC producer goro about to block, total blocked: 2
SIGQUIT: quit
PC=0x7ff80f8cc5d6 m=0 sigcode=0

goroutine 0 gp=0x9b249a0 m=0 mp=0x9b25880 [idle]:
runtime.pthread_cond_wait(0x9b25dd8, 0x9b25d98)
	/usr/local/go/src/runtime/sys_darwin.go:547 +0x2e fp=0x7ff7b77199d0 sp=0x7ff7b77199a8 pc=0x9840b8e
runtime.semasleep(0xffffffffffffffff)
	/usr/local/go/src/runtime/os_darwin.go:72 +0xae fp=0x7ff7b7719a28 sp=0x7ff7b77199d0 pc=0x981caee
runtime.notesleep(0x9b259c0)
	/usr/local/go/src/runtime/lock_sema.go:62 +0x7d fp=0x7ff7b7719a58 sp=0x7ff7b7719a28 pc=0x97f701d
runtime.mPark(...)
	/usr/local/go/src/runtime/proc.go:1887
runtime.stopm()
	/usr/local/go/src/runtime/proc.go:2907 +0x8c fp=0x7ff7b7719a88 sp=0x7ff7b7719a58 pc=0x982732c
runtime.findRunnable()
	/usr/local/go/src/runtime/proc.go:3644 +0xdcf fp=0x7ff7b7719c00 sp=0x7ff7b7719a88 pc=0x9828e2f
runtime.schedule()
	/usr/local/go/src/runtime/proc.go:4017 +0xb1 fp=0x7ff7b7719c38 sp=0x7ff7b7719c00 pc=0x9829f31
runtime.park_m(0xc000003dc0)
	/usr/local/go/src/runtime/proc.go:4141 +0x285 fp=0x7ff7b7719c98 sp=0x7ff7b7719c38 pc=0x982a3e5
runtime.mcall()
	/usr/local/go/src/runtime/asm_amd64.s:459 +0x4e fp=0x7ff7b7719cb0 sp=0x7ff7b7719c98 pc=0x985b90e

goroutine 1 gp=0xc000002380 m=nil [chan receive]:
runtime.gopark(0x300000006401a23?, 0x300000009b900d0?, 0xd0?, 0x0?, 0x9b87108?)
	/usr/local/go/src/runtime/proc.go:435 +0xce fp=0xc000111998 sp=0xc000111978 pc=0x985576e
runtime.chanrecv(0xc00007a310, 0xc000111a7f, 0x1)
	/usr/local/go/src/runtime/chan.go:664 +0x445 fp=0xc000111a10 sp=0xc000111998 pc=0x97f1e05
runtime.chanrecv1(0x9b245c0?, 0x9a2e320?)
	/usr/local/go/src/runtime/chan.go:506 +0x12 fp=0xc000111a38 sp=0xc000111a10 pc=0x97f19b2
testing.(*T).Run(0xc0000036c0, {0x9945f0c?, 0xc000111b30?}, 0x9a2a348)
	/usr/local/go/src/testing/testing.go:1859 +0x431 fp=0xc000111b10 sp=0xc000111a38 pc=0x98bbff1
testing.runTests.func1(0xc0000036c0)
	/usr/local/go/src/testing/testing.go:2279 +0x37 fp=0xc000111b50 sp=0xc000111b10 pc=0x98be2f7
testing.tRunner(0xc0000036c0, 0xc000111c70)
	/usr/local/go/src/testing/testing.go:1792 +0xf4 fp=0xc000111ba0 sp=0xc000111b50 pc=0x98bb0d4
testing.runTests(0xc00000e018, {0x9b1ad70, 0x1, 0x1}, {0x9b25880?, 0x7?, 0x9b24700?})
	/usr/local/go/src/testing/testing.go:2277 +0x4b4 fp=0xc000111ca0 sp=0xc000111ba0 pc=0x98be1d4
testing.(*M).Run(0xc00007c1e0)
	/usr/local/go/src/testing/testing.go:2142 +0x64a fp=0xc000111ed0 sp=0xc000111ca0 pc=0x98bcb8a
main.main()
	_testmain.go:45 +0x9b fp=0xc000111f50 sp=0xc000111ed0 pc=0x99205db
runtime.main()
	/usr/local/go/src/runtime/proc.go:283 +0x28b fp=0xc000111fe0 sp=0xc000111f50 pc=0x982270b
runtime.goexit({})
	/usr/local/go/src/runtime/asm_amd64.s:1700 +0x1 fp=0xc000111fe8 sp=0xc000111fe0 pc=0x985d7e1

goroutine 2 gp=0xc0000028c0 m=nil [force gc (idle)]:
runtime.gopark(0x0?, 0x0?, 0x0?, 0x0?, 0x0?)
	/usr/local/go/src/runtime/proc.go:435 +0xce fp=0xc000052fa8 sp=0xc000052f88 pc=0x985576e
runtime.goparkunlock(...)
	/usr/local/go/src/runtime/proc.go:441
runtime.forcegchelper()
	/usr/local/go/src/runtime/proc.go:348 +0xb3 fp=0xc000052fe0 sp=0xc000052fa8 pc=0x9822a53
runtime.goexit({})
	/usr/local/go/src/runtime/asm_amd64.s:1700 +0x1 fp=0xc000052fe8 sp=0xc000052fe0 pc=0x985d7e1
created by runtime.init.7 in goroutine 1
	/usr/local/go/src/runtime/proc.go:336 +0x1a

goroutine 3 gp=0xc000002e00 m=nil [GC sweep wait]:
runtime.gopark(0x0?, 0x0?, 0x0?, 0x0?, 0x0?)
	/usr/local/go/src/runtime/proc.go:435 +0xce fp=0xc000053780 sp=0xc000053760 pc=0x985576e
runtime.goparkunlock(...)
	/usr/local/go/src/runtime/proc.go:441
runtime.bgsweep(0xc00006a000)
	/usr/local/go/src/runtime/mgcsweep.go:276 +0x94 fp=0xc0000537c8 sp=0xc000053780 pc=0x980be74
runtime.gcenable.gowrap1()
	/usr/local/go/src/runtime/mgc.go:204 +0x25 fp=0xc0000537e0 sp=0xc0000537c8 pc=0x9800385
runtime.goexit({})
	/usr/local/go/src/runtime/asm_amd64.s:1700 +0x1 fp=0xc0000537e8 sp=0xc0000537e0 pc=0x985d7e1
created by runtime.gcenable in goroutine 1
	/usr/local/go/src/runtime/mgc.go:204 +0x66

goroutine 4 gp=0xc000002fc0 m=nil [GC scavenge wait]:
runtime.gopark(0xc00006a000?, 0x99741b0?, 0x1?, 0x0?, 0xc000002fc0?)
	/usr/local/go/src/runtime/proc.go:435 +0xce fp=0xc000053f78 sp=0xc000053f58 pc=0x985576e
runtime.goparkunlock(...)
	/usr/local/go/src/runtime/proc.go:441
runtime.(*scavengerState).park(0x9b24780)
	/usr/local/go/src/runtime/mgcscavenge.go:425 +0x49 fp=0xc000053fa8 sp=0xc000053f78 pc=0x9809909
runtime.bgscavenge(0xc00006a000)
	/usr/local/go/src/runtime/mgcscavenge.go:653 +0x3c fp=0xc000053fc8 sp=0xc000053fa8 pc=0x9809e7c
runtime.gcenable.gowrap2()
	/usr/local/go/src/runtime/mgc.go:205 +0x25 fp=0xc000053fe0 sp=0xc000053fc8 pc=0x9800325
runtime.goexit({})
	/usr/local/go/src/runtime/asm_amd64.s:1700 +0x1 fp=0xc000053fe8 sp=0xc000053fe0 pc=0x985d7e1
created by runtime.gcenable in goroutine 1
	/usr/local/go/src/runtime/mgc.go:205 +0xa5

goroutine 5 gp=0xc000003500 m=nil [finalizer wait]:
runtime.gopark(0x9b45340?, 0x490013?, 0x78?, 0x26?, 0x97f899e?)
	/usr/local/go/src/runtime/proc.go:435 +0xce fp=0xc000052630 sp=0xc000052610 pc=0x985576e
runtime.runfinq()
	/usr/local/go/src/runtime/mfinal.go:196 +0x107 fp=0xc0000527e0 sp=0xc000052630 pc=0x97ff347
runtime.goexit({})
	/usr/local/go/src/runtime/asm_amd64.s:1700 +0x1 fp=0xc0000527e8 sp=0xc0000527e0 pc=0x985d7e1
created by runtime.createfing in goroutine 1
	/usr/local/go/src/runtime/mfinal.go:166 +0x3d

goroutine 6 gp=0xc000003a40 m=nil [synctest.Run, synctest group 6]:
runtime.gopark(0xc000054730?, 0x9840739?, 0x1a?, 0x3c?, 0x24?)
	/usr/local/go/src/runtime/proc.go:435 +0xce fp=0xc0000546e0 sp=0xc0000546c0 pc=0x985576e
internal/synctest.Run(0x9a2a3d8)
	/usr/local/go/src/runtime/synctest.go:191 +0x18c fp=0xc000054758 sp=0xc0000546e0 pc=0x985928c
testing/synctest.Run(...)
	/usr/local/go/src/testing/synctest/synctest.go:38
github.com/glycerine/rpc25519/play.Test_why_does_synctest_block_forever_in_synctest_Wait_hmm(0xc000003880?)
	/Users/jaten/rpc25519/play/buggy_test.go:90 +0x1b fp=0xc000054770 sp=0xc000054758 pc=0x991f9bb
testing.tRunner(0xc000003880, 0x9a2a348)
	/usr/local/go/src/testing/testing.go:1792 +0xf4 fp=0xc0000547c0 sp=0xc000054770 pc=0x98bb0d4
testing.(*T).Run.gowrap1()
	/usr/local/go/src/testing/testing.go:1851 +0x25 fp=0xc0000547e0 sp=0xc0000547c0 pc=0x98bc165
runtime.goexit({})
	/usr/local/go/src/runtime/asm_amd64.s:1700 +0x1 fp=0xc0000547e8 sp=0xc0000547e0 pc=0x985d7e1
created by testing.(*T).Run in goroutine 1
	/usr/local/go/src/testing/testing.go:1851 +0x413

goroutine 7 gp=0xc000003c00 m=nil [synctest.Wait, synctest group 6]:
runtime.gopark(0xc000104eb0?, 0x982c3c0?, 0xc0?, 0xe0?, 0xc000003c00?)
	/usr/local/go/src/runtime/proc.go:435 +0xce fp=0xc000104ea8 sp=0xc000104e88 pc=0x985576e
internal/synctest.Wait()
	/usr/local/go/src/runtime/synctest.go:247 +0x78 fp=0xc000104ee0 sp=0xc000104ea8 pc=0x9859438
testing/synctest.Wait(...)
	/usr/local/go/src/testing/synctest/synctest.go:66
github.com/glycerine/rpc25519/play.Test_why_does_synctest_block_forever_in_synctest_Wait_hmm.func1()
	/Users/jaten/rpc25519/play/buggy_test.go:150 +0x2da fp=0xc000104fe0 sp=0xc000104ee0 pc=0x991fcba
runtime.goexit({})
	/usr/local/go/src/runtime/asm_amd64.s:1700 +0x1 fp=0xc000104fe8 sp=0xc000104fe0 pc=0x985d7e1
created by internal/synctest.Run in goroutine 6
	/usr/local/go/src/runtime/synctest.go:178 +0x10d

goroutine 8 gp=0xc000003dc0 m=nil [sync.Cond.Wait, synctest group 6]:
runtime.gopark(0x9b87108?, 0x1d?, 0x10?, 0x0?, 0xc00001a240?)
	/usr/local/go/src/runtime/proc.go:435 +0xce fp=0xc000100de8 sp=0xc000100dc8 pc=0x985576e
runtime.goparkunlock(...)
	/usr/local/go/src/runtime/proc.go:441
sync.runtime_notifyListWait(0x9b1c1b0, 0x1)
	/usr/local/go/src/runtime/sema.go:597 +0x159 fp=0xc000100e38 sp=0xc000100de8 pc=0x9856dd9
sync.(*Cond).Wait(0x9943c08?)
	/usr/local/go/src/sync/cond.go:71 +0x85 fp=0xc000100e70 sp=0xc000100e38 pc=0x9861e45
github.com/glycerine/rpc25519/play.waitUntilBarrierDown({0x9937ae7, 0x8}, 0xc000100f98?)
	/Users/jaten/rpc25519/play/buggy_test.go:53 +0xf1 fp=0xc000100ef0 sp=0xc000100e70 pc=0x991f611
github.com/glycerine/rpc25519/play.Test_why_does_synctest_block_forever_in_synctest_Wait_hmm.func1.1(0x0)
	/Users/jaten/rpc25519/play/buggy_test.go:103 +0xb4 fp=0xc000100fc8 sp=0xc000100ef0 pc=0x9920354
github.com/glycerine/rpc25519/play.Test_why_does_synctest_block_forever_in_synctest_Wait_hmm.func1.gowrap1()
	/Users/jaten/rpc25519/play/buggy_test.go:115 +0x24 fp=0xc000100fe0 sp=0xc000100fc8 pc=0x9920264
runtime.goexit({})
	/usr/local/go/src/runtime/asm_amd64.s:1700 +0x1 fp=0xc000100fe8 sp=0xc000100fe0 pc=0x985d7e1
created by github.com/glycerine/rpc25519/play.Test_why_does_synctest_block_forever_in_synctest_Wait_hmm.func1 in goroutine 7
	/Users/jaten/rpc25519/play/buggy_test.go:97 +0x65

goroutine 9 gp=0xc000230000 m=nil [select, synctest group 6]:
runtime.gopark(0xc000055f50?, 0x3?, 0x0?, 0x0?, 0xc000055f2a?)
	/usr/local/go/src/runtime/proc.go:435 +0xce fp=0xc000292db8 sp=0xc000292d98 pc=0x985576e
runtime.selectgo(0xc000292f50, 0xc000055f24, 0x0?, 0x1, 0x0?, 0x1)
	/usr/local/go/src/runtime/select.go:351 +0x837 fp=0xc000292ef0 sp=0xc000292db8 pc=0x9834dd7
github.com/glycerine/rpc25519/play.Test_why_does_synctest_block_forever_in_synctest_Wait_hmm.func1.1(0x1)
	/Users/jaten/rpc25519/play/buggy_test.go:106 +0x137 fp=0xc000292fc8 sp=0xc000292ef0 pc=0x99203d7
github.com/glycerine/rpc25519/play.Test_why_does_synctest_block_forever_in_synctest_Wait_hmm.func1.gowrap1()
	/Users/jaten/rpc25519/play/buggy_test.go:115 +0x24 fp=0xc000292fe0 sp=0xc000292fc8 pc=0x9920264
runtime.goexit({})
	/usr/local/go/src/runtime/asm_amd64.s:1700 +0x1 fp=0xc000292fe8 sp=0xc000292fe0 pc=0x985d7e1
created by github.com/glycerine/rpc25519/play.Test_why_does_synctest_block_forever_in_synctest_Wait_hmm.func1 in goroutine 7
	/Users/jaten/rpc25519/play/buggy_test.go:97 +0x65

goroutine 10 gp=0xc0002301c0 m=nil [sync.Cond.Wait, synctest group 6]:
runtime.gopark(0x9b87108?, 0x1d?, 0x10?, 0x0?, 0xc00001a200?)
	/usr/local/go/src/runtime/proc.go:435 +0xce fp=0xc000105e38 sp=0xc000105e18 pc=0x985576e
runtime.goparkunlock(...)
	/usr/local/go/src/runtime/proc.go:441
sync.runtime_notifyListWait(0x9b1c1b0, 0x0)
	/usr/local/go/src/runtime/sema.go:597 +0x159 fp=0xc000105e88 sp=0xc000105e38 pc=0x9856dd9
sync.(*Cond).Wait(0x9943c08?)
	/usr/local/go/src/sync/cond.go:71 +0x85 fp=0xc000105ec0 sp=0xc000105e88 pc=0x9861e45
github.com/glycerine/rpc25519/play.waitUntilBarrierDown({0x9937aef, 0x8}, 0xc000105fc0?)
	/Users/jaten/rpc25519/play/buggy_test.go:53 +0xf1 fp=0xc000105f40 sp=0xc000105ec0 pc=0x991f611
github.com/glycerine/rpc25519/play.Test_why_does_synctest_block_forever_in_synctest_Wait_hmm.func1.2()
	/Users/jaten/rpc25519/play/buggy_test.go:125 +0x9d fp=0xc000105fe0 sp=0xc000105f40 pc=0x992011d
runtime.goexit({})
	/usr/local/go/src/runtime/asm_amd64.s:1700 +0x1 fp=0xc000105fe8 sp=0xc000105fe0 pc=0x985d7e1
created by github.com/glycerine/rpc25519/play.Test_why_does_synctest_block_forever_in_synctest_Wait_hmm.func1 in goroutine 7
	/Users/jaten/rpc25519/play/buggy_test.go:119 +0x188

rax    0x104
rbx    0x16
rcx    0x7ff7b7719898
rdx    0x200
rdi    0x9b25dd8
rsi    0x30100000400
rbp    0x7ff7b7719930
rsp    0x7ff7b7719898
r8     0x0
r9     0xa0
r10    0x0
r11    0x246
r12    0x0
r13    0x30100000400
r14    0x7ff852ee1e80
r15    0x200
rip    0x7ff80f8cc5d6
rflags 0x247
cs     0x7
fs     0x0
gs     0x0
exit status 2
FAIL	github.com/glycerine/rpc25519/play	46.858s

Compilation exited abnormally with code 1 at Wed May  7 07:38:34

*/

// embed vprint.go for a complete single file

// for tons of debug output
var verbose bool = false
var verboseVerbose bool = false

var gtz *time.Location
var chicago *time.Location
var utcTz *time.Location
var nyc *time.Location
var londonTz *time.Location
var frankfurt *time.Location

func init() {

	// do this is ~/.bashrc so we get the default.
	os.Setenv("TZ", "America/chicago")

	var err error
	chicago, err = tz.LoadLocation("America/Chicago")
	panicOn(err)
	utcTz, err = tz.LoadLocation("UTC")
	panicOn(err)
	nyc, err = tz.LoadLocation("America/New_York")
	panicOn(err)
	frankfurt, err = tz.LoadLocation("Europe/Berlin")
	panicOn(err)
	londonTz, err = tz.LoadLocation("Europe/London")
	panicOn(err)

	//gtz = chicago
	gtz = utcTz
}

const rfc3339MsecTz0 = "2006-01-02T15:04:05.000Z07:00"

var myPid = os.Getpid()
var showPid bool

func pp(format string, a ...interface{}) {
	if verboseVerbose {
		tsPrintf(format, a...)
	}
}

func zz(format string, a ...interface{}) {}

// useful during git bisect
var forceQuiet = false

func vv(format string, a ...interface{}) {
	if !forceQuiet {
		tsPrintf(format, a...)
	}
}

func alwaysPrintf(format string, a ...interface{}) {
	tsPrintf(format, a...)
}

var tsPrintfMut sync.Mutex

// time-stamped printf
func tsPrintf(format string, a ...interface{}) {
	tsPrintfMut.Lock()
	if showPid {
		printf("\n%s [pid %v] %s ", fileLine(3), myPid, ts())
	} else {
		printf("\n%s %s ", fileLine(3), ts())
	}
	printf(format+"\n", a...)
	tsPrintfMut.Unlock()
}

// get timestamp for logging purposes
func ts() string {
	return time.Now().In(gtz).Format("2006-01-02 15:04:05.999 -0700 MST")
	//return time.Now().In(nyc).Format("2006-01-02 15:04:05.999 -0700 MST")
}

// so we can multi write easily, use our own printf
var ourStdout io.Writer = os.Stdout

// Printf formats according to a format specifier and writes to standard output.
// It returns the number of bytes written and any write error encountered.
func printf(format string, a ...interface{}) (n int, err error) {
	return fmt.Fprintf(ourStdout, format, a...)
}

func fileLine(depth int) string {
	_, fileName, fileLine, ok := runtime.Caller(depth)
	var s string
	if ok {
		s = fmt.Sprintf("%s:%d", path.Base(fileName), fileLine)
	} else {
		s = ""
	}
	return s
}

func p(format string, a ...interface{}) {
	if verbose {
		tsPrintf(format, a...)
	}
}

func caller(upStack int) string {
	// elide ourself and runtime.Callers
	target := upStack + 2

	pc := make([]uintptr, target+2)
	n := runtime.Callers(0, pc)

	f := runtime.Frame{Function: "unknown"}
	if n > 0 {
		frames := runtime.CallersFrames(pc[:n])
		for i := 0; i <= target; i++ {
			contender, more := frames.Next()
			if i == target {
				f = contender
			}
			if !more {
				break
			}
		}
	}
	return f.Function
}

func panicOn(err error) {
	if err != nil {
		panic(err)
	}
}

// return stack dump for calling goroutine.
func stack() string {
	return string(debug.Stack())
}

// return stack dump for all goroutines
func allstacks() string {
	buf := make([]byte, 8192)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			return string(buf[:n])
		}
		buf = make([]byte, 2*len(buf))
	}
}

// IsNil uses reflect to to return true iff the face
// contains a nil pointer, map, array, slice, or channel.
func IsNil(face interface{}) bool {
	if face == nil {
		return true
	}
	switch reflect.TypeOf(face).Kind() {
	case reflect.Ptr, reflect.Array, reflect.Map, reflect.Slice, reflect.Chan:
		return reflect.ValueOf(face).IsNil()
	}
	return false
}

func thisStack() []byte {
	buf := make([]byte, 8192)
	nw := runtime.Stack(buf, false) // false => just us, no other goro.
	buf = buf[:nw]
	return buf
}

// GoroNumber returns the calling goroutine's number.
func GoroNumber() int {
	buf := make([]byte, 48)
	nw := runtime.Stack(buf, false) // false => just us, no other goro.
	buf = buf[:nw]

	// prefix "goroutine " is len 10.
	i := 10
	for buf[i] != ' ' && i < 30 {
		i++
	}
	n, err := strconv.Atoi(string(buf[10:i]))
	panicOn(err)
	return n
}

// abort the program with error code 1 after printing msg to Stderr.
func stop(msg interface{}) {
	switch e := msg.(type) {
	case error:
		fmt.Fprintf(os.Stderr, "%s: %s\n", fileLine(2), e.Error())
		os.Exit(1)
	default:
		fmt.Fprintf(os.Stderr, "%s: %v\n", fileLine(2), msg)
		os.Exit(1)
	}
}

func stopOn(err error) {
	if err == nil {
		return
	}
	fmt.Fprintf(os.Stderr, "%s: %v\n", fileLine(2), err.Error())
	os.Exit(1)
}
