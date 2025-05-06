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

type inBubbleState struct {
	barrierMut sync.Mutex
	waitCond   *sync.Cond

	barrierUp           chan struct{}
	numWaitingAtBarrier atomic.Int64

	step time.Duration

	// logical clocks
	tm              int64
	barrierUpTime   int64
	barrierDownTime int64

	// N = 1 seems to correctly deadlock
	// N = 2 hangs, time.Sleep() does not return. Why?
	// The synctest.Wait does not get to run...
	// My guess is that it is not handling the cond.Wait properly? just a guess.
	N int64 // number of producers

	clocks []int64 // logical clocks for consumer and producers
}

func newInBubbleState() (s *inBubbleState) {
	var N int64 = 2 // number of producers
	s = &inBubbleState{
		step:      time.Second,
		N:         N,
		barrierUp: make(chan struct{}),
		clocks:    make([]int64, N+1), // logical clocks for consumer and producers
	}
	s.waitCond = sync.NewCond(&s.barrierMut)
	return s
}

func (s *inBubbleState) highestClock() (maxLC int64) {
	for _, lc := range s.clocks {
		maxLC = max(maxLC, lc)
	}
	return
}
func (s *inBubbleState) lowestClock() (minLC int64) {
	minLC = 1 << 62
	for _, lc := range s.clocks {
		minLC = min(minLC, lc)
	}
	return
}
func (s *inBubbleState) advanceLC() {
	for i := range s.clocks {
		s.clocks[i] = s.tm + 1
	}
}

func (s *inBubbleState) waitUntilBarrierDown(who string, i int64) {
	s.barrierMut.Lock()
	tot := s.numWaitingAtBarrier.Add(1)
	vv("%v goro about to block, total blocked: %v", who, tot)
	s.waitCond.Wait() // some may be block here, fine, good. should let synctest.Wait proceed.
	// we have the lock!
	tot = s.numWaitingAtBarrier.Add(-1)
	vv("%v awoke! tot waiting now %v", who, tot)
	s.barrierMut.Unlock()
}

func (s *inBubbleState) raiseBarrier() {
	s.barrierMut.Lock()

	// closed channel means barrier up
	ch := make(chan struct{})
	close(ch)
	//initTimeBarrierUpNotDurablyBlocking = ch
	s.barrierUp = ch

	s.barrierUpTime = s.tm + 1
	vv("barrierUpTime = %v", s.barrierUpTime)
	s.barrierMut.Unlock()
}

func (s *inBubbleState) releaseBarrier(moreTime int64) {
	s.barrierMut.Lock()
	s.barrierDownTime = s.tm + 1

	nwait := s.numWaitingAtBarrier.Load()
	vv("num waiting at barrier = %v", nwait)

	//initTimeBarrierUpNotDurablyBlocking = nil
	s.barrierUp = nil
	s.advanceLC()
	s.tm += moreTime
	s.waitCond.Broadcast()
	vv("after waitCond.Broadcast()")
	s.barrierMut.Unlock()
}

var initTimeBarrierUpNotDurablyBlocking = make(chan struct{})

func Test_preferred_use_of_synctest_Wait(t *testing.T) {

	synctest.Run(func() {

		s := newInBubbleState()

		shutdown := make(chan struct{})

		awake := make(chan time.Time)
		for i := range s.N {
			//me := "producer"
			go func(i int64) {
				// producer workers.
				for {
					time.Sleep(s.step)
					//s.clocks[i]++
					//if s.clocks[i] >= s.tm {
					//	s.waitUntilBarrierDown(me, i)
					//	vv("producer %v released from BEFORE the select", i)
					//}

					select { // synctest.Wait should proceed, unless initTimeBarrierUpNotDurablyBlocking is here...
					case awake <- time.Now():
						vv("producer %v produced, lc = %v", i, s.clocks[i])
					case <-shutdown:
						//case <-s.barrierUp:
						//	// NO! :) //case <-initTimeBarrierUpNotDurablyBlocking:
						//	s.waitUntilBarrierDown(me, i)
						//	vv("producer %v released from barrier in select", i)
					}
				}
			}(i)
		}

		// service some reads on awake chan, to verify this gets blocked too
		go func() {
			// consumer worker.
			//me := "consumer"
			for {
				time.Sleep(s.step)
				s.clocks[s.N]++
				//if s.clocks[s.N] >= s.tm {
				//	s.waitUntilBarrierDown(me, s.N)
				//}
				select {
				case <-awake:
					vv("the consumer consumed, lc = %v", s.clocks[s.N])
				case <-shutdown:
					//case <-s.barrierUp:
					//	// NO! case <-initTimeBarrierUpNotDurablyBlocking:
					//	s.waitUntilBarrierDown(me, s.N)
				}
			}
		}()

		// let other goro go for a bit
		time.Sleep(s.step)
		vv("scheduler about to synctest.Wait()") //
		// let all the goro get blocked
		synctest.Wait()

		vv("1) ideally, scheduler has time frozen, can make atomic state changes")

		vv("2) ideally, scheduler has time frozen, can make atomic state changes")

		vv("3) ideally, scheduler has time frozen, can make atomic state changes")

		vv("scheduler about to advance time by 10 ticks, which might fire alarms")
		time.Sleep(s.step * 3)

		vv("scheduler after 3 ticks")

		time.Sleep(s.step * 10)

		vv("scheduler after 10 more ticks")

		vv("scheduler's last print")

		close(shutdown)

	})
}

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
