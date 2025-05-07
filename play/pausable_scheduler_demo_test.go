package main

// The proof of concept demonstrates
// how to pause a test simulation that is
// running inside a synctest bubble.
//
// We use the fact that any goroutine in
// the bubble is blocked on a select with a
// channel from "outside" the bubble, then:
//
// a) that goroutine is not durably blocked; and
//
// b) the scheduler will not return from synctest.Wait(); and
//
// c) by forcing the node/worker goroutine in (a) to block
// waiting for input from the blocked scheduler in (b),
// then it will block eventually too.
//
// run with
// GOTRACEBACK=all GOEXPERIMENT=synctest go test -v -run pausable pausable_scheduler_demo_test.go

import (
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"fmt"
	"io"
	"os"
	"path"
	"runtime"

	"4d63.com/tz"
)

// since outside any bubble, the selects on these will
// pause the bubble's scheduler goro from exiting synctest.Wait().
// The workers will not be paused, but will cycle until
// they run through their step budget and ask the
// scheduler for more steps.
var pauseSchedulerCh = make(chan struct{})
var pauseMut sync.Mutex

// must be buffered. Size is the max number of steps a scheduler
// is allowed to run before going to single stepping.
var schedAllowedCh = make(chan bool, 1000)

// cause scheduler to not return from synctest.Wait(),
// pausing the simulation.
func pauseScheduler() {
	pauseMut.Lock()
	defer pauseMut.Unlock()
	pauseSchedulerCh = make(chan struct{})

	vv(" ------ paused scheduler ---- len schedAllowedCh = %v", len(schedAllowedCh))
}
func resumeScheduler() {
	pauseMut.Lock()
	defer pauseMut.Unlock()
	// release any worker selects that are blocked...
	close(pauseSchedulerCh)
	vv(" ++++++++++++ about to resume scheduler ++++++")
	pauseSchedulerCh = nil

	// give it one step at a time after the initial batch.
	if len(schedAllowedCh) == 0 {
		schedAllowedCh <- true
	}
}

func getPauseChan() chan struct{} {
	pauseMut.Lock()
	defer pauseMut.Unlock()
	return pauseSchedulerCh
}

func Test_pausable_simulation_demo_using_outside_of_bubble_chan(t *testing.T) {

	go func() {
		// if desired, let scheduler run for a bunch first...
		for range 10 {
			schedAllowedCh <- true
		}
		resumeScheduler()
		for {
			time.Sleep(time.Millisecond)
			if len(schedAllowedCh) > 0 {
				continue
			}
			pauseScheduler()
			time.Sleep(time.Second * 3)
			resumeScheduler()
		}
	}()

	synctest.Run(func() {

		step := time.Second
		consumerCanTakeSteps := make(chan time.Duration)
		producerCanTakeSteps := make(chan time.Duration)

		factory := make(chan time.Time)
		shutdown := make(chan struct{})
		set := time.Second
		_ = set
		minSleep := time.Nanosecond
		_ = minSleep

		// produce
		go func() {
			var nStep time.Duration
			for {
				// get our step budget
				vv("producer asking for a step budget")
				select {
				case nStep = <-producerCanTakeSteps:
					vv("producer got budget nStep = %v", nStep)
				case <-shutdown:
					return
				case <-getPauseChan():
				}
				// do that many steps
				for ; nStep > 0; nStep -= step {
					made := time.Now()
					select {
					case factory <- made:
						vv("producer factory <- made = %v", made)
					case <-shutdown:
						return
					case <-getPauseChan():
					}
				}
			}
		}()

		// consume
		go func() {
			var nStep time.Duration
			for {
				// get our step budget
				vv("consumer asking for a step budget")
				select {
				case nStep = <-consumerCanTakeSteps:
					vv("consumer got budget nStep = %v", nStep)
				case <-shutdown:
					return
				case <-getPauseChan():
				}
				// do that many steps
				for ; nStep > 0; nStep -= step {
					select {
					case tm := <-factory:
						vv("the consumer got factory -> tm=%v", tm)
					case <-shutdown:
						return
					case <-getPauseChan():
					}
				}
			}
		}()

		// why schedAllowedCh?
		// This works without it, but the scheduler runs through
		// two time steps, time.Sleep() calls, before pausing.
		// By adding schedAllowedCh, we can more tightly
		// control the simulation.
		for j := 0; ; j++ {

			var nProd time.Duration = 3
			var nCons time.Duration = 3
			vv("j=%v scheduler giving nProd = %v  and nCons = %v", j, nProd, nCons)
			consumerCanTakeSteps <- step * nCons
			producerCanTakeSteps <- step * nProd

			vv("scheduler about to synctest.Wait(); len schedAllowedCh = %v", len(schedAllowedCh))

			//runtime.Gosched() // try to let the pauser pause us if desired.

			<-schedAllowedCh

			// let all the goro get blocked
			synctest.Wait()

			vv("scheduler about to time.Sleep(step=%v)", step)

			time.Sleep(step) // hmm... scheduler never wakes from this sleep after the first pause.

			vv("j=%v, scheduler past synctest.Wait()", j)
		}

		close(shutdown)
	})
}

// embed vprint.go for a complete single file (can ignore)

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
