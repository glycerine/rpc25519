package sparsified

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"path"
	"reflect"
	"regexp"
	"runtime"
	"runtime/debug"
	"strconv"
	"sync"
	"time"
)

// vprint: debugging facilities.

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
	chicago, err = time.LoadLocation("America/Chicago")
	panicOn(err)
	utcTz, err = time.LoadLocation("UTC")
	panicOn(err)
	nyc, err = time.LoadLocation("America/New_York")
	panicOn(err)
	frankfurt, err = time.LoadLocation("Europe/Berlin")
	panicOn(err)
	londonTz, err = time.LoadLocation("Europe/London")
	panicOn(err)

	gtz = chicago
}

const rfc3339MsecTz0 = "2006-01-02T15:04:05.000Z07:00"

var myPid = os.Getpid()
var showPid bool
var showGoro bool = true

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
	if showGoro {
		printf("\n%s [goID %v] %s ", fileLine(3), GoroNumber(), ts())
	} else if showPid {
		printf("\n%s [pid %v] %s ", fileLine(3), myPid, ts())
	} else {
		printf("\n%s %s ", fileLine(3), ts())
	}
	printf(format+"\n", a...)
	tsPrintfMut.Unlock()
}

// get timestamp for logging purposes
func ts() string {
	return time.Now().In(chicago).Format("2006-01-02 15:04:05.999 -0700 MST")
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

//func panicOn(err error) {
//	if err != nil {
//		panic(err)
//	}
//}

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

// isNil uses reflect to to return true iff the face
// contains a nil pointer, map, array, slice, or channel.
func isNil(face interface{}) bool {
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

/*func fileLine(depth int) string {
	_, fileName, fileLine, ok := runtime.Caller(depth)
	var s string
	if ok {
		s = fmt.Sprintf("%s:%d", path.Base(fileName), fileLine)
	} else {
		s = ""
	}
	return s
}
*/

// list live goroutines. not really tested; verify
// that it is complete before trusting it.
func getGoroutineIDs() map[int]bool {
	// Allocate a large enough buffer for the stack trace
	buf := make([]byte, 1024*1024)
	n := runtime.Stack(buf, true)

	// Create a scanner to read the stack trace line by line
	scanner := bufio.NewScanner(bytes.NewReader(buf[:n]))

	// Store goroutine IDs
	m := make(map[int]bool)

	// Regular expression to match "goroutine X [state]" lines
	re := regexp.MustCompile(`goroutine (\d+) \[`)

	for scanner.Scan() {
		line := scanner.Text()
		matches := re.FindStringSubmatch(line)
		if len(matches) > 1 {
			if id, err := strconv.Atoi(matches[1]); err == nil {
				m[id] = true
			}
		}
	}

	return m
}

func panicOn(err error) {
	if err != nil {
		panic(err)
	}
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
