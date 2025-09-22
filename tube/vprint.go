package tube

import (
	"fmt"
	"io"
	"os"
	"path"
	"reflect"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"unicode"
	//"sync"
	"time"

	"4d63.com/tz"
	rpc "github.com/glycerine/rpc25519"
)

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
	//if faketime {
	gtz = utcTz
	//}
}

const rfc3339MsecTz0 = "2006-01-02T15:04:05.000Z07:00"
const rfc3339NanoNumericTZ0pad = "2006-01-02T15:04:05.000000000-07:00"
const rfc3339NanoTz0 = "2006-01-02 15:04:05.000000000Z07:00"

var myPid = os.Getpid()
var showPid bool
var showGoID bool = true

func nice(tm time.Time) string {
	return tm.Format(rfc3339MsecTz0)
}
func nice9(tm time.Time) string {
	return tm.Format(rfc3339NanoTz0)
}

func (s *TubeNode) ww(format string, a ...interface{}) {
	if s.name == "node_4" {
		tsPrintf(format, a...)
	}
}

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

func xx(format string, a ...interface{}) {
	//if !forceQuiet {
	tsPrintf(format, a...)
	//}
}

var selectDebugOn bool

// select statement case return printing
func vvsel(format string, a ...interface{}) {
	if selectDebugOn && !forceQuiet {
		tsPrintf(format, a...)
	}
}

func alwaysPrintf(format string, a ...interface{}) {
	tsPrintf(format, a...)
}

// use rpc.TsPrintfMut to prevent message interleaving in the log.
//var tsPrintfMut sync.Mutex

// time-stamped printf
func tsPrintf(format string, a ...interface{}) {
	rpc.TsPrintfMut.Lock()
	if showPid {
		printf("\n%s [pid %v] %s ", fileLine(3), myPid, ts())
	} else {
		if showGoID {
			printf("\n%s [goID %v] %s ", fileLine(3), GoroNumber(), ts())
		} else {
			printf("\n%s %s ", fileLine(3), ts())
		}
	}
	printf(format+"\n", a...)
	rpc.TsPrintfMut.Unlock()
}

// get timestamp for logging purposes
func ts() string {
	//return time.Now().In(gtz).Format("2006-01-02 15:04:05.999 -0700 MST")
	return time.Now().In(gtz).Format(rfc3339NanoNumericTZ0pad)
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
	//vv("derived goro of %v from '%v'", n, string(buf))
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

func equalIgnoringSpaces(a, b string) bool {
	a1 := strings.ReplaceAll(a, " ", "")
	b1 := strings.ReplaceAll(b, " ", "")
	return a1 == b1
}

func hasWhiteSpace(name string) bool {
	for _, r := range name {
		if unicode.IsSpace(r) {
			return true
		}
	}
	return false
}

func panicf(format string, a ...interface{}) {
	panic(fmt.Sprintf(format, a...))
}
