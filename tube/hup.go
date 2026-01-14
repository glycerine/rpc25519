package tube

// optional debug utility:
// intercept SIGHUP and dump a memory profile

import (
	"os"
	"os/signal"
	"runtime/pprof"
	"strings"
	"syscall"

	"github.com/glycerine/cryrand"
	_ "net/http/pprof" // for web based profiling while running
)

var sigHupCh chan os.Signal

func init() {
	//return // comment to turn on this debugging.

	path := "debug"
	sigHupCh = make(chan os.Signal, 1)
	signal.Notify(sigQuitCh, syscall.SIGHUP)
	go func() {
		for range sigHupCh {
			rnd8 := cryrand.RandomStringWithUp(8)
			fn := path + ".memprof." + rnd8
			alwaysPrintf("got HUP, write mem profile to '%v'.", fn)
			writeMemProfiles(fn)
		}
	}()
}

func writeMemProfiles(fn string) {
	if !strings.HasSuffix(fn, ".") {
		fn += "."
	}
	h, err := os.Create(fn + "heap")
	panicOn(err)
	defer h.Close()
	a, err := os.Create(fn + "allocs")
	panicOn(err)
	defer a.Close()
	g, err := os.Create(fn + "goroutine")
	panicOn(err)
	defer g.Close()

	hp := pprof.Lookup("heap")
	ap := pprof.Lookup("allocs")
	gp := pprof.Lookup("goroutine")

	panicOn(hp.WriteTo(h, 1))
	panicOn(ap.WriteTo(a, 1))
	panicOn(gp.WriteTo(g, 2))
}
