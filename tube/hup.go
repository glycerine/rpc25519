package tube

// optional debug utility:
// intercept SIGHUP and dump a memory profile

import (
	"fmt"
	"os"
	"os/signal"
	"runtime/pprof"
	"strings"
	"syscall"

	"github.com/glycerine/cryrand"
	rpc "github.com/glycerine/rpc25519"
	_ "net/http/pprof" // for web based profiling while running
)

var debugGlobalCkt = rpc.NewMutexmap[string, *rpc.Circuit]()

var sigHupCh chan os.Signal

func init() {
	//return // comment to turn on this debugging.

	path := "debug"
	sigHupCh = make(chan os.Signal, 1)
	signal.Notify(sigHupCh, syscall.SIGHUP)
	go func() {
		for range sigHupCh {
			rnd8 := cryrand.RandomStringWithUp(8)
			fn := path + ".memprof." + rnd8
			alwaysPrintf("got HUP, write mem profile to '%v'.", fn)
			writeMemProfiles(fn)

			ckts := debugGlobalCkt.GetValSlice()
			alwaysPrintf("here are the %v active ckt:\n", len(ckts))
			for i, ckt := range ckts {
				fmt.Printf("[%02d] %v\n", i, ckt)
			}
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
