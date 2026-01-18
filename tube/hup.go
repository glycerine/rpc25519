package tube

// optional debug utility:
// intercept SIGHUP and dump a memory profile

import (
	"fmt"
	"os"
	"os/signal"
	"runtime/pprof"
	"sort"
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
			if false { // skip mem profile dump for now
				rnd8 := cryrand.RandomStringWithUp(8)
				fn := path + ".memprof." + rnd8
				alwaysPrintf("got HUP, write mem profile to '%v'.", fn)
				writeMemProfiles(fn)
			}

			// sort and circuits to detect redundacy. this
			// is how we originally diagnosed the (now
			// fixed by circuit pruning) circuit+goroutine leaks.
			ckts := debugGlobalCkt.GetValSlice()

			// count by endpoint pairs
			m := make(map[string]int)
			m2 := make(map[string]int)
			for _, ckt := range ckts {
				endpoints := fmt.Sprintf("local: %v  remote: %v [RemotePeerID: %v]", ckt.LocalPeerName, ckt.RemotePeerName, ckt.RemotePeerID)
				n := m[endpoints]
				m[endpoints] = n + 1

				e2 := fmt.Sprintf("local: %v  remote: %v remotePeerName: %v", ckt.LocalPeerName, ckt.RemoteServiceName, ckt.RemotePeerName)
				n = m2[e2]
				m2[e2] = n + 1

			}
			alwaysPrintf("HUP: here are the %v active ckt, most recently made first:\n", len(ckts))
			alwaysPrintf("(and counted by endpoints):\n")
			for endp, count := range m {
				fmt.Printf("%v   -> count: %v\n", endp, count)
			}
			alwaysPrintf("\n(and counted by endpoints + remotePeerName):\n")
			for endp, count := range m2 {
				fmt.Printf("%v   -> count: %v\n", endp, count)
			}
			fmt.Println()
			sort.Sort(byCircuitSN(ckts))
			for i, ckt := range ckts {
				fmt.Printf("[%02d] %v\n", i, ckt)
			}

			//fmt.Printf("allstacks = \n%v\n", allstacks())
		}
	}()
}

type byCircuitSN []*rpc.Circuit

func (lo byCircuitSN) Len() int { return len(lo) }
func (lo byCircuitSN) Less(i, j int) bool {
	return lo[i].CircuitSN > lo[j].CircuitSN // most recent first
}
func (lo byCircuitSN) Swap(i, j int) {
	lo[i], lo[j] = lo[j], lo[i]
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
