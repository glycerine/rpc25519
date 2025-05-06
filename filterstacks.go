package rpc25519

// optional debug utility:
// intercept SIGQUIT and skip showing gc stacks

import (
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"
)

var sigQuitCh chan os.Signal

func init() {
	//return // comment to turn on this debugging.
	sigQuitCh = make(chan os.Signal, 1)
	signal.Notify(sigQuitCh, syscall.SIGQUIT)
	go func() {
		for range sigQuitCh {
			// Allocate buffer for stack trace
			buf := make([]byte, 1<<20)
			for {
				n := runtime.Stack(buf, true)
				if n < len(buf) {
					buf = buf[:n]
					break
				}
				buf = make([]byte, 2*len(buf))
			}

			// Filter out GC-related goroutines
			var filtered []string
			for _, stack := range strings.Split(string(buf), "\n\n") {
				if !strings.Contains(stack, "GC sweep wait") &&
					!strings.Contains(stack, "GC scavenge wait") &&
					!strings.Contains(stack, "runtime/mgcsweep.go") &&
					!strings.Contains(stack, "runtime/mgcscavenge.go") {
					filtered = append(filtered, stack)
				}
			}

			ts := "\n" + time.Now().In(gtz).Format("2006-01-02 15:04:05.999 -0700 MST")
			os.Stderr.Write([]byte(ts))
			os.Stderr.Write([]byte("SIGQUIT: quit after filtering.\nfilterstacks.go filtered stacks:\n\n"))
			os.Stderr.Write([]byte(strings.Join(filtered, "\n\n")))
			os.Exit(1)
		}
	}()
}
