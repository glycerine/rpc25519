package tube

import (
	"fmt"
	"testing"
)

func TestFsyncBenchmark(t *testing.T) {
	return
	if testing.Short() {
		t.Skip("skipping fsync benchmark in short mode")
	}

	rate, size := DetermineOptimalFsync()
	fmt.Printf("\nOptimal Fsync Rate: %.2f fsyncs/sec at Buffer Size: %d bytes\n", rate, size)
}
