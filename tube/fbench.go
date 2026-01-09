package tube

import (
	"fmt"
	"io/ioutil"
	"os"
	"time"
)

// DetermineOptimalFsync determines the optimal fsync frequency for maximum bandwidth.
// It returns the optimal fsyncs per second and the buffer size used to achieve it.
func DetermineOptimalFsync() (float64, int) {
	fmt.Println("Benchmarking fsync bandwidth...")

	// Create a temporary file for benchmarking
	f, err := ioutil.TempFile(".", "fsync_bench_")
	if err != nil {
		fmt.Printf("Error creating temp file: %v\n", err)
		return 0, 0
	}
	defer os.Remove(f.Name())
	defer f.Close()

	// Ensure the file is on disk
	f.Sync()

	bufferSizes := []int{
		//4 * 1024,          // 4KB
		//16 * 1024,         // 16KB
		//64 * 1024,         // 64KB
		256 * 1024,         // 256KB
		1 * 1024 * 1024,    // 1MB
		4 * 1024 * 1024,    // 4MB
		16 * 1024 * 1024,   // 16MB
		32 * 1024 * 1024,   // 32MB
		64 * 1024 * 1024,   // 64MB
		128 * 1024 * 1024,  // 128MB
		256 * 1024 * 1024,  // 256MB
		512 * 1024 * 1024,  // 512MB
		1024 * 1024 * 1024, // 1GB
	}

	var maxBandwidth float64
	var optimalFsyncsPerSec float64
	var optimalBufSize int

	fmt.Printf("%-15s %-15s %-15s\n", "Buffer Size", "Bandwidth", "Fsyncs/Sec")
	fmt.Println("------------------------------------------------")

	for _, size := range bufferSizes {
		duration := 2 * time.Second
		start := time.Now()
		bytesWritten := 0
		fsyncCount := 0

		buf := make([]byte, size)

		// Run for a fixed duration
		for time.Since(start) < duration {
			n, err := f.Write(buf)
			if err != nil {
				fmt.Printf("Write error: %v\n", err)
				break
			}
			bytesWritten += n

			err = f.Sync()
			if err != nil {
				fmt.Printf("Sync error: %v\n", err)
				break
			}
			fsyncCount++
		}

		elapsed := time.Since(start).Seconds()
		bandwidth := float64(bytesWritten) / 1024 / 1024 / elapsed // MB/s
		fsyncsPerSec := float64(fsyncCount) / elapsed

		fmt.Printf("%-15d %-10.2f MB/s   %-10.2f\n", size, bandwidth, fsyncsPerSec)

		// Simple logic: user asked for frequency with max bandwidth.
		// Usually larger buffers = higher bandwidth = lower fsync frequency.
		// But we'll track the max bandwidth we observe.
		if bandwidth > maxBandwidth {
			maxBandwidth = bandwidth
			optimalFsyncsPerSec = fsyncsPerSec
			optimalBufSize = size
		}

		// Reset file position to avoid growing too large if that's a concern,
		// but sequential write is more realistic for Raft logs.
		// However, f.Sync() forces data out.
		// If we truncate, we might change performance characteristics.
		// Let's just keep writing. The temp file is deleted at the end.
	}

	fmt.Println("------------------------------------------------")
	return optimalFsyncsPerSec, optimalBufSize
}

/* mac book pro SSD:

GOEXPERIMENT=synctest go test -v -count=1 -run=TestFsyncBenchmark
=== RUN   TestFsyncBenchmark
Benchmarking fsync bandwidth...
Buffer Size     Bandwidth       Fsyncs/Sec
------------------------------------------------
262144          11.16      MB/s   44.64
1048576         39.86      MB/s   39.86
4194304         138.15     MB/s   34.54
16777216        420.76     MB/s   26.30
33554432        715.55     MB/s   22.36
67108864        1084.57    MB/s   16.95
134217728       1553.25    MB/s   12.13
256 MB          1985.94    MB/s   7.76  <<< resonable approx of peak
536870912       2100.41    MB/s   4.10
1073741824      2136.01    MB/s   2.09  <<<<<<<< peak
------------------------------------------------

Optimal Fsync Rate: 2.09 fsyncs/sec at Buffer Size: 1073741824 bytes
--- PASS: TestFsyncBenchmark (21.13s)

older linux box, rog.
Western Digital Black NVME drive SN850 2TB

go test -v -run Fsync
=== RUN   TestFsyncBenchmark
Benchmarking fsync bandwidth...
Buffer Size     Bandwidth       Fsyncs/Sec
------------------------------------------------
262144          288.65     MB/s   1154.59
1048576         498.92     MB/s   498.92
4194304         641.31     MB/s   160.33
16777216        839.17     MB/s   52.45
33554432        911.76     MB/s   28.49
67108864        950.55     MB/s   14.85
128 MB          953.49     MB/s   7.45  <<<<<<<< peak
268435456       850.95     MB/s   3.32
536870912       883.37     MB/s   1.73
1073741824      794.11     MB/s   0.78
------------------------------------------------

Optimal Fsync Rate: 7.45 fsyncs/sec at Buffer Size: 134217728 bytes
--- PASS: TestFsyncBenchmark (23.95s)

*/
