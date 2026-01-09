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
	if isDarwin {
		panicOn(actuallyFsyncOnDarwin(f))
	}

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
		duration := 10 * time.Second
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
			if isDarwin {
				panicOn(actuallyFsyncOnDarwin(f))
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

/* mac book pro SSD (are we sure this is fsyncing?? nope, it was not!!!)

Now with the added actuallyFsyncOnDarwin() call.

=== RUN   TestFsyncBenchmark
Benchmarking fsync bandwidth...
Buffer Size     Bandwidth       Fsyncs/Sec
------------------------------------------------
262144          10.16      MB/s   40.66
1048576         37.89      MB/s   37.89
4194304         133.17     MB/s   33.29
16777216        404.14     MB/s   25.26
33554432        661.25     MB/s   20.66
67108864        1011.61    MB/s   15.81
134217728       1447.85    MB/s   11.31
256MB           1910.00    MB/s   7.46  << might approximate peak
536870912       2081.12    MB/s   4.06
1073741824      2311.88    MB/s   2.26  << peak
------------------------------------------------

Optimal Fsync Rate: 2.26 fsyncs/sec at Buffer Size: 1073741824 bytes
--- PASS: TestFsyncBenchmark (101.02s)


Old (Bad) mac book pro, 10 seconds per setting, not really fsync-ing!

Buffer Size     Bandwidth       Fsyncs/Sec
------------------------------------------------
262144          11.49      MB/s   45.96
1048576         38.46      MB/s   38.46
4194304         140.57     MB/s   35.14
16777216        419.70     MB/s   26.23
33554432        676.85     MB/s   21.15
67108864        1048.61    MB/s   16.38
134217728       1477.86    MB/s   11.55
256 MB          2002.49    MB/s   7.82 << might approx peak
536870912       2164.22    MB/s   4.23
1073741824      2303.77    MB/s   2.25 << peak
------------------------------------------------



older linux box, rog.
Western Digital Black NVME drive SN850 2TB

go test -v -run Fsync
=== RUN   TestFsyncBenchmark  at 2 seconds
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

rog with 10 seconds per setting:

Buffer Size     Bandwidth       Fsyncs/Sec
------------------------------------------------
262144          289.09     MB/s   1156.35
1048576         498.81     MB/s   498.81
4194304         634.36     MB/s   158.59
16 MB           789.75     MB/s   49.36 << peak
33554432        693.71     MB/s   21.68
67108864        569.00     MB/s   8.89
134217728       617.10     MB/s   4.82
268435456       612.89     MB/s   2.39
536870912       733.50     MB/s   1.43
1073741824      786.52     MB/s   0.77
------------------------------------------------

aorus: hard drive? apparently smaller SD of same model

WD_BLACK SN850 1TB

=== RUN   TestFsyncBenchmark at 2 sec per setting
Benchmarking fsync bandwidth...
Buffer Size     Bandwidth       Fsyncs/Sec
------------------------------------------------
262144          0.25       MB/s   1.02
1048576         1.01       MB/s   1.01
4194304         2.73       MB/s   0.68
16777216        12.06      MB/s   0.75
33554432        21.01      MB/s   0.66
67108864        28.70      MB/s   0.45
134217728       1979.04    MB/s   15.46
268435456       2096.97    MB/s   8.19
536870912       2157.38    MB/s   4.21
1073741824      2022.83    MB/s   1.98
------------------------------------------------

Optimal Fsync Rate: 4.21 fsyncs/sec at Buffer Size: 536870912 bytes
--- PASS: TestFsyncBenchmark (25.22s)

Aorus at 10 second per setting (not understampling now):

Benchmarking fsync bandwidth...
Buffer Size     Bandwidth       Fsyncs/Sec
------------------------------------------------
262144          392.58     MB/s   1570.31
1048576         979.65     MB/s   979.65
4194304         1448.52    MB/s   362.13
16 MB           1918.62    MB/s   119.91 << peak
33554432        730.35     MB/s   22.82
67108864        785.65     MB/s   12.28
134217728       505.86     MB/s   3.95
268435456       524.39     MB/s   2.05
536870912       624.46     MB/s   1.22
1073741824      409.16     MB/s   0.40
------------------------------------------------

Optimal Fsync Rate: 119.91 fsyncs/sec at Buffer Size: 16777216 bytes
--- PASS: TestFsyncBenchmark (104.20s)

*/
