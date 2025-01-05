package progress

import (
	"fmt"
	"os"
	"strings"
	"time"

	// to simulate variable transfer speed
	"math/rand"

	"golang.org/x/term"
)

/*
1. The \r character returns the cursor to the start of the line

2. \033[K (or \x1b[K) clears from the cursor to the end of the line

3. Make sure to print a newline (fmt.Println()) when the transfer is complete

4. For better handling of terminal output, you might want to use a library like github.com/cheggaaa/pb or github.com/schollz/progressbar

5. Some terminals might not support ANSI escape codes (though most modern ones do)
If you want to handle terminal capabilities more robustly, you could use the golang.org/x/term package:
*/

func isTerminal() bool {
	return term.IsTerminal(int(os.Stdout.Fd()))
}

func printProgress(current, total int64, filename string) {
	if !isTerminal() {
		return // or handle non-terminal output differently
	}
	// ... rest of progress printing code ...
}

type TransferStats struct {
	isTerm         bool
	filename       string
	fileSize       int64
	fileSizeString string
	lastUpdate     time.Time
	lastBytes      int64
	emaSpeed       float64 // bytes per second
	alpha          float64 // EMA smoothing factor (between 0 and 1)
}

func NewTransferStats(fileSize int64, filename string) *TransferStats {
	return &TransferStats{
		isTerm:         isTerminal(),
		fileSize:       fileSize,
		fileSizeString: formatBytes(float64(fileSize), true),
		filename:       filename,
		lastUpdate:     time.Now(),
		alpha:          0.1, // Adjust this value to change smoothing (higher = more reactive)
	}
}

func (ts *TransferStats) updateSpeed(currentBytes int64) (change int64) {
	now := time.Now()
	duration := now.Sub(ts.lastUpdate).Seconds()
	change = currentBytes - ts.lastBytes
	if duration > 0 {
		// Calculate current speed
		bytesTransferred := currentBytes - ts.lastBytes
		currentSpeed := float64(bytesTransferred) / duration

		// Update EMA
		if ts.emaSpeed == 0 {
			ts.emaSpeed = currentSpeed // Initialize EMA
		} else {
			ts.emaSpeed = ts.alpha*currentSpeed + (1-ts.alpha)*ts.emaSpeed
		}
	}
	ts.lastUpdate = now
	ts.lastBytes = currentBytes
	return
}

func ExampleTransferWithSpeed() {
	filename := "example.txt"
	fileSize := int64(1000000)
	stats := NewTransferStats(fileSize, filename)

	// Simulate varying transfer speeds
	for transferred := int64(0); transferred <= fileSize; transferred += 50000 {
		stats.PrintProgressWithSpeed(transferred)
		// Simulate variable transfer speeds
		time.Sleep(time.Duration(50+rand.Intn(100)) * time.Millisecond)
	}
	fmt.Println() // Final newline
}

// silent if not stdout is not a terminal.
func (s *TransferStats) PrintProgressWithSpeed(current int64) {
	width := 40 // Progress bar width
	percentage := float64(current) / float64(s.fileSize)
	completed := int(percentage * float64(width))

	// Update speed calculation
	changed := s.updateSpeed(current)

	if !s.isTerm {
		return
	}

	speed := formatBytes(s.emaSpeed, false)
	if changed == 0 {
		speed = "-stalled-"
	}
	// Build progress bar
	var bar strings.Builder
	bar.WriteString("[")
	for i := 0; i < width; i++ {
		if i < completed {
			bar.WriteRune('=')
		} else if i == completed {
			bar.WriteRune('>')
		} else {
			bar.WriteRune(' ')
		}
	}
	bar.WriteString("]")

	// Create the full status line with fixed width
	status := fmt.Sprintf("\r%-20s %s %6.2f%% %10s total: %s",
		truncateString(s.filename, 20),
		bar.String(),
		percentage*100,
		speed,
		s.fileSizeString,
	)

	// Write the entire line at once
	fmt.Print(status)
}

// Helper function to truncate or pad a string to exact width
func truncateString(s string, width int) string {
	if len(s) > width {
		return s[:width]
	}
	return fmt.Sprintf("%-*s", width, s) // Left align and pad with spaces
}

// Update formatBytes to always return same-width strings
func formatBytes(bytes float64, isTotal bool) string {
	units := []string{"B/s  ", "KB/s ", "MB/s ", "GB/s "}
	if isTotal {
		units = []string{"B", "KB", "MB", "GB"}
	}
	unitIndex := 0
	value := bytes

	for value >= 1024 && unitIndex < len(units)-1 {
		value /= 1024
		unitIndex++
	}
	if isTotal {
		return fmt.Sprintf("%0.2f %s", value, units[unitIndex])
	}
	return fmt.Sprintf("%7.2f %s", value, units[unitIndex]) // Fixed width of 7 for number
}

//func main() {
//	ExampleTransferWithSpeed()
//}
