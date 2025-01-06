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

type TransferStats struct {
	isTerm         bool
	filename       string
	fileSize       int64
	fileSizeString string
	lastUpdate     time.Time
	lastBytes      int64
	emaSpeed       float64 // bytes per second
	alpha          float64 // EMA smoothing factor (between 0 and 1)

	lastDisplay        time.Time
	minRefreshInterval time.Duration
	startTime          time.Time
}

func NewTransferStats(fileSize int64, filename string) *TransferStats {
	now := time.Now()
	return &TransferStats{
		isTerm:             isTerminal(),
		fileSize:           fileSize,
		fileSizeString:     formatBytesTotal(float64(fileSize)),
		filename:           filename,
		lastUpdate:         now,
		alpha:              0.1, // Adjust this value to change smoothing (higher = more reactive)
		lastDisplay:        now,
		minRefreshInterval: 50 * time.Millisecond,
		startTime:          now}
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
	fileSize := int64(10000000)
	stats := NewTransferStats(fileSize, filename)

	// Simulate varying transfer speeds
	for transferred := int64(0); transferred <= fileSize; transferred += 50000 {
		stats.PrintProgressWithSpeed(transferred)
		// Simulate variable transfer speeds
		time.Sleep(time.Duration(50+rand.Intn(100)) * time.Millisecond)
	}
	fmt.Println() // Final newline
}

func (s *TransferStats) SilentProgressWithSpeed(current int64) {
	s.DoProgressWithSpeed(current, true)
}

// silent if not stdout is not a terminal.
func (s *TransferStats) PrintProgressWithSpeed(current int64) {
	s.DoProgressWithSpeed(current, false)
}

// Allow user choice of silent or not.
func (s *TransferStats) DoProgressWithSpeed(current int64, silent bool) {

	now := time.Now()
	if now.Sub(s.lastDisplay) < s.minRefreshInterval {
		return
	}
	s.lastDisplay = now

	width := 30 // Progress bar width
	percentage := float64(current) / float64(s.fileSize)
	completed := int(percentage * float64(width))

	// Update speed calculation
	changed := s.updateSpeed(current)

	if !s.isTerm || silent {
		return
	}

	// Calculate ETA
	var eta time.Duration
	if s.emaSpeed > 0 {
		remainingBytes := s.fileSize - current
		eta = time.Duration(float64(remainingBytes)/s.emaSpeed) * time.Second
	}

	speed := formatSpeed(s.emaSpeed)
	if current != s.fileSize && changed == 0 {
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

	// Format the status line
	//
	status := fmt.Sprintf("\r%s %s %3.0f%%  %6s  %7s  %s ETA",
		truncateString(s.filename, 20),
		bar.String(),
		percentage*100,
		formatBytesTotal(float64(current)),
		speed,
		formatDuration(eta))

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
func formatSpeed(bytes float64) string {
	units := []string{"B/s  ", "KB/s ", "MB/s ", "GB/s "}

	unitIndex := 0
	value := bytes

	for value >= 1024 && unitIndex < len(units)-1 {
		value /= 1024
		unitIndex++
	}
	return fmt.Sprintf("%7.1f %s", value, units[unitIndex]) // Fixed width of 7 for number
}

// Update formatBytes to match scp style
func formatBytesTotal(value float64) string {
	units := []string{"B", "KB", "MB", "GB", "TB"}
	unitIndex := 0

	for value >= 1024 && unitIndex < len(units)-1 {
		value /= 1024
		unitIndex++
	}
	return fmt.Sprintf("%5.1f %s", value, units[unitIndex])
}

func formatDuration(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	// Format as mm:ss for durations less than 1 hour
	if d.Hours() < 1 {
		return fmt.Sprintf("%02d:%02d",
			int(d.Minutes()),
			int(d.Seconds())%60)
	}
	// Format as hh:mm:ss for longer durations
	return fmt.Sprintf("%02d:%02d:%02d",
		int(d.Hours()),
		int(d.Minutes())%60,
		int(d.Seconds())%60)
}

//func main() {
//	ExampleTransferWithSpeed()
//}
