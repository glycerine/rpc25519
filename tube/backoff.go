package tube

import (
	//cry "crypto/rand"
	//"encoding/binary"
	"math"
	"time"
)

// expBackoffConfig defines the parameters for exponential backoff
type expBackoffConfig struct {
	// Initial delay before first retry
	InitialDelay time.Duration
	// Maximum delay between retries
	MaxDelay time.Duration
	// Factor to multiply the delay by after each retry
	Factor float64
	// Jitter adds randomness to prevent thundering herd.
	// Percentage of the current delay to use for
	// a jitter window around 0.
	Jitter float64
}

// DefaultBackoffConfig provides reasonable defaults
var defaultExpBackoffConfig = expBackoffConfig{
	InitialDelay: 10 * time.Millisecond,
	MaxDelay:     5 * time.Second,
	Factor:       2.0,
	Jitter:       0.2, // 20% jitter
}

// Backoff implements exponential backoff with jitter
type expBackoff struct {
	config  expBackoffConfig
	attempt int
	last    time.Time
}

// NewBackoff creates a new Backoff instance
func newExpBackoff(config expBackoffConfig) *expBackoff {
	return &expBackoff{
		config:  config,
		attempt: 0,
	}
}

// Next returns the next delay duration
func (b *expBackoff) next() time.Duration {
	// Calculate exponential delay
	delay := float64(b.config.InitialDelay) * math.Pow(b.config.Factor, float64(b.attempt))

	// Apply jitter
	f1 := float64(cryptoRandInt64RangePosOrNeg(1e6-1)) / 2e6 // in (-0.5, 0.5)
	jitter := f1 * b.config.Jitter * delay
	delay += jitter

	// Cap at max delay
	if delay > float64(b.config.MaxDelay) {
		delay = float64(b.config.MaxDelay)
		// but still jitter, a little.
		jitter = f1 * b.config.Jitter * delay / 2
		if jitter < 0 {
			jitter = -jitter
		}
		delay += jitter
	}

	b.attempt++
	b.last = time.Now()
	return time.Duration(delay)
}

// Reset resets the backoff counter
func (b *expBackoff) reset() {
	b.attempt = 0
	b.last = time.Time{}
}
