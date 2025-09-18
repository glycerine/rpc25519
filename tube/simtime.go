package tube

import (
	"sync"
	"time"
)

// Clock is an interface that abstracts time operations
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
	NewTicker(d time.Duration) Ticker
	Sleep(d time.Duration)
}

// Ticker abstracts the time.Ticker
type Ticker interface {
	C() <-chan time.Time
	Stop()
}

// RealClock implements Clock with actual time
type RealClock struct{}

func (RealClock) Now() time.Time                         { return time.Now() }
func (RealClock) After(d time.Duration) <-chan time.Time { return time.After(d) }
func (RealClock) Sleep(d time.Duration)                  { time.Sleep(d) }

func (RealClock) NewTicker(d time.Duration) Ticker {
	t := time.NewTicker(d)
	return &realTicker{t}
}

type realTicker struct {
	*time.Ticker
}

func (t *realTicker) C() <-chan time.Time { return t.Ticker.C }

// MockClock implements Clock with controllable time
type MockClock struct {
	mu      sync.Mutex
	now     time.Time
	timers  []*mockTimer
	tickers []*mockTicker
}

func NewMockClock(now time.Time) *MockClock {
	return &MockClock{now: now}
}

func (c *MockClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *MockClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	ch := make(chan time.Time, 1)
	timer := &mockTimer{
		c:       c,
		fireAt:  c.now.Add(d),
		ch:      ch,
		stopped: false,
	}
	c.timers = append(c.timers, timer)
	return ch
}

func (c *MockClock) NewTicker(d time.Duration) Ticker {
	c.mu.Lock()
	defer c.mu.Unlock()

	ch := make(chan time.Time, 1)
	ticker := &mockTicker{
		c:        c,
		ch:       ch,
		interval: d,
		nextTick: c.now.Add(d),
		stopped:  false,
	}
	c.tickers = append(c.tickers, ticker)
	return ticker
}

func (c *MockClock) Sleep(d time.Duration) {
	<-c.After(d)
}

// Advance moves the mock clock forward by the specified duration
// It triggers any timers or tickers that should fire during this period
func (c *MockClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	end := c.now.Add(d)

	for c.now.Before(end) {
		nextEvent := end

		// Find the next timer that will fire
		for _, timer := range c.timers {
			if !timer.stopped && timer.fireAt.Before(nextEvent) {
				nextEvent = timer.fireAt
			}
		}

		// Find the next ticker that will tick
		for _, ticker := range c.tickers {
			if !ticker.stopped && ticker.nextTick.Before(nextEvent) {
				nextEvent = ticker.nextTick
			}
		}

		// Advance time to the next event
		c.now = nextEvent

		// Fire timers
		activeTimers := make([]*mockTimer, 0, len(c.timers))
		for _, timer := range c.timers {
			if !timer.stopped {
				if !timer.fireAt.After(c.now) {
					// Timer fires
					select {
					case timer.ch <- c.now:
					default:
					}
					timer.stopped = true
				} else {
					activeTimers = append(activeTimers, timer)
				}
			}
		}
		c.timers = activeTimers

		// Process tickers
		for _, ticker := range c.tickers {
			if !ticker.stopped {
				for !ticker.nextTick.After(c.now) {
					select {
					case ticker.ch <- ticker.nextTick:
					default:
					}
					ticker.nextTick = ticker.nextTick.Add(ticker.interval)
				}
			}
		}
	}
}

type mockTimer struct {
	c       *MockClock
	fireAt  time.Time
	ch      chan time.Time
	stopped bool
}

type mockTicker struct {
	c        *MockClock
	interval time.Duration
	nextTick time.Time
	ch       chan time.Time
	stopped  bool
}

func (t *mockTicker) C() <-chan time.Time { return t.ch }

func (t *mockTicker) Stop() {
	t.c.mu.Lock()
	defer t.c.mu.Unlock()
	t.stopped = true
}

// Node represents a node in the Raft cluster with clock dependency injection
type Node struct {
	id              int
	clock           Clock
	electionTimeout time.Duration
	electionTimer   <-chan time.Time
	heartbeatTicker Ticker
	state           string // "follower", "candidate", "leader"
	// Other Raft state...
}

func NewNode(id int, clock Clock) *Node {
	node := &Node{
		id:              id,
		clock:           clock,
		state:           "follower",
		electionTimeout: randomTimeout(150, 300, clock), // Typical Raft timeout range in ms
	}

	// Initialize election timer
	node.resetElectionTimer()

	return node
}

func randomTimeout(min, max int, clock Clock) time.Duration {
	// Use a deterministic random source in tests
	rang := max - min
	return time.Duration(min+rang/2) * time.Millisecond
}

func (n *Node) resetElectionTimer() {
	n.electionTimeout = randomTimeout(150, 300, n.clock)
	n.electionTimer = n.clock.After(n.electionTimeout)
}

func (n *Node) becomeLeader() {
	n.state = "leader"
	if n.heartbeatTicker != nil {
		n.heartbeatTicker.Stop()
	}
	n.heartbeatTicker = n.clock.NewTicker(50 * time.Millisecond)
	// Other leader initialization...
}

func (n *Node) becomeCandidate() {
	n.state = "candidate"
	// Start election process...
	// After election logic, eventually call becomeLeader() or revert to follower
}

/*

import (
	"math/rand/v2"
	"time"
)

func randomTimeout(min, max int, clock Clock) time.Duration {
	// Use the thread-local PRNG from math/rand/v2
	range := max - min
	return time.Duration(min + rand.IntN(range)) * time.Millisecond
}

In testing scenarios where you want deterministic randomness, you can create a custom source and use it with a rand.Rand instance:

import (
"math/rand/v2"
)

// For testing with deterministic randomness
func deterministicRandomTimeout(min, max int, seed int64) time.Duration {
src := rand.NewPCG(seed, seed) // Create deterministic source with PCG algorithm
rng := rand.New(src)           // Create a rand.Rand with this source

range := max - min
return time.Duration(min + rng.IntN(range)) * time.Millisecond
}

For the most efficient use of the thread-local PRNG in your Raft implementation, simply use the global functions from math/rand/v2 in your timing-sensitive code paths, like election timeouts and backoff calculations.

*/
