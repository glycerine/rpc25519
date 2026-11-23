//go:build !par

package tube

// parFullSuite: without -tags par, only the fast, representative
// subset of PAR tests runs; see parFullOnly.
const parFullSuite = false
