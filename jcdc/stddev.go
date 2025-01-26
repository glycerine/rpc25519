package jcdc

import (
	"math"
)

// StdDevTracker tracks the running stddev and mean
// as each observation is added with AddObs(). Use
// the methods Mean() and SampleStdDev() to retrieve
// the summary statistics.
//
// Algorithm reference:
// SANDIA REPORT SAND2008-6212
// Unlimited Release
// Printed September 2008
// "Formulas for Robust, One-Pass Parallel
// Computation of Covariances and Arbitrary-Order Statistical Moments"
// by Philippe Pebay
// http://prod.sandia.gov/techlib/access-control.cgi/2008/086212.pdf (now stale?)
// backup urls:
// https://www.osti.gov/biblio/1028931
// https://www.osti.gov/servlets/purl/1028931
type StdDevTracker struct {

	// W is the sum of all weights seen.
	W float64

	// A is the weighted mean
	A float64

	// Q is the weighted numerator for the variance (the Quadratic term)
	Q float64
}

// Mean returns the weighted mean.
func (s *StdDevTracker) Mean() float64 {
	return s.A
}

// SampleStdDev returns the weighted sample standard deviation.
func (s *StdDevTracker) SampleStdDev() float64 {
	return math.Sqrt(s.Q / (s.W - 1))
}

// AddObs adds the observation x with the given weight
// to the tracker.
func (s *StdDevTracker) AddObs(x float64, weight float64) {

	// W is the sum of all weights seen.
	s.W += weight

	// need to save the old value for the updates below.
	a0 := s.A

	// A is the weighted mean
	s.A = a0 + weight*(x-a0)/s.W

	// update the quadratic term.
	// Q/W gives the weighted variance, when needed.
	s.Q += weight * (x - a0) * (x - s.A)
}

// MeanSd returns the mean and sample standard
// deviation from a single pass through the observations in x.
func MeanSd(x []float64) (mean, stddev float64) {
	var sdt StdDevTracker
	for _, v := range x {
		sdt.AddObs(v, 1)
	}
	return sdt.Mean(), sdt.SampleStdDev()
}
