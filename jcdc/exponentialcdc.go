package jcdc

/*
import (
	"math"
)

// manual port from the chunker.py of
// https://github.com/dbaarda/rollsum-chunking/blob/master/chunker.py

func solve(f func(float64) float64, x0, x1, e float64) float64 {
	// set defaults
	if x0 == 0 {
		x0 = -1.0e9
	}
	if x1 == 0 {
		x1 = 1.0e9
	}
	if e == 0 {
		e = 1.0e-9
	}

	//""" Solve f(x)=0 for x where x0<=x<=x1 within +-e. """
	y0 := f(x0)
	y1 := f(x1)
	if y0*y1 <= 0 {
		panic("y0 and y1 must have different sign.")
	}
	for (x1 - x0) > e {
		xm := (x0 + x1) / 2.0
		ym := f(xm)
		if y0*ym > 0 {
			x0, y0 = xm, ym
		} else {
			x1, y1 = xm, ym
		}
	}
	return x0
}
*/
