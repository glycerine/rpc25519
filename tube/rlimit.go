package tube

import (
	"fmt"
	"syscall"
)

var _ = fmt.Printf

func raiseFileHandleLimit(target uint64) {

	// 1. Check current limits
	var rLimit0 syscall.Rlimit
	err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit0)
	panicOn(err)
	//fmt.Printf("Current Limit: Cur %d, Max %d\n", rLimit.Cur, rLimit.Max)

	if rLimit0.Cur > target {
		// already done. don't lower it.
		return
	}

	// 2. Set the desired new limit
	rLimit1 := rLimit0
	if rLimit0.Max < target {
		// If the hard limit is less than our target, we can only set it to the hard limit
		// (unless we are root and change the hard limit first)
		target = rLimit0.Max
	}

	rLimit1.Cur = target

	// 3. Apply the new limit
	err = syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit1)
	panicOn(err)

	// Verify
	var rLimit2 syscall.Rlimit
	syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit2)
	if rLimit2.Cur != rLimit0.Cur {
		fmt.Printf("open file handle limit: %v (was %v)\n", rLimit2.Cur, rLimit0.Cur)
	}
}
