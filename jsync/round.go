package jsync

func RoundUpPow2(x int64) int64 {
	if x <= 0 {
		return 1
	}

	// Subtract 1 from x to handle the case where x is already a power of 2
	x--

	// Set all bits after the highest set bit
	x |= x >> 1
	x |= x >> 2
	x |= x >> 4
	x |= x >> 8
	x |= x >> 16
	x |= x >> 32

	// Add 1 to get the next power of 2
	return x + 1

}
