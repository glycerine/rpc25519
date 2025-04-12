package jcdc

// precomputed distance table using the following code:
/*
	package main

	import (
		"fmt"
	)

	// Helper function to compute the Hamming weight (number of set bits) of a byte.
	func hammingWeight(b byte) int {
        // could also just: return bits.OnesCount8(b) using "math/bits"
		count := 0
		for b != 0 {
			count++
			b &= b - 1 // Clear the least significant set bit
		}
		return count
	}

	// Function to compute the Hamming distance between two bytes.
	func hammingDistance(a, b byte) int {
		return hammingWeight(a ^ b)
	}

	func main() {
        // don't need the full table at the moment, just one slice of it.
        // (see below).
        //
		//fmt.Println("var hammingDistanceTable [256][256]int = [256][256]int{")
        //
		//for outByte := 0; outByte < 256; outByte++ {
		//	fmt.Print("{")
		//	for inByte := 0; inByte < 256; inByte++ {
		//		distance := hammingDistance(byte(inByte), byte(outByte))
		//		fmt.Printf("%d, ", distance)
		//	}
		//	fmt.Println("},")
		//}
        //
		//fmt.Println("}")

        fmt.Println()
		fmt.Println("var hammingDistanceTo0xAA [256]int = [256]int{")
		for outByte := 0; outByte < 256; outByte++ {
				distance := hammingDistance(0xAA, byte(outByte))
				fmt.Printf("%d, ", distance)
		}
		fmt.Println("}")

	}
*/

var hammingDistanceTo0xAA [256]int = [256]int{
	4, 5, 3, 4, 5, 6, 4, 5, 3, 4, 2, 3, 4, 5, 3, 4, 5, 6, 4, 5, 6, 7, 5, 6, 4, 5, 3, 4, 5, 6, 4, 5, 3, 4, 2, 3, 4, 5, 3, 4, 2, 3, 1, 2, 3, 4, 2, 3, 4, 5, 3, 4, 5, 6, 4, 5, 3, 4, 2, 3, 4, 5, 3, 4, 5, 6, 4, 5, 6, 7, 5, 6, 4, 5, 3, 4, 5, 6, 4, 5, 6, 7, 5, 6, 7, 8, 6, 7, 5, 6, 4, 5, 6, 7, 5, 6, 4, 5, 3, 4, 5, 6, 4, 5, 3, 4, 2, 3, 4, 5, 3, 4, 5, 6, 4, 5, 6, 7, 5, 6, 4, 5, 3, 4, 5, 6, 4, 5, 3, 4, 2, 3, 4, 5, 3, 4, 2, 3, 1, 2, 3, 4, 2, 3, 4, 5, 3, 4, 5, 6, 4, 5, 3, 4, 2, 3, 4, 5, 3, 4, 2, 3, 1, 2, 3, 4, 2, 3, 1, 2, 0, 1, 2, 3, 1, 2, 3, 4, 2, 3, 4, 5, 3, 4, 2, 3, 1, 2, 3, 4, 2, 3, 4, 5, 3, 4, 5, 6, 4, 5, 3, 4, 2, 3, 4, 5, 3, 4, 5, 6, 4, 5, 6, 7, 5, 6, 4, 5, 3, 4, 5, 6, 4, 5, 3, 4, 2, 3, 4, 5, 3, 4, 2, 3, 1, 2, 3, 4, 2, 3, 4, 5, 3, 4, 5, 6, 4, 5, 3, 4, 2, 3, 4, 5, 3, 4}
