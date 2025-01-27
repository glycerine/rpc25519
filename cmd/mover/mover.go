// rename a directory atomically
package main

import (
	"fmt"
	"os"
)

func main() {
	args := os.Args
	if len(args) != 3 {
		fmt.Fprintf(os.Stderr, "mover must have 2 arguments: oldpath newpath. We see %v: '%#v'\n", len(args), args)
		os.Exit(1)
	}
	oldpath := args[1]
	newpath := args[2]
	fmt.Printf("moving %v -> %v\n", oldpath, newpath)
	// Check if newpath exists and is a directory
	info, err := os.Stat(oldpath)
	if err == nil && !info.IsDir() {
		fmt.Fprintf(os.Stderr, "oldpath exists but is not a directory: '%v'\n", oldpath)
		os.Exit(1)
	}
	info, err = os.Stat(newpath)
	if err == nil && !info.IsDir() {
		fmt.Fprintf(os.Stderr, "newpath exists but is not a directory: '%v'\n", newpath)
		os.Exit(1)
	}

	// Suppose:
	// update in newpath
	// oldvers in oldpath
	err = atomicDirSwap(oldpath, newpath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to rename directory: %v\n", err)
		os.Exit(1)
	}
	// after the atomic swap, now we can delete "newpath"
	// which now has oldvers in it.

	err = os.RemoveAll(newpath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to remove directory: %v\n", err)
		os.Exit(1)
	}

}
