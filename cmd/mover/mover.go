// rename a directory atomically
package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
)

func main() {
	args := os.Args
	if len(args) != 3 {
		fmt.Fprintf(os.Stderr, "mover must have 2 arguments: newpath oldpath, in that order. After, only the oldpath top dir will exist, but its contents will be that of newpath. We see %v: '%#v'\n", len(args), args)
		os.Exit(1)
	}
	oldpath := args[1]
	newpath := args[2]
	fmt.Printf("moving %v -> %v\n", newpath, oldpath)
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

// this is dreck, use os.Chmod() instead., but the idea of making
// all dir read-only is good, to lock out updates while we sync.
func main2_make_dirs_readonly() {
	dirPath := flag.String("dir", "", "Directory to lock")
	flag.Parse()

	if *dirPath == "" {
		fmt.Println("Usage: go run main.go -dir <directory>")
		os.Exit(1)
	}

	// Clean the path and remove trailing slash
	*dirPath = filepath.Clean(strings.TrimSuffix(*dirPath, "/"))

	// Verify directory exists and is accessible
	info, err := os.Stat(*dirPath)
	if err != nil {
		fmt.Printf("Error accessing directory: %v\n", err)
		os.Exit(1)
	}
	if !info.IsDir() {
		fmt.Printf("Error: %s is not a directory\n", *dirPath)
		os.Exit(1)
	}

	// Set up signal handling for cleanup
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start a goroutine to handle signals
	go func() {
		<-sigChan
		fmt.Println("\nReceived interrupt signal, unlocking directory...")
		unlockDirectory(*dirPath)
		os.Exit(0)
	}()

	// Lock the directory
	if err := lockDirectory(*dirPath); err != nil {
		fmt.Printf("Error locking directory: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("Directory tree is now locked for writing")
	fmt.Println("Press Enter to unlock and exit...")

	// Wait for user input
	fmt.Scanln()

	// Unlock the directory
	if err := unlockDirectory(*dirPath); err != nil {
		fmt.Printf("Error unlocking directory: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("Directory tree unlocked")
}

func lockDirectory(dir string) error {
	return setImmutableFlag(dir, true)
}

func unlockDirectory(dir string) error {
	return setImmutableFlag(dir, false)
}

func setImmutableFlag(dir string, lock bool) error {
	// Build the find command to get all files and directories
	findCmd := exec.Command("find", dir)
	findOutput, err := findCmd.Output()
	if err != nil {
		return fmt.Errorf("failed to list directory contents: %v", err)
	}

	// Process each path
	paths := strings.Split(string(findOutput), "\n")
	flag := "+i"
	if !lock {
		flag = "-i"
	}

	for _, path := range paths {
		if path == "" {
			continue
		}

		// Execute chattr command
		chattrCmd := exec.Command("chattr", flag, path)
		if err := chattrCmd.Run(); err != nil {
			return fmt.Errorf("failed to set immutable flag on %s: %v", path, err)
		}
	}

	return nil
}
