package rpc25519

import (
	"fmt"
	"os"
	"runtime/debug"
)

var LAST_GIT_COMMIT_HASH string
var NEAREST_GIT_TAG string
var GIT_BRANCH string
var GO_VERSION string

func GetCodeVersion(programName string) string {
	return fmt.Sprintf("%s commit: %s / nearest-git-tag: %s / branch: %s / go version: %s\n",
		programName, LAST_GIT_COMMIT_HASH, NEAREST_GIT_TAG, GIT_BRANCH, GO_VERSION)
}

func Exit1IfVersionReq() {
	for _, a := range os.Args {
		if a == "-version" || a == "--version" {

			if bi, ok := debug.ReadBuildInfo(); ok {

				fmt.Fprintf(os.Stderr, "%v version: %+v\n", os.Args[0], bi)

				fmt.Fprintf(os.Stderr, "\n%s\n", GetCodeVersion(os.Args[0]))
				os.Exit(1)
			}
		}
	}
}
