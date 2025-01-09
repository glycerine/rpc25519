package rpc25519

// mostly for tests. compare files and directories
// for expected structure.

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

func SliceEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func SliceStringEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func SliceBoolEqual(a, b []bool) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func dup(a []string) []string {
	b := make([]string, len(a))
	for i := range a {
		b[i] = a[i]
	}
	return b
}

func dupIntSlc(a []int) []int {
	b := make([]int, len(a))
	for i := range a {
		b[i] = a[i]
	}
	return b
}

// err indicates filesystem operation error, not difference.
func CompareDirs(expected string, observed string) (diff string, err error) {
	obs, err := ListFilesUnderDir(observed, false, "")
	panicOn(err)
	exp, err := ListFilesUnderDir(expected, false, "")
	panicOn(err)

	//vv("observed:")
	omap := make(map[string]bool)
	emap := make(map[string]bool)
	for _, o := range obs {
		omap[o] = true
		//fmt.Println(o)
	}
	//vv("expected:")
	for _, e := range exp {
		emap[e] = true
		//fmt.Println(e)
	}
	// compare trees on the surface
	if StringSlicesDiffer(exp, obs) {
		return fmt.Sprintf("CompareDirs difference: expected='%v' (%v files); observed='%v' (%v files) had different surface tree structure. e-o='%v'; o-e='%v'", expected, len(exp), observed, len(obs), StringSliceSub(exp, obs, expected+sep), StringSliceSub(obs, exp, observed+sep)), nil
	}

	// check contents
	for i := range exp {
		e := expected + sep + exp[i]
		o := observed + sep + obs[i]
		//vv("e = '%v'", e)
		//vv("o = '%v'", o)
		n, diff, err := DiffDontStop(e, o)
		if err != nil || n > 0 {
			return fmt.Sprintf("CompareDirs difference in content between expected file '%v' and observed file '%v': '%v' (/usr/bin/diff err='%v')", e, o, string(diff), err), nil
		}
	}
	return
}

// if includeRoot, return the full path, otherwise reltaive to root.
// if requriedSuffix supplied, files must end in that.
func ListFilesUnderDir(root string, includeRoot bool, requiredSuffix string) (files []string, err error) {
	if !dirExists(root) {
		return
	}
	n := len(root) + 1
	if includeRoot {
		n = 0
	}
	err = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if len(path) < n {
			//vv("short path='%v'; len=%v", path, len(path))
		} else {
			if info == nil {
				vv("info was nil for path = '%v'", path)
			}
			if info.IsDir() {
				// skip directories.
			} else {
				if requiredSuffix == "" || strings.HasSuffix(path, requiredSuffix) {
					files = append(files, path[n:])
				}
			}
		}
		return nil
	})
	return
}

func CompareFilesDiffLen(expected string, observed string) (diffLineCount int) {
	diffLineCount, _ = CompareFiles(expected, observed)
	return
}

// returns length of diff
func CompareFiles(expected string, observed string) (int, []byte) {
	cmd := exec.Command("diff", "-b", observed, expected)
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		if false { // don't stop now.
			stop(fmt.Sprintf("CompareFiles(): error during '\ndiff %s %s\n': %s", observed, expected, err))
		}
		fmt.Printf("CompareFiles(): error during '\ndiff %s %s\n': %s", observed, expected, err)
		return -1, nil // unknown, but not 0
	}

	N := len(out.Bytes())
	if N != 0 {
		// too verbose.
		//fmt.Printf("\nexpected 0-length diff, but got:\n%s\n", string(out.Bytes()))
	}

	return N, out.Bytes()
}

func CompareFilesFirstNLines(diffFirstNlines int, expected string, observed string) (int, []byte) {
	alwaysPrintf("doing:\ndiff %v %v\n", observed, expected)

	x, err := ioutil.ReadFile(expected)
	if err != nil {
		fmt.Printf("CompareFiles(): error during '\ndiff %s %s\n': %s", observed, expected, err)
		return -2, nil // unknown, but not 0
	}
	o, err := ioutil.ReadFile(observed)
	if err != nil {
		fmt.Printf("CompareFiles(): error during '\ndiff %s %s\n': %s", observed, expected, err)
		return -2, nil // unknown, but not 0
	}

	xl := strings.Split(string(x), "\n")
	ol := strings.Split(string(o), "\n")

	if len(xl) < diffFirstNlines {
		return 1, []byte("expected did not have enough lines")
	}
	if len(ol) < diffFirstNlines {
		return 1, []byte("observed did not have enough lines")
	}
	for i := 0; i < diffFirstNlines; i++ {
		if xl[i] != ol[i] {
			return i + 1, []byte(fmt.Sprintf("first difference on line %v:\n ===== expected:\n\n%v\n\n ===== observed:\n\n%v\n\n", i+1, xl[i], ol[i]))
		}
	}
	return 0, nil
}

func StringSlicesDiffer(as, bs []string) bool {
	if len(as) != len(bs) {
		return true
	}
	for i := range as {
		if as[i] != bs[i] {
			return true
		}
	}
	return false
}

func DiffDontStop(expected string, observed string) (int, []byte, error) {
	cmd := exec.Command("diff", "-r", "-b", "-B", observed, expected)
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	N := len(out.Bytes())
	return N, out.Bytes(), err
}

// subtract bs from as and return the difference. Preserves
// the order given by as. aprefix is pre-pended to as returned.
func StringSliceSub(as, bs []string, aprefix string) (d []string) {
	amap := make(map[string]bool)
	for _, a := range as {
		amap[a] = true
	}
	for _, b := range bs {
		delete(amap, b)
	}
	for _, a := range as {
		if amap[a] {
			d = append(d, aprefix+a)
		}
	}
	return
}
