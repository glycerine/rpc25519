package tube

import (
	"fmt"
	"strings"

	rb "github.com/glycerine/rbtree"
)

type sortme struct {
	linenum int
	line    string
}

// old is old; b is new. The diff will be labeled thusly.
func textDiff(old, b string) (r string) {

	olds := strings.Split(old, "\n")
	bs := strings.Split(b, "\n")
	oldmap := make(map[string]int)
	bmap := make(map[string]int)
	for i, a0 := range olds {
		oldmap[a0] = i
	}
	for i, b0 := range bs {
		bmap[b0] = i
	}
	oldleft := rb.NewTree(sortByLineNum)
	bleft := rb.NewTree(sortByLineNum)
	oldleftmap := make(map[string]int)
	bleftmap := make(map[string]int)

	for i, a0 := range olds {
		if _, ok := bmap[a0]; ok {
			// skip this line
		} else {
			oldleft.Insert(&sortme{linenum: i, line: a0})
			oldleftmap[a0] = i
		}
	}
	for i, b0 := range bs {
		if _, ok := oldmap[b0]; ok {
			// skip this line
		} else {
			bleft.Insert(&sortme{linenum: i, line: b0})
			bleftmap[b0] = i
		}
	}
	nold := len(olds)
	nb := len(bs)
	mx := max(nold, nb)
	i := 0
	const contigTogether = false
	lastprint := 0
	for i < mx {
		oldprev := i
		bprev := i
		if i < nold {
			if _, ok := oldleftmap[olds[i]]; ok {
				if i > lastprint+1 {
					r += "---\n"
				}
				lastprint = i
				r += fmt.Sprintf("[OLD line %v] %v\n", i, olds[i])
				if contigTogether {
					it := oldleft.FindGE(&sortme{linenum: i})
					it = it.Next()
					for ; !it.Limit(); it = it.Next() {
						sm := it.Item().(*sortme)
						j := sm.linenum
						if j == oldprev+1 {
							r += fmt.Sprintf("[OLD line %v] %v\n", j, olds[j])
							oldprev = j
						} else {
							// end of contiguous span of A-only.
							break
						}
					}
				}
			}
		}
		if i < nb {
			if _, ok := bleftmap[bs[i]]; ok {
				if i > lastprint+1 {
					r += "---\n"
				}
				lastprint = i
				r += fmt.Sprintf("[NEW line %v] %v\n", i, bs[i])
				if contigTogether {
					it := bleft.FindGE(&sortme{linenum: i})
					it = it.Next()
					for ; !it.Limit(); it = it.Next() {
						sm := it.Item().(*sortme)
						j := sm.linenum
						if j == bprev+1 {
							r += fmt.Sprintf("[NEW line %v] %v\n", j, bs[j])
							bprev = j
						} else {
							// end of contiguous span of A-only.
							break
						}
					}
				}
			}
		}
		i = max(oldprev, bprev) + 1
	}

	return
}

func sortByLineNum(a, b rb.Item) int {
	ak := a.(*sortme).linenum
	bk := b.(*sortme).linenum
	switch {
	case ak < bk:
		return -1
	case ak > bk:
		return 1
	default:
		return 0
	}
}
