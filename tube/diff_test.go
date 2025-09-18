package tube

import (
	"fmt"
	"testing"
)

func TestTextDiff(t *testing.T) {
	old := `old0`
	b := `new0`

	diff := textDiff(old, b)
	//vv("diff = '%v'", diff)

	want := `[OLD line 0] old0
[NEW line 0] new0
`

	if diff != want {
		panic(fmt.Sprintf("got '%v'; want '%v'", diff, want))
	}

	old = `old0
old1
old2`
	b = `old0
new1
old2`

	diff = textDiff(old, b)
	//vv("diff = '%v'", diff)

	want = `[OLD line 1] old1
[NEW line 1] new1
`

	if diff != want {
		fmt.Printf("got: \n'%v'\n\n want:\n'%v'\n", diff, want)
		panic("fix")
	}

	old = `old0
old1
old2`
	b = `old0
old1
new2`

	diff = textDiff(old, b)
	//vv("diff = '%v'", diff)

	want = `---
[OLD line 2] old2
[NEW line 2] new2
`

	if diff != want {
		fmt.Printf("got: \n'%v'\n\n want:\n'%v'\n", diff, want)
		panic("fix")
	}

}
