//go:build synctest

package hermes

import (
	"testing"
	"testing/synctest"
)

func hermesBubble(t *testing.T, f func(t *testing.T)) {
	synctest.Test(t, f)
}
