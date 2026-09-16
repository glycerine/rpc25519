//go:build !synctest

package hermes

import "testing"

func hermesBubble(t *testing.T, f func(t *testing.T)) {
	f(t)
}
