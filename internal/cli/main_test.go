package cli

import (
	"os"
	"testing"
)

// A trace stops when the disk runs low (the tracelog tests check that);
// here it must not depend on the free space of the machine running the
// tests.
func TestMain(m *testing.M) {
	traceMinFree = 0
	os.Exit(m.Run())
}
