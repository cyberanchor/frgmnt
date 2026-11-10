//go:build !linux

package tracelog

// diskFree is not implemented here: the trace does not stop on a full disk.
func diskFree(string) (int64, bool) { return 0, false }
