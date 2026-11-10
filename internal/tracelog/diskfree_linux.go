//go:build linux

package tracelog

import "syscall"

// diskFree reports the space an unprivileged process can still use on the
// file system holding dir.
func diskFree(dir string) (int64, bool) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return 0, false
	}
	return int64(st.Bavail) * int64(st.Bsize), true
}
