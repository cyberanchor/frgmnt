//go:build linux

package rtstats

import (
	"os"
	"strconv"
	"strings"
	"syscall"
)

// procStats adds what Linux tells about the process: resident memory,
// open files, CPU time.
func procStats(s *Sample) {
	if raw, err := os.ReadFile("/proc/self/statm"); err == nil {
		if f := strings.Fields(string(raw)); len(f) > 1 {
			if pages, err := strconv.ParseInt(f[1], 10, 64); err == nil {
				s.RSSBytes = pages * int64(os.Getpagesize())
			}
		}
	}
	if d, err := os.Open("/proc/self/fd"); err == nil {
		if names, err := d.Readdirnames(-1); err == nil {
			s.OpenFiles = len(names) - 1 // the directory itself is open while it is read
		}
		d.Close()
	}
	var ru syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &ru) == nil {
		s.ProcUserS = float64(ru.Utime.Sec) + float64(ru.Utime.Usec)/1e6
		s.ProcSysS = float64(ru.Stime.Sec) + float64(ru.Stime.Usec)/1e6
	}
}
