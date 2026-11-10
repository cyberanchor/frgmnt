package rtstats

import (
	"math"
	"runtime"
	"testing"
	"time"
)

func TestReadGivesTheRuntimeState(t *testing.T) {
	r := NewReader()
	_ = r.Read()
	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	s := r.Read()
	if s.Goroutines == 0 || s.HeapBytes == 0 || s.TotalBytes < s.HeapBytes || s.GCCycles == 0 {
		t.Errorf("sample %+v", s)
	}
	if s.GCPauses == 0 {
		t.Errorf("no GC pause in an interval with runtime.GC: %+v", s)
	}
	if runtime.GOOS == "linux" && (s.RSSBytes == 0 || s.OpenFiles == 0 || s.ProcUserS+s.ProcSysS == 0) {
		t.Errorf("no process figures on Linux: %+v", s)
	}
}

// The histograms cover the interval since the previous Read: a second
// Read with nothing new has no GC pauses.
func TestHistogramsCoverTheInterval(t *testing.T) {
	r := NewReader()
	runtime.GC()
	if s := r.Read(); s.GCPauses == 0 {
		t.Fatalf("first read: no pauses: %+v", s)
	}
	if s := r.Read(); s.GCPauses != 0 {
		t.Errorf("second read right after: %d pauses, want 0", s.GCPauses)
	}
}

func TestQuantile(t *testing.T) {
	buckets := []float64{0, 1, 2, 4, math.Inf(1)}
	counts := []uint64{50, 40, 9, 1}
	for q, want := range map[float64]float64{0.5: 1, 0.9: 2, 0.99: 4, 1: 4} {
		if got := quantile(counts, buckets, q); got != want {
			t.Errorf("q%.2f = %v, want %v", q, got, want)
		}
	}
	if got := quantile([]uint64{0, 0, 0, 0}, buckets, 0.5); got != 0 {
		t.Errorf("empty: %v", got)
	}
}
