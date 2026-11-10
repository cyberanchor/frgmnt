// Package rtstats reads the state of the Go runtime and of the process
// for the trace: goroutines, memory, garbage collection, how long
// goroutines wait to run, CPU time, and on Linux the resident memory and
// the open files. It is what pprof would show over time, sampled once a
// second into the pipeline events at almost no cost.
package rtstats

import (
	"math"
	"runtime/metrics"
	"sync"
)

// Sample is the state at one moment. The histogram figures (GC pauses,
// scheduling latency) cover the time since the previous Read.
type Sample struct {
	Goroutines uint64 `json:"goroutines"`
	HeapBytes  uint64 `json:"heap_bytes"`  // live heap objects
	HeapGoal   uint64 `json:"heap_goal"`   // heap size the next GC aims at
	TotalBytes uint64 `json:"total_bytes"` // all memory the Go runtime has mapped
	GCCycles   uint64 `json:"gc_cycles"`   // since the start

	GCPauseP50Ms  float64 `json:"gc_pause_p50_ms"`
	GCPauseMaxMs  float64 `json:"gc_pause_max_ms"`
	GCPauses      uint64  `json:"gc_pauses"`        // in the interval
	SchedLatP50Ms float64 `json:"sched_lat_p50_ms"` // how long a runnable goroutine waited for a CPU
	SchedLatP99Ms float64 `json:"sched_lat_p99_ms"` // (one CPU and 200 workers: this is the first thing to look at)
	SchedLatMaxMs float64 `json:"sched_lat_max_ms"` // upper bound of the highest bucket hit
	UserCPUS      float64 `json:"user_cpu_s"`       // Go code, estimated by the runtime, since the start
	GCCPUS        float64 `json:"gc_cpu_s"`         // the garbage collector, likewise

	// From the operating system (Linux).
	RSSBytes  int64   `json:"rss_bytes,omitempty"`
	OpenFiles int     `json:"open_files,omitempty"`
	ProcUserS float64 `json:"proc_user_s,omitempty"` // CPU time of the process, user
	ProcSysS  float64 `json:"proc_sys_s,omitempty"`  // and system
}

// Reader reads Samples; it keeps the histograms of the previous Read so a
// Sample covers the interval since then. Safe for concurrent use.
type Reader struct {
	mu      sync.Mutex
	samples []metrics.Sample
	prev    map[string][]uint64
}

// The metrics, by name; names a Go version does not have are skipped
// (the GC pause metric was renamed in Go 1.22).
const (
	mGoroutines = "/sched/goroutines:goroutines"
	mHeap       = "/memory/classes/heap/objects:bytes"
	mHeapGoal   = "/gc/heap/goal:bytes"
	mTotal      = "/memory/classes/total:bytes"
	mGCCycles   = "/gc/cycles/total:gc-cycles"
	mGCPauses   = "/sched/pauses/total/gc:seconds" // Go 1.22+
	mGCPausesV1 = "/gc/pauses:seconds"             // Go 1.21
	mSchedLat   = "/sched/latencies:seconds"
	mUserCPU    = "/cpu/classes/user:cpu-seconds"
	mGCCPU      = "/cpu/classes/gc/total:cpu-seconds"
)

// NewReader returns a Reader of the metrics this Go version has.
func NewReader() *Reader {
	have := map[string]bool{}
	for _, d := range metrics.All() {
		have[d.Name] = true
	}
	r := &Reader{prev: map[string][]uint64{}}
	for _, name := range []string{mGoroutines, mHeap, mHeapGoal, mTotal, mGCCycles, mSchedLat, mUserCPU, mGCCPU} {
		if have[name] {
			r.samples = append(r.samples, metrics.Sample{Name: name})
		}
	}
	switch {
	case have[mGCPauses]:
		r.samples = append(r.samples, metrics.Sample{Name: mGCPauses})
	case have[mGCPausesV1]:
		r.samples = append(r.samples, metrics.Sample{Name: mGCPausesV1})
	}
	return r
}

// Read takes a Sample.
func (r *Reader) Read() Sample {
	r.mu.Lock()
	defer r.mu.Unlock()
	metrics.Read(r.samples)
	var s Sample
	for _, m := range r.samples {
		switch m.Value.Kind() {
		case metrics.KindUint64:
			v := m.Value.Uint64()
			switch m.Name {
			case mGoroutines:
				s.Goroutines = v
			case mHeap:
				s.HeapBytes = v
			case mHeapGoal:
				s.HeapGoal = v
			case mTotal:
				s.TotalBytes = v
			case mGCCycles:
				s.GCCycles = v
			}
		case metrics.KindFloat64:
			switch m.Name {
			case mUserCPU:
				s.UserCPUS = m.Value.Float64()
			case mGCCPU:
				s.GCCPUS = m.Value.Float64()
			}
		case metrics.KindFloat64Histogram:
			h := m.Value.Float64Histogram()
			counts := r.delta(m.Name, h.Counts)
			switch m.Name {
			case mSchedLat:
				s.SchedLatP50Ms = quantile(counts, h.Buckets, 0.50) * 1000
				s.SchedLatP99Ms = quantile(counts, h.Buckets, 0.99) * 1000
				s.SchedLatMaxMs = quantile(counts, h.Buckets, 1) * 1000
			case mGCPauses, mGCPausesV1:
				s.GCPauseP50Ms = quantile(counts, h.Buckets, 0.50) * 1000
				s.GCPauseMaxMs = quantile(counts, h.Buckets, 1) * 1000
				for _, c := range counts {
					s.GCPauses += c
				}
			}
		}
	}
	procStats(&s)
	return s
}

// delta returns the counts since the previous Read of the histogram.
func (r *Reader) delta(name string, counts []uint64) []uint64 {
	prev := r.prev[name]
	out := make([]uint64, len(counts))
	for i, c := range counts {
		if i < len(prev) && c >= prev[i] {
			out[i] = c - prev[i]
		} else {
			out[i] = c
		}
	}
	r.prev[name] = append(prev[:0], counts...)
	return out
}

// quantile returns the upper bound of the bucket holding quantile q of
// the counts (its lower bound for the last, unbounded bucket), 0 without
// counts.
func quantile(counts []uint64, buckets []float64, q float64) float64 {
	var total uint64
	for _, c := range counts {
		total += c
	}
	if total == 0 {
		return 0
	}
	rank := uint64(math.Ceil(q * float64(total)))
	if rank == 0 {
		rank = 1
	}
	var seen uint64
	for i, c := range counts {
		seen += c
		if seen >= rank {
			if hi := buckets[i+1]; !math.IsInf(hi, 1) {
				return hi
			}
			return buckets[i]
		}
	}
	return 0
}
