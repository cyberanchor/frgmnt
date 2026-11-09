// Package tracelog writes the structured trace of the engine
// (checker.TraceEvent) as JSON Lines, for the console's -trace-file and
// the panel's per-worker trace.
package tracelog

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/cyberanchor/frgmnt/internal/checker"
)

// Writer writes one event per line. Writes are buffered and flushed every
// second and at Close, so a crash loses at most the last second. A failed
// write does not stop the run; Close reports the first error. Safe for
// concurrent use: every worker of a run writes to it.
type Writer struct {
	mu       sync.Mutex
	f        *os.File
	w        *bufio.Writer
	max      int64 // bytes; 0 = no limit
	size     int64 // bytes in the file
	full     bool  // the limit was reached
	err      error
	events   int64
	dropped  int64
	stop     chan struct{}
	finished chan struct{}
}

// Open creates path with mode 0600 (a trace holds the URLs and the
// answers of a run) or appends to it. With maxBytes > 0 the file stops
// growing at about that size: one "trace_truncated" event is written and
// later events are only counted.
func Open(path string, appendTo bool, maxBytes int64) (*Writer, error) {
	flags := os.O_CREATE | os.O_WRONLY
	if appendTo {
		flags |= os.O_APPEND
	} else {
		flags |= os.O_TRUNC
	}
	f, err := os.OpenFile(path, flags, 0o600)
	if err != nil {
		return nil, err
	}
	var size int64
	if fi, err := f.Stat(); err == nil {
		size = fi.Size()
	}
	t := &Writer{f: f, w: bufio.NewWriterSize(f, 64*1024), max: maxBytes, size: size,
		stop: make(chan struct{}), finished: make(chan struct{})}
	go t.flusher()
	return t, nil
}

func (t *Writer) flusher() {
	defer close(t.finished)
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case <-t.stop:
			return
		case <-tick.C:
			t.mu.Lock()
			if err := t.w.Flush(); err != nil && t.err == nil {
				t.err = err
			}
			t.mu.Unlock()
		}
	}
}

// Write adds one event. It is the checker.Logger.Event of a run.
func (t *Writer) Write(e checker.TraceEvent) {
	if e.Time.IsZero() {
		e.Time = time.Now()
	}
	line, err := json.Marshal(e)
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.full {
		t.dropped++
		return
	}
	if err == nil && t.max > 0 && t.size+int64(len(line))+1 > t.max {
		t.full = true
		t.dropped++
		note, _ := json.Marshal(checker.TraceEvent{Time: time.Now(), Kind: "trace_truncated",
			Message: fmt.Sprintf("the trace reached its limit of %d MB; later events are not written", t.max>>20)})
		line = note
	}
	if err == nil {
		line = append(line, '\n')
		var n int
		n, err = t.w.Write(line)
		t.size += int64(n)
	}
	if err != nil {
		if t.err == nil {
			t.err = err
		}
		return
	}
	if !t.full {
		t.events++
	}
}

// Close flushes and closes the file. It returns how many events were
// written and dropped (over the limit), and the first error.
func (t *Writer) Close() (events, dropped int64, err error) {
	close(t.stop)
	<-t.finished
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.w.Flush(); err != nil && t.err == nil {
		t.err = err
	}
	if err := t.f.Close(); err != nil && t.err == nil {
		t.err = err
	}
	return t.events, t.dropped, t.err
}

// StatsInfo turns the engine's counters into the info of a run_end event.
func StatsInfo(e checker.Stats) map[string]any {
	return map[string]any{
		"checked": e.Checked, "free": e.Free, "reserved": e.Reserved, "auction": e.Auction,
		"available": e.Available, "taken": e.Taken, "sold": e.Sold, "unknown": e.Unknown,
		"errors": e.Errors, "retries": e.Retries, "rate_limited": e.RateLimit, "pauses": e.Pauses,
		"challenges": e.Challenges, "conn_new": e.ConnNew, "conn_reused": e.ConnReuse,
		"cache_hits": e.CacheHits, "cache_stale": e.CacheStale, "cache_misses": e.CacheMisses,
	}
}

// BlockedName is the "blocked" info of run_end: why the run stopped, or "".
func BlockedName(k checker.BlockKind) string {
	switch k {
	case checker.BlockedByChallenge:
		return "challenge"
	case checker.BlockedByThrottle:
		return "throttle"
	}
	return ""
}
