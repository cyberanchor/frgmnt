package cli

import (
	"bufio"
	"encoding/json"
	"os"
	"sync"
	"time"

	"github.com/cyberanchor/frgmnt/internal/checker"
)

// traceFile writes the structured trace (-trace-file) as JSON Lines: one
// checker.TraceEvent per line, from every worker. Writes are buffered and
// flushed every second and at Close, so a crash loses at most the last
// second. A failed write does not stop the run; Close reports the first
// error.
type traceFile struct {
	mu     sync.Mutex
	f      *os.File
	w      *bufio.Writer
	err    error
	events int64
	stop   chan struct{}
	done   chan struct{}
}

// openTraceFile creates path (mode 0600: a trace holds the URLs and
// answers of the run), or appends to it when appendTo is set (-resume,
// -append).
func openTraceFile(path string, appendTo bool) (*traceFile, error) {
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
	t := &traceFile{f: f, w: bufio.NewWriterSize(f, 64*1024), stop: make(chan struct{}), done: make(chan struct{})}
	go t.flusher()
	return t, nil
}

func (t *traceFile) flusher() {
	defer close(t.done)
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

// write adds one event; it is the Logger.Event of the run.
func (t *traceFile) write(e checker.TraceEvent) {
	line, err := json.Marshal(e)
	t.mu.Lock()
	defer t.mu.Unlock()
	if err == nil {
		line = append(line, '\n')
		_, err = t.w.Write(line)
	}
	if err != nil {
		if t.err == nil {
			t.err = err
		}
		return
	}
	t.events++
}

// Close flushes and closes the file. It returns the first error of the
// run and how many events were written.
func (t *traceFile) Close() (int64, error) {
	close(t.stop)
	<-t.done
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.w.Flush(); err != nil && t.err == nil {
		t.err = err
	}
	if err := t.f.Close(); err != nil && t.err == nil {
		t.err = err
	}
	return t.events, t.err
}

// statsInfo turns the engine's counters into the info of run_end.
func statsInfo(e checker.Stats) map[string]any {
	return map[string]any{
		"checked": e.Checked, "free": e.Free, "reserved": e.Reserved, "auction": e.Auction,
		"available": e.Available, "taken": e.Taken, "sold": e.Sold, "unknown": e.Unknown,
		"errors": e.Errors, "retries": e.Retries, "rate_limited": e.RateLimit, "pauses": e.Pauses,
		"challenges": e.Challenges, "conn_new": e.ConnNew, "conn_reused": e.ConnReuse,
		"cache_hits": e.CacheHits, "cache_stale": e.CacheStale, "cache_misses": e.CacheMisses,
	}
}
