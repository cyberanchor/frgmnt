// Package profiling gives the console and the panel pprof: the live
// endpoints of net/http/pprof (on a mux of their own, never on
// http.DefaultServeMux), CPU and heap profiles written to files, and the
// execution trace of runtime/trace with a size limit.
package profiling

import (
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"path/filepath"
	"runtime"
	rpprof "runtime/pprof"
	rtrace "runtime/trace"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cyberanchor/frgmnt/internal/tracelog"
)

// Handler serves the pprof endpoints under /debug/pprof/: the index, the
// named profiles (heap, allocs, goroutine, block, mutex, threadcreate),
// cmdline, profile (CPU, ?seconds=N), symbol and trace (the execution
// trace, ?seconds=N).
func Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return mux
}

// EnableContention turns on the block and mutex profiles: on average one
// blocking event per millisecond spent blocked, and one mutex contention
// in a hundred. They cost a little on every block, so they come with
// -pprof only.
func EnableContention() {
	runtime.SetBlockProfileRate(int(time.Millisecond))
	runtime.SetMutexProfileFraction(100)
}

// IsLoopback reports whether addr ("host:port") listens on this machine
// only.
func IsLoopback(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// Serve starts a pprof server on addr. It returns the address it listens
// on (addr may end in :0) and a function that stops it.
func Serve(addr string) (net.Addr, func(), error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, nil, err
	}
	// No write timeout: /debug/pprof/profile and trace take ?seconds=N.
	srv := &http.Server{Handler: Handler(), ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = srv.Serve(ln) }()
	return ln.Addr(), func() { _ = srv.Close() }, nil
}

// StartCPU writes a CPU profile of everything until stop to path.
func StartCPU(path string) (stop func() error, err error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, err
	}
	if err := rpprof.StartCPUProfile(f); err != nil {
		f.Close()
		return nil, err
	}
	return func() error {
		rpprof.StopCPUProfile()
		return f.Close()
	}, nil
}

// WriteHeap writes the heap profile to path after a garbage collection.
// It holds what is live (inuse_*) and everything allocated since the start
// (alloc_*: go tool pprof -sample_index=alloc_space).
func WriteHeap(path string) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	runtime.GC()
	if err := rpprof.Lookup("heap").WriteTo(f, 0); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// ExecTrace is a running execution trace (runtime/trace): every goroutine
// switch, syscall, network wait and GC, with the tasks and regions of the
// checks. It is large (megabytes a second under load), so it stops by
// itself at maxBytes or when the disk has less than minFree left.
type ExecTrace struct {
	f        *os.File
	path     string
	max      int64
	minFree  int64
	n        atomic.Int64
	stopping atomic.Bool
	stopOnce sync.Once
	stopped  chan struct{}
	why      atomic.Pointer[string]
	lastFree time.Time
}

// StartExecTrace starts the execution trace into path; 0 = no limit.
func StartExecTrace(path string, maxBytes, minFree int64) (*ExecTrace, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, err
	}
	t := &ExecTrace{f: f, path: path, max: maxBytes, minFree: minFree, stopped: make(chan struct{})}
	if err := rtrace.Start(t); err != nil {
		f.Close()
		return nil, err
	}
	return t, nil
}

// Write is the sink of runtime/trace. Past the limit it asks for a stop:
// trace.Stop waits for this writer, so it runs on another goroutine, and
// the bytes until then are still written (cutting the stream would make
// the file unreadable).
func (t *ExecTrace) Write(p []byte) (int, error) {
	n, err := t.f.Write(p)
	total := t.n.Add(int64(n))
	if t.max > 0 && total >= t.max {
		t.stopAsync(fmt.Sprintf("reached its limit of %d MB", t.max>>20))
	} else if t.minFree > 0 && time.Since(t.lastFree) > time.Second {
		t.lastFree = time.Now()
		if free, ok := tracelog.FreeSpace(filepath.Dir(t.path)); ok && free < t.minFree {
			t.stopAsync(fmt.Sprintf("less than %d MB free on the disk", t.minFree>>20))
		}
	}
	return n, err
}

func (t *ExecTrace) stopAsync(why string) {
	if t.stopping.CompareAndSwap(false, true) {
		t.why.Store(&why)
		go t.stop()
	}
}

func (t *ExecTrace) stop() {
	t.stopOnce.Do(func() {
		rtrace.Stop()
		close(t.stopped)
	})
}

// Stop ends the trace (if it did not stop by itself) and closes the file.
// stoppedEarly tells why it ended before Stop, or "".
func (t *ExecTrace) Stop() (bytes int64, stoppedEarly string, err error) {
	t.stopping.Store(true)
	t.stop()
	<-t.stopped
	if p := t.why.Load(); p != nil {
		stoppedEarly = *p
	}
	return t.n.Load(), stoppedEarly, t.f.Close()
}
