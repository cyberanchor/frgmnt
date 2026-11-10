package checker

import (
	"bytes"
	"context"
	"net/http"
	rpprof "runtime/pprof"
	rtrace "runtime/trace"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// In an execution trace every check is a task ("check") with the regions
// of its steps, so go tool trace shows where each name spent its time.
func TestExecTraceHasTasksAndRegions(t *testing.T) {
	pageServer(t)
	c := New(Config{Workers: 1, Timeout: 3 * time.Second, CacheDir: t.TempDir(), CacheTTL: time.Hour})
	defer c.Close()
	var buf bytes.Buffer
	if err := rtrace.Start(&buf); err != nil {
		t.Skipf("execution trace already running: %v", err)
	}
	_ = c.CheckOne(context.Background(), "sold3")
	_ = c.CheckOne(context.Background(), "free3")
	rtrace.Stop()
	for _, want := range []string{"check", "name", "gate", "rate", "http", "body", "parse", "cache-write", "drain", "cache"} {
		if !bytes.Contains(buf.Bytes(), []byte(want)) {
			t.Errorf("no %q in the execution trace (%d bytes)", want, buf.Len())
		}
	}
}

// The checks run under the pprof label component=check-worker (on top of
// the host's labels), so a profile tells them apart from the rest.
func TestCheckWorkersHavePprofLabels(t *testing.T) {
	release := make(chan struct{})
	var once sync.Once
	free := func() { once.Do(func() { close(release) }) }
	t.Cleanup(free)
	var arrived atomic.Bool
	withServer(t, func(w http.ResponseWriter, r *http.Request) {
		arrived.Store(true)
		<-release
		freeRedirect(w, r)
	})
	c := New(Config{Workers: 1, Timeout: 5 * time.Second})
	defer c.Close()
	in := make(chan string, 1)
	in <- "labelled1"
	close(in)
	out := make(chan Result, 1)
	done := make(chan struct{})
	ctx := rpprof.WithLabels(context.Background(), rpprof.Labels("tile", "t1"))
	go func() { c.Run(ctx, in, out); close(done) }()
	waitFor(t, "the request at the server", arrived.Load)
	var prof bytes.Buffer
	if err := rpprof.Lookup("goroutine").WriteTo(&prof, 1); err != nil {
		t.Fatal(err)
	}
	free()
	<-done
	if s := prof.String(); !strings.Contains(s, `"component":"check-worker"`) || !strings.Contains(s, `"tile":"t1"`) {
		t.Errorf("no goroutine with the labels in the profile:\n%.2000s", s)
	}
}
