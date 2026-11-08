package worker

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cyberanchor/frgmnt/internal/checker"
	"github.com/cyberanchor/frgmnt/internal/logbus"
	"github.com/cyberanchor/frgmnt/internal/store"
)

// fakeFragment answers every username with "free" (302 to /?query=) after
// an optional delay and counts how many times each name was requested.
type fakeFragment struct {
	srv   *httptest.Server
	delay atomic.Int64 // nanoseconds
	mu    sync.Mutex
	hits  map[string]int
}

func newFakeFragment(t *testing.T, delay time.Duration) *fakeFragment {
	t.Helper()
	f := &fakeFragment{hits: map[string]int{}}
	f.delay.Store(int64(delay))
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(r.URL.Path, "/username/")
		f.mu.Lock()
		f.hits[name]++
		f.mu.Unlock()
		if d := time.Duration(f.delay.Load()); d > 0 {
			select {
			case <-time.After(d):
			case <-r.Context().Done():
				return
			}
		}
		http.Redirect(w, r, "/?query="+name, http.StatusFound)
	}))
	old := checker.FragmentURLTemplate
	checker.FragmentURLTemplate = f.srv.URL + "/username/%s"
	t.Cleanup(func() {
		checker.FragmentURLTemplate = old
		f.srv.Close()
	})
	return f
}

func (f *fakeFragment) hitsFor(name string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.hits[name]
}

// harness is a Manager on a fresh store with the page cache off, plus a
// counter of ERROR events published on the bus.
type harness struct {
	st   *store.Store
	bus  *logbus.Bus
	m    *Manager
	errs atomic.Int64
	last atomic.Value // last ERROR message
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	h := &harness{st: st, bus: logbus.New(5000, logbus.LevelInfo)}
	ch, cancel := h.bus.Subscribe("", 4096)
	go func() {
		for e := range ch {
			if e.Level == "ERROR" {
				h.errs.Add(1)
				h.last.Store(e.Message)
			}
		}
	}()
	h.m = New(st, h.bus)
	h.m.ConfigureCache("", 0, 0, true)
	t.Cleanup(func() {
		h.m.CloseAll()
		cancel()
		_ = st.Close()
	})
	return h
}

func (h *harness) lastError() string {
	s, _ := h.last.Load().(string)
	return s
}

func testNames(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("name%05d", i)
	}
	return out
}

func testConfig(workers int) checker.Config {
	cfg := checker.DefaultConfig()
	cfg.Workers = workers
	cfg.Retries = 0
	return cfg
}

func waitNotRunning(w *Worker, d time.Duration) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if w.Status() != StatusRunning {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}

// persistedCounts returns (names flagged checked, result rows) of a worker.
func persistedCounts(t *testing.T, st *store.Store, id string) (checked, rows int64) {
	t.Helper()
	ctx := context.Background()
	total, err := st.InputCount(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	pending, err := st.PendingInputCount(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	counts, err := st.CountByStatus(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range counts {
		rows += c
	}
	return total - pending, rows
}

// The process used to stop like this on SIGINT: cancel the workers, close
// the database right away. The "checked" flags were written one by one
// before their batch of rows, so names ended up flagged without a result
// and Resume never checked them again.
func TestAbruptShutdownKeepsFlagsAndResultsConsistent(t *testing.T) {
	newFakeFragment(t, 100*time.Millisecond) // ~100 names/s: the run is mid-way at shutdown
	dir := t.TempDir()
	st, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	m := New(st, logbus.New(1000, logbus.LevelInfo))
	m.ConfigureCache("", 0, 0, true)
	ctx := context.Background()
	w, err := m.Create(ctx, "shutdown", testConfig(10))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.SetInput(ctx, w.ID(), testNames(400)); err != nil {
		t.Fatal(err)
	}
	if err := m.Start(ctx, w.ID(), StartOptions{}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1200 * time.Millisecond)

	for _, wk := range m.List() {
		wk.mu.RLock()
		cancel := wk.cancel
		wk.mu.RUnlock()
		cancel()
	}
	_ = st.Close() // the database goes away while the run is still flushing
	waitNotRunning(w, 10*time.Second)

	st2, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	checked, rows := persistedCounts(t, st2, w.ID())
	t.Logf("after shutdown: %d names flagged checked, %d result rows", checked, rows)
	if checked != rows {
		t.Errorf("%d names flagged checked, %d result rows: Resume would skip names without a result", checked, rows)
	}
}

// The producer kept the only SQLite connection busy with an open cursor
// while it waited for room in the 1024-name input channel, and the result
// writer needs that connection. A list longer than the channel buffers
// (about 2,070 names) therefore stopped dead: 3,000 names hung at ~1,050
// checked, batches were dropped on the 5 s write timeout, and every API call
// that touched the database timed out too.
func TestLargeListCompletes(t *testing.T) {
	newFakeFragment(t, 0)
	h := newHarness(t)
	ctx := context.Background()
	w, err := h.m.Create(ctx, "large", testConfig(20))
	if err != nil {
		t.Fatal(err)
	}
	const n = 3000
	if _, err := h.m.SetInput(ctx, w.ID(), testNames(n)); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if err := h.m.Start(ctx, w.ID(), StartOptions{}); err != nil {
		t.Fatal(err)
	}

	// The API must stay responsive while the run is going.
	time.Sleep(300 * time.Millisecond)
	apiCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	_, apiErr := h.st.CountByStatus(apiCtx, w.ID())
	cancel()
	if apiErr != nil {
		t.Errorf("database query during the run: %v", apiErr)
	}

	if !waitNotRunning(w, 60*time.Second) {
		_ = h.m.Stop(w.ID())
		t.Fatalf("a %d-name list did not finish in 60s (checked %d)", n, w.Stats().Checked)
	}
	checked, rows := persistedCounts(t, h.st, w.ID())
	t.Logf("%d names in %v: %d flagged checked, %d result rows, %d ERROR events",
		n, time.Since(start).Round(time.Millisecond), checked, rows, h.errs.Load())
	if rows != n || checked != n {
		t.Errorf("persisted %d rows and %d flags, want %d (last error: %q)", rows, checked, n, h.lastError())
	}
	if w.Status() != StatusDone {
		t.Errorf("status = %s, want done", w.Status())
	}
}

// Stop stored every check that was in flight as "error (cancelled)" and
// flagged the name done, so Resume never checked those names again.
func TestStopThenResumeChecksInterruptedNames(t *testing.T) {
	fake := newFakeFragment(t, 400*time.Millisecond)
	h := newHarness(t)
	ctx := context.Background()
	w, err := h.m.Create(ctx, "stopper", testConfig(20))
	if err != nil {
		t.Fatal(err)
	}
	all := testNames(200)
	if _, err := h.m.SetInput(ctx, w.ID(), all); err != nil {
		t.Fatal(err)
	}
	if err := h.m.Start(ctx, w.ID(), StartOptions{}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1000 * time.Millisecond) // two waves done, 20 requests in flight
	if err := h.m.Stop(w.ID()); err != nil {
		t.Fatal(err)
	}
	if !waitNotRunning(w, 15*time.Second) {
		t.Fatal("worker did not stop")
	}
	checked, rows := persistedCounts(t, h.st, w.ID())
	t.Logf("after Stop: %d result rows, %d names flagged checked", rows, checked)

	fake.delay.Store(0)
	if err := h.m.Start(ctx, w.ID(), StartOptions{Resume: true}); err != nil {
		t.Fatal(err)
	}
	if !waitNotRunning(w, 30*time.Second) {
		t.Fatal("resume did not finish")
	}
	rs, err := h.st.ListResults(ctx, w.ID(), 0, 10000)
	if err != nil {
		t.Fatal(err)
	}
	perName := map[string]int{}
	for _, r := range rs {
		perName[r.Username]++
		if r.Error == "cancelled" {
			t.Errorf("%s was stored as 'error (cancelled)'", r.Username)
		}
	}
	missing := 0
	for _, n := range all {
		if perName[n] != 1 {
			missing++
		}
	}
	if missing > 0 {
		t.Errorf("after Stop + Resume, %d of %d names have no result (or more than one)", missing, len(all))
	}
}

// Start writes w.cancel under w.mu while Stop, Delete and CloseAll read it
// without the lock: a data race that `go test -race` reports. Run with -race.
func TestStartStopConcurrentNoRace(t *testing.T) {
	newFakeFragment(t, 50*time.Millisecond)
	h := newHarness(t)
	ctx := context.Background()
	w, err := h.m.Create(ctx, "racy", testConfig(2))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.m.SetInput(ctx, w.ID(), testNames(20)); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(3)
	go func() { defer wg.Done(); _ = h.m.Start(ctx, w.ID(), StartOptions{}) }()
	go func() { defer wg.Done(); _ = h.m.Stop(w.ID()) }()
	go func() { defer wg.Done(); h.m.CloseAll() }()
	wg.Wait()
	_ = h.m.Stop(w.ID())
	waitNotRunning(w, 10*time.Second)
}
