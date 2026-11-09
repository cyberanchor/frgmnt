package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
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

// On SIGINT/SIGTERM the web mode calls CloseAll and then closes the
// database. CloseAll only cancelled the runs and returned at once, so the
// result writer's last flush hit a closed database ("persist: sql: database
// is closed") and the last results of every run were lost.
func TestCloseAllWaitsForRunningWorkers(t *testing.T) {
	newFakeFragment(t, 100*time.Millisecond)
	dir := t.TempDir()
	st, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	bus := logbus.New(1000, logbus.LevelInfo)
	var errs atomic.Int64
	var lastErr atomic.Value
	ch, unsubscribe := bus.Subscribe("", 1024)
	defer unsubscribe()
	go func() {
		for e := range ch {
			if e.Level == "ERROR" {
				errs.Add(1)
				lastErr.Store(e.Message)
			}
		}
	}()
	m := New(st, bus)
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

	m.CloseAll()
	stillRunning := w.Status() == StatusRunning
	verdicts := w.Stats().Checked
	_ = st.Close()                     // the deferred Close in webapp.Main
	time.Sleep(300 * time.Millisecond) // a late writer would fail now
	waitNotRunning(w, 10*time.Second)

	st2, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	checked, rows := persistedCounts(t, st2, w.ID())
	last, _ := lastErr.Load().(string)
	t.Logf("CloseAll: running=%v, %d verdicts, %d rows saved, %d flags, %d ERROR events (last %q)",
		stillRunning, verdicts, rows, checked, errs.Load(), last)
	if stillRunning {
		t.Error("CloseAll returned while the worker was still running")
	}
	if rows != verdicts || checked != rows {
		t.Errorf("%d verdicts, but %d rows and %d flags saved", verdicts, rows, checked)
	}
	if errs.Load() > 0 {
		t.Errorf("%d ERROR events during shutdown (last %q)", errs.Load(), last)
	}
}

// Delete removed the rows while the cancelled run was still writing.
func TestDeleteWaitsForTheRun(t *testing.T) {
	newFakeFragment(t, 100*time.Millisecond)
	h := newHarness(t)
	ctx := context.Background()
	w, err := h.m.Create(ctx, "doomed", testConfig(10))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.m.SetInput(ctx, w.ID(), testNames(400)); err != nil {
		t.Fatal(err)
	}
	if err := h.m.Start(ctx, w.ID(), StartOptions{}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(700 * time.Millisecond)
	if err := h.m.Delete(ctx, w.ID()); err != nil {
		t.Fatal(err)
	}
	if w.Status() == StatusRunning {
		t.Error("Delete returned while the run was still going")
	}
	time.Sleep(300 * time.Millisecond)
	if h.errs.Load() > 0 {
		t.Errorf("ERROR events after Delete: %d (last %q)", h.errs.Load(), h.lastError())
	}
}

// A worker blocked by Cloudflare (every answer HTTP 403 with
// cf-mitigated: challenge) went through the whole list in seconds, stored
// an error for every name, flagged them all checked and ended "done", so
// Resume had nothing left to do. Now the run stops with status error, and
// Resume checks every name without a verdict, the errors included.
func TestBlockedRunStopsAndResumeRechecks(t *testing.T) {
	var blocked atomic.Bool
	blocked.Store(true)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if blocked.Load() {
			w.Header().Set("Cf-Mitigated", "challenge")
			w.WriteHeader(http.StatusForbidden)
			return
		}
		name := strings.TrimPrefix(r.URL.Path, "/username/")
		http.Redirect(w, r, "/?query="+name, http.StatusFound)
	}))
	old := checker.FragmentURLTemplate
	checker.FragmentURLTemplate = srv.URL + "/username/%s"
	t.Cleanup(func() { checker.FragmentURLTemplate = old; srv.Close() })

	h := newHarness(t)
	ctx := context.Background()
	w, err := h.m.Create(ctx, "blocked", testConfig(4))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.m.SetInput(ctx, w.ID(), testNames(300)); err != nil {
		t.Fatal(err)
	}
	if err := h.m.Start(ctx, w.ID(), StartOptions{}); err != nil {
		t.Fatal(err)
	}
	if !waitNotRunning(w, 10*time.Second) {
		t.Fatal("blocked run did not stop")
	}
	if st := w.Status(); st != StatusError {
		t.Errorf("status after the block: %s, want error", st)
	}
	if !strings.Contains(h.lastError(), "Cloudflare challenge") {
		t.Errorf("last error log: %q, want the Cloudflare block explained", h.lastError())
	}
	checked, rows := persistedCounts(t, h.st, w.ID())
	if checked >= 300 || rows >= 300 {
		t.Errorf("blocked run: %d names checked, %d result rows; want it to stop early", checked, rows)
	}
	if n, _ := h.m.Pending(ctx, w.ID()); n != 300 {
		t.Errorf("Pending after the block = %d, want 300 (errors count as pending)", n)
	}

	blocked.Store(false)
	if err := h.m.Start(ctx, w.ID(), StartOptions{Resume: true}); err != nil {
		t.Fatal(err)
	}
	if !waitNotRunning(w, 20*time.Second) {
		t.Fatal("resume did not finish")
	}
	counts, err := h.st.CountByStatus(ctx, w.ID())
	if err != nil {
		t.Fatal(err)
	}
	if w.Status() != StatusDone || counts["free"] != 300 || counts["error"] != 0 {
		t.Errorf("after Resume: status %s, counts %v; want done with 300 free and no errors", w.Status(), counts)
	}
}

// The tile shows Stats().Checked of Stats().Total. Checked counted only
// the current run, so after Stop and Resume a finished list of 40 names
// showed "32/40 = 80%" with status done.
func TestTileProgressAddsUpOverResume(t *testing.T) {
	newFakeFragment(t, 50*time.Millisecond)
	h := newHarness(t)
	ctx := context.Background()
	w, err := h.m.Create(ctx, "resume", testConfig(2))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.m.SetInput(ctx, w.ID(), testNames(40)); err != nil {
		t.Fatal(err)
	}
	if err := h.m.Start(ctx, w.ID(), StartOptions{}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for w.Stats().Checked < 6 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	_ = h.m.Stop(w.ID())
	waitNotRunning(w, 10*time.Second)
	if err := h.m.Start(ctx, w.ID(), StartOptions{Resume: true}); err != nil {
		t.Fatal(err)
	}
	if !waitNotRunning(w, 20*time.Second) {
		t.Fatal("resume did not finish")
	}
	st := w.Stats()
	if w.Status() != StatusDone || st.Checked != 40 || st.Total != 40 || st.Free != 40 {
		t.Errorf("after Stop and Resume: status %s, checked %d of %d, free %d; want done 40/40, 40 free",
			w.Status(), st.Checked, st.Total, st.Free)
	}
}

// After a restart every tile showed 0/N and zero counters although the
// results were in the database, and a finished worker came back "ready".
func TestProgressSurvivesRestart(t *testing.T) {
	newFakeFragment(t, 0)
	h := newHarness(t)
	ctx := context.Background()
	w, err := h.m.Create(ctx, "restart", testConfig(4))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.m.SetInput(ctx, w.ID(), testNames(18)); err != nil {
		t.Fatal(err)
	}
	if err := h.m.Start(ctx, w.ID(), StartOptions{}); err != nil {
		t.Fatal(err)
	}
	if !waitNotRunning(w, 10*time.Second) {
		t.Fatal("run did not finish")
	}

	m2 := New(h.st, h.bus) // the process restarts on the same database
	m2.ConfigureCache("", 0, 0, true)
	if err := m2.Load(ctx); err != nil {
		t.Fatal(err)
	}
	w2, err := m2.Get(w.ID())
	if err != nil {
		t.Fatal(err)
	}
	st := w2.Stats()
	if w2.Status() != StatusDone || st.Checked != 18 || st.Total != 18 || st.Free != 18 {
		t.Errorf("after a restart: status %s, checked %d of %d, free %d; want done 18/18, 18 free",
			w2.Status(), st.Checked, st.Total, st.Free)
	}
}

// Loading a new list into a worker kept the results of the old one, so a
// Resume mixed both lists (and the download listed names that are not in
// the list any more).
func TestNewListThenResumeChecksOnlyNewNames(t *testing.T) {
	f := newFakeFragment(t, 0)
	h := newHarness(t)
	ctx := context.Background()
	w, err := h.m.Create(ctx, "lists", testConfig(2))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.m.SetInput(ctx, w.ID(), []string{"name1", "name2", "name3", "name4", "name5"}); err != nil {
		t.Fatal(err)
	}
	if err := h.m.Start(ctx, w.ID(), StartOptions{}); err != nil {
		t.Fatal(err)
	}
	waitNotRunning(w, 10*time.Second)
	if _, err := h.m.SetInput(ctx, w.ID(), []string{"name4", "name5", "name6", "name7"}); err != nil {
		t.Fatal(err)
	}
	if err := h.m.Start(ctx, w.ID(), StartOptions{Resume: true}); err != nil {
		t.Fatal(err)
	}
	waitNotRunning(w, 10*time.Second)
	rs, err := h.st.ListResults(ctx, w.ID(), 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, r := range rs {
		got[r.Username] = true
	}
	if len(rs) != 4 || !got["name4"] || !got["name5"] || !got["name6"] || !got["name7"] {
		t.Errorf("results after a new list and Resume: %v, want name4..name7 once each", got)
	}
	for _, n := range []string{"name4", "name5"} {
		if c := f.hitsFor(n); c != 1 {
			t.Errorf("%s (kept from the old list) requested %d times, want 1", n, c)
		}
	}
	if st := w.Stats(); w.Status() != StatusDone || st.Checked != 4 || st.Total != 4 {
		t.Errorf("status %s, %d/%d, want done 4/4", w.Status(), st.Checked, st.Total)
	}
}

// Clear then Resume: "resume: nothing left to do", and the worker stayed
// without any results.
func TestClearThenResumeChecksAgain(t *testing.T) {
	newFakeFragment(t, 0)
	h := newHarness(t)
	ctx := context.Background()
	w, err := h.m.Create(ctx, "clear", testConfig(2))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.m.SetInput(ctx, w.ID(), testNames(6)); err != nil {
		t.Fatal(err)
	}
	if err := h.m.Start(ctx, w.ID(), StartOptions{}); err != nil {
		t.Fatal(err)
	}
	waitNotRunning(w, 10*time.Second)
	if err := h.m.ClearResults(ctx, w.ID()); err != nil {
		t.Fatal(err)
	}
	if st := w.Stats(); w.Status() != StatusReady || st.Checked != 0 {
		t.Errorf("after Clear: status %s, checked %d; want ready, 0", w.Status(), st.Checked)
	}
	if err := h.m.Start(ctx, w.ID(), StartOptions{Resume: true}); err != nil {
		t.Fatal(err)
	}
	waitNotRunning(w, 10*time.Second)
	if _, rows := persistedCounts(t, h.st, w.ID()); rows != 6 {
		t.Errorf("after Clear and Resume: %d results, want 6", rows)
	}
}

// Storing a big list takes seconds. A Start in that time used to pass the
// status check and begin a run that read the list while it was replaced;
// a second upload could interleave the same way. Both are refused now
// until the list is stored.
func TestStartRefusedWhileListIsStored(t *testing.T) {
	newFakeFragment(t, 0)
	h := newHarness(t)
	ctx := context.Background()
	w, err := h.m.Create(ctx, "loader", testConfig(2))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.m.SetInput(ctx, w.ID(), testNames(3)); err != nil {
		t.Fatal(err)
	}
	inside := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := h.m.SetInputFrom(ctx, w.ID(), func(add func(string) error) error {
			if err := add("first"); err != nil {
				return err
			}
			close(inside)
			<-release
			return add("second")
		})
		done <- err
	}()
	<-inside
	if err := h.m.Start(ctx, w.ID(), StartOptions{}); err != ErrWorkerBusy {
		t.Errorf("Start while a list is stored: %v, want ErrWorkerBusy", err)
	}
	if _, err := h.m.SetInput(ctx, w.ID(), testNames(5)); err != ErrWorkerBusy {
		t.Errorf("second list while one is stored: %v, want ErrWorkerBusy", err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if n, _ := h.st.InputCount(ctx, w.ID()); n != 2 {
		t.Errorf("%d names stored, want 2", n)
	}
	if err := h.m.Start(ctx, w.ID(), StartOptions{}); err != nil {
		t.Fatalf("Start after the list was stored: %v", err)
	}
	if !waitNotRunning(w, 10*time.Second) {
		t.Fatal("run did not finish")
	}
}

// While one tile stores a big list, the database is busy for seconds (a
// shuffled list of 500,000 names takes ~12 s). The result writer of a
// running tile gave up after 5 s and dropped its batch: the results were
// lost and the names were checked again on the next Resume. Now it keeps
// the batch and writes it once the database is free.
func TestResultsSurviveABusyDatabase(t *testing.T) {
	fake := newFakeFragment(t, 20*time.Millisecond)
	h := newHarness(t)
	ctx := context.Background()
	run, err := h.m.Create(ctx, "running", testConfig(4))
	if err != nil {
		t.Fatal(err)
	}
	names := testNames(300)
	if _, err := h.m.SetInput(ctx, run.ID(), names); err != nil {
		t.Fatal(err)
	}
	loader, err := h.m.Create(ctx, "loader", testConfig(1))
	if err != nil {
		t.Fatal(err)
	}
	if err := h.m.Start(ctx, run.ID(), StartOptions{}); err != nil {
		t.Fatal(err)
	}
	// Hold the database inside the upload transaction for 6 s once the
	// run is under way.
	for deadline := time.Now().Add(5 * time.Second); fake.hitsFor(names[20]) == 0 && time.Now().Before(deadline); {
		time.Sleep(5 * time.Millisecond)
	}
	held := make(chan error, 1)
	go func() {
		_, err := h.m.SetInputFrom(ctx, loader.ID(), func(add func(string) error) error {
			time.Sleep(6 * time.Second)
			return add("loadedname")
		})
		held <- err
	}()
	if !waitNotRunning(run, 30*time.Second) {
		t.Fatal("run did not finish")
	}
	if err := <-held; err != nil {
		t.Fatal(err)
	}
	checked, rows := persistedCounts(t, h.st, run.ID())
	if rows != 300 || checked != 300 {
		t.Errorf("%d result rows and %d names flagged checked, want 300 and 300 (last error: %q)", rows, checked, h.lastError())
	}
}

// The per-result debug event is built only when the bus passes debug
// entries on; at info it cost 2.7 µs and 24 allocations per result and
// was dropped right away.
func TestResultEventsOnlyAtDebugLevel(t *testing.T) {
	newFakeFragment(t, 0)
	h := newHarness(t)
	ctx := context.Background()
	events, cancel := h.bus.Subscribe("", 1024)
	defer cancel()
	var seen atomic.Int64
	go func() {
		for e := range events {
			if strings.Contains(e.Message, `"event":"result"`) {
				seen.Add(1)
			}
		}
	}()
	w, err := h.m.Create(ctx, "events", testConfig(2))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.m.SetInput(ctx, w.ID(), testNames(5)); err != nil {
		t.Fatal(err)
	}
	runOnce := func(resume bool) {
		t.Helper()
		if err := h.m.Start(ctx, w.ID(), StartOptions{Resume: resume}); err != nil {
			t.Fatal(err)
		}
		if !waitNotRunning(w, 10*time.Second) {
			t.Fatal("run did not finish")
		}
		time.Sleep(50 * time.Millisecond) // let the subscriber catch up
	}
	runOnce(false)
	if n := seen.Load(); n != 0 {
		t.Errorf("%d result events at info level, want 0", n)
	}
	h.bus.SetLevel(logbus.LevelDebug)
	runOnce(false)
	if n := seen.Load(); n != 5 {
		t.Errorf("%d result events at debug level, want 5", n)
	}
}

// A tile that Fragment keeps throttling stops with the error status and a
// log line that says so, keeps every name pending, and its warnings (the
// pauses) reach the log: the panel set no logger for the engine before.
func TestThrottledRunStopsWithItsOwnMessage(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/", http.StatusFound) // every name and the probe
	}))
	old := checker.FragmentURLTemplate
	checker.FragmentURLTemplate = srv.URL + "/username/%s"
	t.Cleanup(func() { checker.FragmentURLTemplate = old; srv.Close() })

	h := newHarness(t)
	var warns atomic.Int64
	events, cancel := h.bus.Subscribe("", 1024)
	defer cancel()
	go func() {
		for e := range events {
			if e.Level == "WARN" && strings.Contains(e.Message, "throttling") {
				warns.Add(1)
			}
		}
	}()
	ctx := context.Background()
	cfg := testConfig(4)
	cfg.BlockCooldown = 20 * time.Millisecond
	cfg.ThrottleMax = 300 * time.Millisecond
	w, err := h.m.Create(ctx, "throttled", cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.m.SetInput(ctx, w.ID(), testNames(40)); err != nil {
		t.Fatal(err)
	}
	if err := h.m.Start(ctx, w.ID(), StartOptions{}); err != nil {
		t.Fatal(err)
	}
	if !waitNotRunning(w, 10*time.Second) {
		t.Fatal("throttled run did not stop")
	}
	if st := w.Status(); st != StatusError {
		t.Errorf("status: %s, want error", st)
	}
	if !strings.Contains(h.lastError(), "kept throttling") {
		t.Errorf("last error log: %q, want the throttling explained", h.lastError())
	}
	if checked, rows := persistedCounts(t, h.st, w.ID()); checked != 0 || rows != 0 {
		t.Errorf("%d names checked, %d rows; a throttled name must stay pending", checked, rows)
	}
	time.Sleep(50 * time.Millisecond)
	if warns.Load() == 0 {
		t.Error("no throttle warning reached the log")
	}
}

// The panel gave the engine a logger with warnings only, so the debug and
// trace levels of the Logs tab showed no HTTP detail. The engine's debug
// and trace lines reach the tile's log now, and follow the level of the
// bus when it changes: nothing at info, a line per try at debug, every
// step at trace.
func TestEngineDetailFollowsTheLogLevel(t *testing.T) {
	newFakeFragment(t, 0)
	h := newHarness(t)
	ctx := context.Background()
	var mu sync.Mutex
	lines := map[string][]string{} // level -> engine lines
	events, cancel := h.bus.Subscribe("", 4096)
	defer cancel()
	go func() {
		for e := range events {
			if strings.Contains(e.Message, " #1") || strings.Contains(e.Message, "probe ") {
				mu.Lock()
				lines[e.Level] = append(lines[e.Level], e.Message)
				mu.Unlock()
			}
		}
	}()
	w, err := h.m.Create(ctx, "levels", testConfig(1))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.m.SetInput(ctx, w.ID(), testNames(2)); err != nil {
		t.Fatal(err)
	}
	run := func() {
		t.Helper()
		if err := h.m.Start(ctx, w.ID(), StartOptions{}); err != nil {
			t.Fatal(err)
		}
		if !waitNotRunning(w, 10*time.Second) {
			t.Fatal("run did not finish")
		}
		time.Sleep(50 * time.Millisecond)
	}
	count := func(level string) int {
		mu.Lock()
		defer mu.Unlock()
		return len(lines[level])
	}
	run()
	if n := count("DEBUG") + count("TRACE"); n != 0 {
		t.Errorf("%d engine lines at info, want none", n)
	}
	h.bus.SetLevel(logbus.LevelDebug)
	run()
	if count("DEBUG") == 0 || count("TRACE") != 0 {
		t.Errorf("at debug: %d debug and %d trace lines, want debug only", count("DEBUG"), count("TRACE"))
	}
	mu.Lock()
	joined := strings.Join(lines["DEBUG"], "\n")
	mu.Unlock()
	if !strings.Contains(joined, "#1 | -> free | 302 HTTP/1.1 |") || !strings.Contains(joined, "net=") {
		t.Errorf("debug lines lack the per-try timing line:\n%s", joined)
	}
	h.bus.SetLevel(logbus.LevelTrace)
	run()
	if count("TRACE") == 0 {
		t.Error("no trace lines at the trace level")
	}
}

// A worker whose config has trace on writes the structured trace of its
// runs to <data>/traces/<id>.jsonl: a fresh start begins the file again,
// Resume appends, deleting the worker deletes it, and the file stops at
// maxTraceBytes with a trace_truncated line.
func TestTileTraceFile(t *testing.T) {
	newFakeFragment(t, 0)
	h := newHarness(t)
	ctx := context.Background()
	cfg := testConfig(2)
	cfg.Trace = true
	w, err := h.m.Create(ctx, "traced", cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.m.SetInput(ctx, w.ID(), testNames(5)); err != nil {
		t.Fatal(err)
	}
	run := func(resume bool) {
		t.Helper()
		if err := h.m.Start(ctx, w.ID(), StartOptions{Resume: resume}); err != nil {
			t.Fatal(err)
		}
		if !waitNotRunning(w, 10*time.Second) {
			t.Fatal("run did not finish")
		}
	}
	kinds := func() map[string]int {
		t.Helper()
		raw, err := os.ReadFile(h.m.TracePath(w.ID()))
		if err != nil {
			t.Fatal(err)
		}
		out := map[string]int{}
		for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
			var e checker.TraceEvent
			if err := json.Unmarshal([]byte(line), &e); err != nil {
				t.Fatalf("not JSON: %q", line)
			}
			out[e.Kind]++
			if e.Kind == "run_start" && e.Info["worker"] != w.ID() {
				t.Errorf("run_start info %v", e.Info)
			}
		}
		return out
	}
	run(false)
	if k := kinds(); k["run_start"] != 1 || k["run_end"] != 1 || k["attempt"] != 5 || k["result"] != 5 {
		t.Errorf("first run: %v", k)
	}
	if fi, err := os.Stat(h.m.TracePath(w.ID())); err != nil || fi.Mode().Perm() != 0o600 {
		t.Errorf("trace file: %v %v", err, fi.Mode().Perm())
	}
	run(false) // a fresh start: a new file
	if k := kinds(); k["run_start"] != 1 {
		t.Errorf("after a fresh start: %d run_start, want 1", k["run_start"])
	}
	// Two more names: the five checked keep their results, Resume checks
	// the new ones and appends to the trace.
	if _, err := h.m.SetInput(ctx, w.ID(), testNames(7)); err != nil {
		t.Fatal(err)
	}
	run(true)
	if k := kinds(); k["run_start"] != 2 || k["result"] != 7 {
		t.Errorf("after Resume: %v, want 2 run_start and 5+2 results (appended)", k)
	}

	old := maxTraceBytes
	maxTraceBytes = 2 << 10
	t.Cleanup(func() { maxTraceBytes = old })
	if _, err := h.m.SetInput(ctx, w.ID(), testNames(40)); err != nil {
		t.Fatal(err)
	}
	run(false)
	if k := kinds(); k["trace_truncated"] != 1 || k["run_end"] != 0 {
		t.Errorf("over the limit: %v, want one trace_truncated and nothing after it", k)
	}

	if err := h.m.Delete(ctx, w.ID()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(h.m.TracePath(w.ID())); !os.IsNotExist(err) {
		t.Errorf("trace file after Delete: %v, want gone", err)
	}
}

// The rows a run saves carry the split of the time and the tries.
func TestSavedResultsCarryTheTimeSplit(t *testing.T) {
	newFakeFragment(t, 20*time.Millisecond)
	h := newHarness(t)
	ctx := context.Background()
	cfg := testConfig(1)
	cfg.RateRPS = 10 // one start per 100 ms: every name but the first waits
	w, err := h.m.Create(ctx, "split", cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.m.SetInput(ctx, w.ID(), testNames(3)); err != nil {
		t.Fatal(err)
	}
	if err := h.m.Start(ctx, w.ID(), StartOptions{}); err != nil {
		t.Fatal(err)
	}
	if !waitNotRunning(w, 10*time.Second) {
		t.Fatal("run did not finish")
	}
	rs, err := h.st.ListResults(ctx, w.ID(), 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	waited := 0
	for _, r := range rs {
		if r.Attempts != 1 || r.NetNs < int64(20*time.Millisecond) || r.TookNs < r.NetNs {
			t.Errorf("%s: took %d net %d wait %d tries %d", r.Username, r.TookNs, r.NetNs, r.WaitNs, r.Attempts)
		}
		if r.WaitNs >= int64(50*time.Millisecond) {
			waited++
		}
	}
	if len(rs) != 3 || waited < 2 {
		t.Errorf("%d rows, %d waited for a -rate start; want 3 and 2", len(rs), waited)
	}
}

// A traced run gets a "pipeline" event every second: the queues and what
// the workers do.
func TestTileTraceHasPipelineEvents(t *testing.T) {
	newFakeFragment(t, 500*time.Millisecond)
	h := newHarness(t)
	ctx := context.Background()
	cfg := testConfig(1)
	cfg.Trace = true
	w, err := h.m.Create(ctx, "pipeline", cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.m.SetInput(ctx, w.ID(), testNames(3)); err != nil {
		t.Fatal(err)
	}
	if err := h.m.Start(ctx, w.ID(), StartOptions{}); err != nil {
		t.Fatal(err)
	}
	if !waitNotRunning(w, 10*time.Second) {
		t.Fatal("run did not finish")
	}
	raw, err := os.ReadFile(h.m.TracePath(w.ID()))
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		var e checker.TraceEvent
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			t.Fatalf("not JSON: %q", line)
		}
		if e.Kind == "pipeline" {
			n++
			if e.Info["workers"] != float64(1) || e.Info["busy"] == nil {
				t.Errorf("pipeline info %v", e.Info)
			}
		}
	}
	if n == 0 {
		t.Error("no pipeline event in a run of about 1.5 s")
	}
}
