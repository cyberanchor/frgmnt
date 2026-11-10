// Package worker provides per-tile lifecycle management. Each Worker owns
// its own checker.Checker, input queue, cancel func, and a small results
// batcher that flushes to the SQLite store. Worker also broadcasts every
// outcome on the shared logbus so the UI can render it live.
package worker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/cyberanchor/frgmnt/internal/checker"
	"github.com/cyberanchor/frgmnt/internal/logbus"
	"github.com/cyberanchor/frgmnt/internal/store"
	"github.com/cyberanchor/frgmnt/internal/tracelog"
	"github.com/cyberanchor/frgmnt/internal/version"
)

// Status reflects the runtime state of one tile.
type Status string

const (
	StatusIdle    Status = "idle"
	StatusReady   Status = "ready"
	StatusRunning Status = "running"
	StatusPaused  Status = "paused"
	StatusDone    Status = "done"
	StatusError   Status = "error"
)

// Manager owns every Worker known to the running process. It is the only
// component allowed to mutate worker lifecycle state.
type Manager struct {
	store *store.Store
	bus   *logbus.Bus

	mu      sync.RWMutex
	workers map[string]*Worker

	// Page-cache defaults applied to every checker created by this manager.
	cacheDir      string
	cacheTTL      time.Duration
	cacheSoldTTL  time.Duration
	cacheDisabled bool
}

// New returns an empty Manager. Call Load() once at startup to rehydrate
// from the database (workers persisted across restarts are recreated in
// StatusIdle/StatusReady, never auto-resumed).
func New(s *store.Store, b *logbus.Bus) *Manager {
	return &Manager{
		store:   s,
		bus:     b,
		workers: make(map[string]*Worker),
	}
}

// ConfigureCache overrides the page-cache defaults. An empty dir places the
// cache under <data>/pagecache. Call once at startup, before Load/Start.
func (m *Manager) ConfigureCache(dir string, ttl, soldTTL time.Duration, disabled bool) {
	m.cacheDir = dir
	m.cacheTTL = ttl
	m.cacheSoldTTL = soldTTL
	m.cacheDisabled = disabled
}

// applyCache fills empty per-worker cache fields with manager defaults.
func (m *Manager) applyCache(cfg checker.Config) checker.Config {
	if m.cacheDisabled {
		cfg.CacheDir = ""
		return cfg
	}
	if cfg.CacheDir == "" {
		cfg.CacheDir = m.cacheDir
		if cfg.CacheDir == "" {
			cfg.CacheDir = filepath.Join(m.store.DataDir(), "pagecache")
		}
	}
	if cfg.CacheTTL == 0 {
		cfg.CacheTTL = m.cacheTTL
	}
	if cfg.CacheSoldTTL == 0 {
		cfg.CacheSoldTTL = m.cacheSoldTTL
	}
	return cfg
}

// Load reads every persisted worker into memory. Running state is reset.
func (m *Manager) Load(ctx context.Context) error {
	rows, err := m.store.ListWorkers(ctx)
	if err != nil {
		return err
	}
	for _, row := range rows {
		w, err := m.fromRow(row)
		if err != nil {
			m.bus.Publish(logbus.LevelError, "", fmt.Sprintf("load worker %s: %v", row.ID, err))
			continue
		}
		// Rehydrated workers never auto-run. A finished list stays done:
		// it used to come back as "ready" with zero counters on the tile.
		m.refreshProgress(ctx, w)
		switch total, checked := w.Total(), w.checkedCount(); {
		case total == 0:
			w.setStatus(StatusIdle)
		case checked >= total:
			w.setStatus(StatusDone)
		default:
			w.setStatus(StatusReady)
		}
		m.workers[w.id] = w
	}
	return nil
}

// List returns every Worker, sorted by creation time (ascending).
func (m *Manager) List() []*Worker {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*Worker, 0, len(m.workers))
	for _, w := range m.workers {
		out = append(out, w)
	}
	// Stable sort by createdAt to match the DB order.
	sortWorkers(out)
	return out
}

// Get returns the Worker with the given id, or ErrNotFound.
func (m *Manager) Get(id string) (*Worker, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	w, ok := m.workers[id]
	if !ok {
		return nil, ErrNotFound
	}
	return w, nil
}

// Create persists a new Worker and returns it. The runtime instance is
// StatusIdle and has no input queue until SetInput is called.
func (m *Manager) Create(ctx context.Context, name string, cfg checker.Config) (*Worker, error) {
	id, err := newID()
	if err != nil {
		return nil, err
	}
	cfgJSON, err := json.Marshal(cfg)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	row := store.Worker{
		ID:         id,
		Name:       name,
		ConfigJSON: string(cfgJSON),
		Status:     string(StatusIdle),
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	if err := m.store.CreateWorker(ctx, row); err != nil {
		return nil, err
	}
	w, err := m.fromRow(row)
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	m.workers[id] = w
	m.mu.Unlock()
	m.bus.Publish(logbus.LevelInfo, id, fmt.Sprintf("worker %q created", name))
	return w, nil
}

// Update overwrites the name and config. If the worker is running the
// new config is applied on the next Start.
func (m *Manager) Update(ctx context.Context, id, name string, cfg checker.Config) (*Worker, error) {
	w, err := m.Get(id)
	if err != nil {
		return nil, err
	}
	cfgJSON, err := json.Marshal(cfg)
	if err != nil {
		return nil, err
	}
	if err := m.store.UpdateWorker(ctx, id, name, string(cfgJSON)); err != nil {
		return nil, err
	}
	w.mu.Lock()
	w.name = name
	w.cfg = cfg
	w.mu.Unlock()
	m.bus.Publish(logbus.LevelInfo, id, fmt.Sprintf("worker %q updated (workers=%d, retries=%d)", name, cfg.Workers, cfg.Retries))
	return w, nil
}

// Delete cancels and removes a worker (the DB cascade drops inputs/results).
func (m *Manager) Delete(ctx context.Context, id string) error {
	m.mu.Lock()
	w, ok := m.workers[id]
	if ok {
		delete(m.workers, id)
	}
	m.mu.Unlock()
	if !ok {
		return ErrNotFound
	}
	// Let the run save its last batch before the rows go away.
	w.stop("the worker was deleted")
	w.wait(stopTimeout)
	if err := m.store.DeleteWorker(ctx, id); err != nil {
		return err
	}
	m.removeTrace(id)
	m.bus.Publish(logbus.LevelInfo, id, "worker deleted")
	return nil
}

// TracePath is where the structured trace of a worker's runs is written
// when its config has trace on (gzip). With its size limit the trace keeps
// the end of the runs: the part before it is in tracelog.OlderPath.
func (m *Manager) TracePath(id string) string {
	return filepath.Join(m.store.DataDir(), "traces", id+".jsonl.gz")
}

// legacyTracePath is the plain trace file of 2.1.0, which kept the start
// of a run and cut the rest.
func (m *Manager) legacyTracePath(id string) string {
	return filepath.Join(m.store.DataDir(), "traces", id+".jsonl")
}

// TraceFiles lists the trace files of a worker that exist, oldest first.
// A trace written by 2.1.0 is listed only while the worker has no newer
// one.
func (m *Manager) TraceFiles(id string) []string {
	if files := tracelog.Files(m.TracePath(id)); len(files) > 0 {
		return files
	}
	if _, err := os.Stat(m.legacyTracePath(id)); err == nil {
		return []string{m.legacyTracePath(id)}
	}
	return nil
}

// RemoveTrace deletes the trace files of a worker; the worker must not be
// running.
func (m *Manager) RemoveTrace(id string) error {
	var first error
	for _, p := range []string{tracelog.OlderPath(m.TracePath(id)), m.TracePath(id), m.legacyTracePath(id)} {
		if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) && first == nil {
			first = err
		}
	}
	return first
}

func (m *Manager) removeTrace(id string) { _ = m.RemoveTrace(id) }

// maxTraceBytes caps the trace of a worker on disk, both segments together
// (compressed: about a million requests without page bodies); a variable
// so tests can lower it.
var maxTraceBytes int64 = 100 << 20

// TraceMinFree stops a trace when the disk runs low; a variable so tests
// (here and in the server) do not depend on the free space of the machine
// they run on.
var TraceMinFree int64 = tracelog.DefaultMinFree

// SetInput replaces the input list of a worker. Allowed only when the
// worker is not currently running.
func (m *Manager) SetInput(ctx context.Context, id string, names []string) (int64, error) {
	return m.SetInputFrom(ctx, id, func(add func(name string) error) error {
		for _, n := range names {
			if err := add(n); err != nil {
				return err
			}
		}
		return nil
	})
}

// SetInputFrom is SetInput for a list that is not held in a slice (see
// store.ReplaceInputsFrom). Storing a big list takes seconds; meanwhile
// the worker cannot be started and a second list is refused, so a run
// never reads a list that is being replaced.
func (m *Manager) SetInputFrom(ctx context.Context, id string, each func(add func(name string) error) error) (int64, error) {
	w, err := m.Get(id)
	if err != nil {
		return 0, err
	}
	if !w.beginLoad() {
		return 0, ErrWorkerBusy
	}
	defer w.endLoad()
	loadStart := time.Now()
	n, err := m.store.ReplaceInputsFrom(ctx, id, each)
	if err != nil {
		return 0, err
	}
	w.mu.Lock()
	w.lastInput = map[string]any{"names": n, "stored_at": loadStart, "store_ms": time.Since(loadStart).Milliseconds()}
	w.mu.Unlock()
	m.refreshProgress(ctx, w)
	switch checked := w.checkedCount(); {
	case n == 0:
		w.setStatus(StatusIdle)
	case checked >= n:
		w.setStatus(StatusDone) // every name already has a result
	default:
		w.setStatus(StatusReady)
	}
	msg := fmt.Sprintf("input loaded: %d usernames", n)
	if checked := w.checkedCount(); checked > 0 {
		msg += fmt.Sprintf(" (%d already have a result and are kept)", checked)
	}
	m.bus.Publish(logbus.LevelInfo, id, msg)
	return n, nil
}

// StartOptions controls Start behaviour.
type StartOptions struct {
	// Resume continues from the next pending name (does not clear results
	// and does not reset the checked flags). When false, the run starts
	// from scratch — results are cleared and every input is re-checked.
	Resume bool
}

// Start kicks off the run loop. Returns ErrWorkerBusy if already running.
func (m *Manager) Start(ctx context.Context, id string, opt StartOptions) error {
	w, err := m.Get(id)
	if err != nil {
		return err
	}
	if err := w.tryStart(); err != nil {
		return err
	}

	// Use a context derived from the manager-level background context,
	// not the request context, so the worker survives request completion.
	wCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	now := time.Now()
	w.mu.Lock()
	w.cancel = cancel
	w.stopWhy = ""
	w.done = done
	w.startedAt = now
	w.resume = opt.Resume
	w.mu.Unlock()
	_ = m.store.SetWorkerStartedAt(context.Background(), w.id, now)

	go func() {
		defer close(done)
		m.run(wCtx, w)
	}()
	return nil
}

// Pending returns how many names a Resume of the worker would check: the
// unchecked ones and the ones whose result is an error.
func (m *Manager) Pending(ctx context.Context, id string) (int64, error) {
	if _, err := m.Get(id); err != nil {
		return 0, err
	}
	pending, err := m.store.PendingInputCount(ctx, id)
	if err != nil {
		return 0, err
	}
	counts, err := m.store.CountByStatus(ctx, id)
	if err != nil {
		return 0, err
	}
	return pending + counts[string(checker.StatusError)], nil
}

// Stop cancels the run loop. Safe on a non-running worker (no-op).
func (m *Manager) Stop(id string) error {
	w, err := m.Get(id)
	if err != nil {
		return err
	}
	w.stop("Stop in the panel or the API")
	return nil
}

// ClearResults wipes the results table for a worker. Disallowed while running.
func (m *Manager) ClearResults(ctx context.Context, id string) error {
	w, err := m.Get(id)
	if err != nil {
		return err
	}
	if w.Status() == StatusRunning {
		return ErrWorkerBusy
	}
	if err := m.store.ClearResults(ctx, id); err != nil {
		return err
	}
	m.refreshProgress(ctx, w)
	if w.Total() > 0 {
		w.setStatus(StatusReady) // nothing is checked any more
	}
	m.bus.Publish(logbus.LevelInfo, id, "results cleared")
	return nil
}

// stopTimeout bounds how long Delete and CloseAll wait for a cancelled run
// to save its last results.
const stopTimeout = 10 * time.Second

// CloseAll cancels every worker and waits for the runs to save their last
// results. It is invoked on graceful shutdown, before the database is
// closed: returning right after the cancel let the caller close the
// database under the result writer, and the tail of every run was lost.
func (m *Manager) CloseAll() {
	m.mu.RLock()
	ws := make([]*Worker, 0, len(m.workers))
	for _, w := range m.workers {
		ws = append(ws, w)
	}
	m.mu.RUnlock()
	for _, w := range ws {
		w.stop("the server is shutting down")
	}
	deadline := time.Now().Add(stopTimeout)
	for _, w := range ws {
		if !w.wait(time.Until(deadline)) {
			m.bus.Publish(logbus.LevelWarn, w.id, "shutdown: the run did not finish in time")
		}
	}
}

// -----------------------------------------------------------------------------
// Worker (one tile)
// -----------------------------------------------------------------------------

// Worker is a single tile in the UI. All field access goes through the
// methods on this type; nothing outside this package should mutate state
// directly.
type Worker struct {
	id        string
	createdAt time.Time

	mu        sync.RWMutex
	name      string
	cfg       checker.Config
	status    Status
	total     int64
	startedAt time.Time
	resume    bool // set by Start; consumed by run
	loading   bool // SetInputFrom is storing a new list; Start waits for it
	cancel    context.CancelFunc
	stopWhy   string           // why the current run was cancelled (the first reason), for its trace
	lastInput map[string]any   // the last list stored: names, time to store it; for the next run's trace
	done      chan struct{}    // closed when the current run returns; nil before the first Start
	chk       *checker.Checker // non-nil while running
	lastStats checker.Stats    // cached snapshot from the most recent run

	// checked and counts describe the stored results: names flagged
	// checked, and results per status. They are read from the database
	// when the worker is loaded or a run starts, and grow with every batch
	// the run saves, so the tile adds up over Resume runs and survives a
	// restart.
	checked int64
	counts  map[string]int64

	manager *Manager
}

// ID returns the immutable worker ID.
func (w *Worker) ID() string { return w.id }

// CreatedAt returns when the worker row was inserted.
func (w *Worker) CreatedAt() time.Time { return w.createdAt }

// Name returns the human-readable name.
func (w *Worker) Name() string {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.name
}

// Config returns a copy of the active checker config.
func (w *Worker) Config() checker.Config {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.cfg
}

// Status returns the lifecycle state.
func (w *Worker) Status() Status {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.status
}

// Total returns the size of the queued input.
func (w *Worker) Total() int64 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.total
}

// Stats returns the counters shown on the tile. Total, Checked and the
// per-status counts describe the stored results of the whole list. They
// used to be the counters of the current run only: after a Resume the tile
// showed "32/40 = 80%" for a finished list, and after a restart zeros. The
// request counters (retries, rate limits, connections, cache, challenges)
// are those of the current run, or of the last one.
func (w *Worker) Stats() checker.Stats {
	w.mu.RLock()
	defer w.mu.RUnlock()
	st := w.lastStats
	if w.chk != nil {
		st = w.chk.Snapshot()
	}
	st.Total = w.total
	st.Checked = w.checked
	c := w.counts
	st.Free = c[string(checker.StatusFree)]
	st.Reserved = c[string(checker.StatusReserved)]
	st.Auction = c[string(checker.StatusAuction)]
	st.Taken = c[string(checker.StatusTaken)]
	st.Sold = c[string(checker.StatusSold)]
	st.Available = c[string(checker.StatusAvailable)]
	st.Unknown = c[string(checker.StatusUnknown)]
	st.Errors = c[string(checker.StatusError)]
	return st
}

func (w *Worker) checkedCount() int64 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.checked
}

// addSaved counts a batch of results that was stored.
func (w *Worker) addSaved(batch []store.Result) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.counts == nil {
		w.counts = make(map[string]int64)
	}
	for _, r := range batch {
		w.counts[r.Status]++
	}
	w.checked += int64(len(batch))
}

// refreshProgress reads the list size, the checked names and the results
// per status of a worker from the database.
func (m *Manager) refreshProgress(ctx context.Context, w *Worker) {
	total, err := m.store.InputCount(ctx, w.id)
	if err != nil {
		m.bus.Publish(logbus.LevelError, w.id, fmt.Sprintf("progress: %v", err))
		return
	}
	pending, err := m.store.PendingInputCount(ctx, w.id)
	if err != nil {
		m.bus.Publish(logbus.LevelError, w.id, fmt.Sprintf("progress: %v", err))
		return
	}
	counts, err := m.store.CountByStatus(ctx, w.id)
	if err != nil {
		m.bus.Publish(logbus.LevelError, w.id, fmt.Sprintf("progress: %v", err))
		return
	}
	w.mu.Lock()
	w.total = total
	w.checked = total - pending
	w.counts = counts
	w.mu.Unlock()
}

// StartedAt returns the wall-clock time of the most recent Start call.
// Zero if the worker has never been started.
func (w *Worker) StartedAt() time.Time {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.startedAt
}

// stop cancels the current run, if any. Start replaces w.cancel under w.mu,
// so it is read under the same lock.
// stop cancels the current run; why goes to its trace ("stop" event).
func (w *Worker) stop(why string) {
	w.mu.Lock()
	cancel := w.cancel
	if w.stopWhy == "" {
		w.stopWhy = why
	}
	w.mu.Unlock()
	cancel()
}

// lastInputInfo describes the last list stored (since the server
// started), or nil.
func (w *Worker) lastInputInfo() map[string]any {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.lastInput
}

func (w *Worker) stopReason() string {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.stopWhy
}

// wait blocks until the current run (if any) has returned, at most d.
// It reports whether the run is over.
func (w *Worker) wait(d time.Duration) bool {
	w.mu.RLock()
	done := w.done
	w.mu.RUnlock()
	if done == nil {
		return true
	}
	select {
	case <-done:
		return true
	default:
	}
	if d <= 0 {
		return false
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-done:
		return true
	case <-t.C:
		return false
	}
}

func (w *Worker) setStatus(s Status) {
	w.mu.Lock()
	w.status = s
	w.mu.Unlock()
	if w.manager != nil && w.manager.store != nil {
		_ = w.manager.store.SetWorkerStatus(context.Background(), w.id, string(s))
	}
}

// tryStart moves a ready, finished or failed worker to running, under one
// lock with the check for a list being stored. The status is saved after
// the lock is released: a Start during a long upload used to wait for the
// database while holding the lock, which also stalled every read of the
// tile.
func (w *Worker) tryStart() error {
	w.mu.Lock()
	switch {
	case w.status == StatusRunning, w.loading:
		w.mu.Unlock()
		return ErrWorkerBusy
	case w.status != StatusReady && w.status != StatusDone && w.status != StatusError:
		w.mu.Unlock()
		return ErrNotReady
	}
	w.status = StatusRunning
	w.mu.Unlock()
	if w.manager != nil && w.manager.store != nil {
		_ = w.manager.store.SetWorkerStatus(context.Background(), w.id, string(StatusRunning))
	}
	return nil
}

// beginLoad marks the worker as storing a new list. It fails while the
// worker runs or another list is being stored.
func (w *Worker) beginLoad() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.loading || w.status == StatusRunning {
		return false
	}
	w.loading = true
	return true
}

func (w *Worker) endLoad() {
	w.mu.Lock()
	w.loading = false
	w.mu.Unlock()
}

// -----------------------------------------------------------------------------
// Run loop
// -----------------------------------------------------------------------------

func (m *Manager) run(ctx context.Context, w *Worker) {
	cfg := m.applyCache(w.Config())
	w.mu.RLock()
	resume := w.resume
	w.mu.RUnlock()

	chk := checker.New(cfg)
	defer chk.Close()
	// The engine's output goes to the tile's log, at the level of the bus:
	// the warnings (throttle pauses, HTTP 429, the stop) and, at debug, a
	// line per HTTP try with its timings. Every step of every request is in
	// the worker's trace file ("Write a trace"), not in the log; an old
	// "trace" log level counts as debug.
	lg := checker.Logger{
		Warnf: func(format string, args ...any) {
			m.bus.Publish(logbus.LevelWarn, w.id, fmt.Sprintf(format, args...))
		},
		Debugf: func(format string, args ...any) {
			m.bus.Publish(logbus.LevelDebug, w.id, fmt.Sprintf(format, args...))
		},
		Level: func() checker.LogLevel {
			if m.bus.Level() >= logbus.LevelDebug {
				return checker.LogDebug
			}
			return checker.LogInfo
		},
	}
	chk.SetLogger(lg)
	w.mu.Lock()
	w.chk = chk
	w.mu.Unlock()
	defer func() {
		w.mu.Lock()
		w.chk = nil
		w.mu.Unlock()
	}()

	total, err := m.store.InputCount(ctx, w.id)
	if err != nil {
		m.bus.Publish(logbus.LevelError, w.id, fmt.Sprintf("input count: %v", err))
		w.setStatus(StatusError)
		return
	}
	if total == 0 {
		m.bus.Publish(logbus.LevelWarn, w.id, "start requested but input queue is empty")
		w.setStatus(StatusIdle)
		return
	}

	var queued int64
	if resume {
		// Errors are not verdicts: check those names again too.
		if n, err := m.store.RequeueErrors(ctx, w.id); err != nil {
			m.bus.Publish(logbus.LevelError, w.id, fmt.Sprintf("requeue errors: %v", err))
		} else if n > 0 {
			m.bus.Publish(logbus.LevelInfo, w.id, fmt.Sprintf("resume: %d names that ended with an error are checked again", n))
		}
		pending, err := m.store.PendingInputCount(ctx, w.id)
		if err != nil {
			m.bus.Publish(logbus.LevelError, w.id, fmt.Sprintf("pending count: %v", err))
			w.setStatus(StatusError)
			return
		}
		if pending == 0 {
			m.bus.Publish(logbus.LevelInfo, w.id, "resume: nothing left to do")
			w.setStatus(StatusDone)
			return
		}
		queued = pending
		m.bus.Publish(logbus.LevelInfo, w.id, fmt.Sprintf(
			"resumed: %d pending of %d total | workers=%d conns=%d retries=%d rate=%d",
			pending, total, cfg.Workers, cfg.MaxConns, cfg.Retries, cfg.RateRPS))
	} else {
		if err := m.store.ClearResults(ctx, w.id); err != nil {
			m.bus.Publish(logbus.LevelError, w.id, fmt.Sprintf("clear results: %v", err))
		}
		queued = total
		m.bus.Publish(logbus.LevelInfo, w.id, fmt.Sprintf(
			"started: %d names | workers=%d conns=%d retries=%d rate=%d",
			queued, cfg.Workers, cfg.MaxConns, cfg.Retries, cfg.RateRPS))
	}
	chk.SetTotal(queued)
	m.refreshProgress(ctx, w)

	// The structured trace of the run, when the worker asks for it: a fresh
	// start begins a new trace, Resume appends to it.
	var trace *tracelog.Writer
	runStart := time.Now()
	if cfg.Trace {
		path := m.TracePath(w.id)
		if !resume {
			_ = os.Remove(m.legacyTracePath(w.id))
		}
		var err error
		if err = os.MkdirAll(filepath.Dir(path), 0o700); err == nil {
			trace, err = tracelog.Open(path, tracelog.Options{
				Append: resume, MaxBytes: maxTraceBytes, MinFree: TraceMinFree,
			})
		}
		if err != nil {
			m.bus.Publish(logbus.LevelError, w.id, fmt.Sprintf("trace: %v (the run goes on without it)", err))
		} else {
			lg.Event = trace.Write
			chk.SetLogger(lg)
			trace.Write(checker.TraceEvent{Time: runStart, Kind: "run_start", Info: map[string]any{
				"program": "frgmnt web", "version": version.Version, "worker": w.id, "worker_name": w.Name(),
				"resume": resume, "list_names": total, "queued": queued,
				"workers": cfg.Workers, "max_conns": cfg.MaxConns, "rate_rps": cfg.RateRPS, "retries": cfg.Retries,
				"retry_delay_ms": cfg.RetryDelay.Milliseconds(), "timeout_ms": cfg.Timeout.Milliseconds(),
				"read_limit": cfg.ReadLimit, "rl_wait_max_ms": cfg.RLWaitMax.Milliseconds(),
				"block_cooldown_ms": cfg.BlockCooldown.Milliseconds(), "throttle_max_ms": cfg.ThrottleMax.Milliseconds(),
				"cache_dir": cfg.CacheDir, "log": m.bus.Level().String(), "trace_bodies": cfg.TraceBodies,
				"trace_max": maxTraceBytes, "env": tracelog.Environment(), "config": cfg, "input": w.lastInputInfo(),
			}})
		}
	}

	in := make(chan string, 1024)
	out := make(chan checker.Result, 1024)

	iter := m.store.IterInputs
	if resume {
		iter = m.store.IterPendingInputs
	}

	// Producer goroutine: stream inputs from DB into the checker.
	go func() {
		defer close(in)
		err := iter(ctx, w.id, func(name string) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case in <- name:
				return nil
			}
		})
		if err != nil && !errors.Is(err, context.Canceled) {
			m.bus.Publish(logbus.LevelError, w.id, fmt.Sprintf("producer: %v", err))
		}
	}()

	// Persister goroutine: batch writes to SQLite.
	batchDone := make(chan struct{})
	go m.persister(ctx, w, out, batchDone, trace)

	// With a trace, the state of the run once a second: the queues and
	// what the workers do.
	pipeDone := make(chan struct{})
	pipeStopped := make(chan struct{})
	if trace == nil {
		close(pipeStopped)
	} else {
		go func() {
			defer close(pipeStopped)
			tick := time.NewTicker(time.Second)
			defer tick.Stop()
			for {
				select {
				case <-pipeDone:
					return
				case now := <-tick.C:
					e := chk.Snapshot()
					trace.Write(checker.TraceEvent{Time: now, Kind: "pipeline", Info: map[string]any{
						"queue_in": len(in), "queue_out": len(out), "checked": e.Checked, "workers": cfg.Workers,
						"busy": e.Busy, "in_flight": e.InFlight, "wait_gate": e.WaitGate, "wait_pause": e.WaitPause,
						"wait_rate": e.WaitRate, "wait_retry": e.WaitRetry, "wait_probe": e.WaitProbe,
						"wait_conn": e.WaitConn, "conn_open": e.ConnOpen, "bytes_in": e.BytesIn, "bytes_out": e.BytesOut,
						"trace": trace.Stats(),
					}})
				}
			}
		}()
	}

	chk.Run(ctx, in, out)
	close(pipeDone)
	<-pipeStopped // no pipeline event after the trace is closed
	blocked := chk.Blocked()
	if trace != nil && ctx.Err() != nil && !blocked {
		trace.Write(checker.TraceEvent{Kind: "stop", Reason: w.stopReason()})
	}
	if blocked {
		// Run gave up early; the producer still waits to hand over a name.
		w.stop("blocked")
	}
	close(out)
	<-batchDone

	// Snapshot stats so the tile keeps showing useful numbers after the
	// checker is closed.
	final := chk.Snapshot()
	if final.CacheHits+final.CacheStale+final.CacheMisses > 0 {
		m.bus.Publish(logbus.LevelInfo, w.id, fmt.Sprintf(
			"cache: hits=%d stale=%d misses=%d", final.CacheHits, final.CacheStale, final.CacheMisses))
	}
	w.mu.Lock()
	w.lastStats = final
	w.mu.Unlock()
	if trace != nil {
		info := tracelog.StatsInfo(final)
		info["elapsed_ms"] = time.Since(runStart).Milliseconds()
		switch {
		case blocked:
			info["blocked"] = tracelog.BlockedName(chk.BlockedBy())
		case ctx.Err() != nil:
			info["stopped"] = true
		}
		info["trace"] = trace.Stats()
		trace.Write(checker.TraceEvent{Kind: "run_end", Info: info})
		st, err := trace.Close()
		if err != nil {
			m.bus.Publish(logbus.LevelError, w.id, fmt.Sprintf("trace: %v (the trace is incomplete)", err))
		} else {
			msg := fmt.Sprintf("trace: %d events, %.1f MB", st.Events, float64(st.Bytes)/(1<<20))
			if st.Rotations > 0 {
				msg += fmt.Sprintf("; it keeps the last %d MB of the runs, the older part was dropped", maxTraceBytes>>20)
			}
			m.bus.Publish(logbus.LevelInfo, w.id, msg)
		}
		if st.Stopped != "" {
			m.bus.Publish(logbus.LevelWarn, w.id, "the trace stopped before the end of the run: "+st.Stopped)
		}
	}

	if blocked {
		msg := fmt.Sprintf("stopped: Fragment answered %d requests in a row with a Cloudflare challenge "+
			"(HTTP 403, cf-mitigated: challenge), so it blocks this IP. The names without a verdict "+
			"stay pending: press Resume later.", checker.ChallengeLimit)
		if chk.BlockedBy() == checker.BlockedByThrottle {
			msg = fmt.Sprintf("stopped: Fragment kept throttling this IP for %v (every request and the probe "+
				"redirected to \"/\", or HTTP 429). The names without a verdict stay pending: press Resume "+
				"later, with a lower rate.", chk.Config().ThrottleMax)
		}
		m.bus.Publish(logbus.LevelError, w.id, msg)
		w.setStatus(StatusError)
		return
	}
	if ctx.Err() != nil {
		m.bus.Publish(logbus.LevelInfo, w.id, "stopped (cancelled)")
		w.setStatus(StatusReady)
		return
	}
	m.bus.Publish(logbus.LevelInfo, w.id, "completed")
	w.setStatus(StatusDone)
}

func (m *Manager) persister(ctx context.Context, w *Worker, in <-chan checker.Result, done chan<- struct{}, trace *tracelog.Writer) {
	defer close(done)
	const batchSize = 32
	const flushEvery = 250 * time.Millisecond
	batch := make([]store.Result, 0, batchSize)
	ticker := time.NewTicker(flushEvery)
	defer ticker.Stop()

	// `in` is closed once the checker is done (or cancelled). The
	// dispatcher in run() does:
	//     chk.Run(ctx, in, out)   // returns promptly on ctx cancel
	//     close(out)              // <-- closes our `in`
	// We MUST drain `in` until it is closed; otherwise a result that the
	// checker emits between "ctx fired" and "close(out)" would be lost.
	// We do NOT select on ctx.Done() here for that reason.
	//
	// Every SQLite write uses a fresh, detached context with a 5s budget:
	//   - Local SQLite writes are cheap (sub-millisecond); cancellation is
	//     not a useful signal for them.
	//   - Using the in-flight ctx here meant that when the user pressed
	//     Stop, the trailing flush failed with "sql: context canceled" and
	//     lost up to batchSize rows.
	//
	// A write that fails keeps its rows. The database is busy for seconds
	// while another tile stores a big list (one connection, one writer),
	// and the batch used to be dropped after 5 s: the results were lost
	// and the names checked again on the next Resume. Now the batch is
	// written again after a pause that doubles up to maxRetryPause, and
	// once it holds maxPending rows the writer stops taking results, so
	// the checks wait (the results channel fills) instead. Only errors that
	// can pass are retried (store.IsTransient): busy, locked, disk full.
	var (
		failing bool // the last write failed
		pause   time.Duration
		retryAt time.Time // no new attempt before this
	)
	flush := func() bool {
		if len(batch) == 0 {
			return true
		}
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		// The rows and the per-name "done" flags used by resume mode are
		// written in one transaction (one commit per batch, not one per
		// name).
		start := time.Now()
		err := m.store.RecordResults(c, batch)
		if trace != nil {
			// Every write of results, so a slow or failing database shows
			// in the trace next to the checks it held up.
			e := checker.TraceEvent{Kind: "db_flush", Info: map[string]any{
				"rows": len(batch), "took_ms": float64(time.Since(start).Microseconds()) / 1000, "retrying": failing,
			}}
			if err != nil {
				e.Error = err.Error()
			}
			trace.Write(e)
		}
		if err != nil {
			if !store.IsTransient(err) {
				// A closed database or a broken file does not get better:
				// report it and go on; the names stay pending for Resume.
				m.bus.Publish(logbus.LevelError, w.id, fmt.Sprintf("persist: %v", err))
				batch = batch[:0]
				return true
			}
			if !failing {
				m.bus.Publish(logbus.LevelWarn, w.id, fmt.Sprintf(
					"results not saved yet (%v); keeping them and trying again", err))
			}
			failing = true
			pause = min(max(2*pause, flushEvery), maxRetryPause)
			retryAt = time.Now().Add(pause)
			return false
		}
		if failing {
			m.bus.Publish(logbus.LevelInfo, w.id, fmt.Sprintf("results saved again (%d rows waited)", len(batch)))
		}
		failing, pause = false, 0
		w.addSaved(batch)
		batch = batch[:0]
		return true
	}

	for {
		take := in
		if len(batch) >= maxPending {
			take = nil // the database refuses writes: let the checks wait
		}
		select {
		case r, ok := <-take:
			if !ok {
				// Drain done. Persist the tail, retrying for a while.
				for deadline := time.Now().Add(finalSaveBudget); !flush() && time.Now().Before(deadline); {
					time.Sleep(time.Until(retryAt))
				}
				if len(batch) > 0 {
					m.bus.Publish(logbus.LevelError, w.id, fmt.Sprintf(
						"%d results could not be saved; their names stay pending and Resume checks them again", len(batch)))
				}
				return
			}
			// The per-result event only reaches the Logs tab at the debug
			// level. It was built for every result anyway (2.7 µs and 24
			// allocations each) and dropped by the bus at info.
			if m.bus.Level() >= logbus.LevelDebug {
				payload, _ := json.Marshal(map[string]any{
					"event":      "result",
					"username":   r.Username,
					"status":     string(r.Status),
					"error":      r.Error,
					"took_ms":    r.Took.Milliseconds(),
					"net_ms":     r.Net.Milliseconds(),
					"wait_ms":    r.Wait.Milliseconds(),
					"work_ms":    r.Work.Milliseconds(),
					"attempts":   r.Attempts,
					"confidence": r.Confidence,
					"evidence":   r.Evidence,
					"cached":     r.Cached,
					"stale":      r.Stale,
				})
				m.bus.Publish(logbus.LevelDebug, w.id, string(payload))
			}
			batch = append(batch, store.Result{
				WorkerID:   w.id,
				Username:   r.Username,
				Status:     string(r.Status),
				Error:      r.Error,
				TookNs:     r.Took.Nanoseconds(),
				Confidence: r.Confidence,
				Evidence:   r.Evidence,
				Cached:     r.Cached,
				NetNs:      r.Net.Nanoseconds(),
				WaitNs:     r.Wait.Nanoseconds(),
				WorkNs:     r.Work.Nanoseconds(),
				Attempts:   r.Attempts,
			})
			if len(batch) >= batchSize && !time.Now().Before(retryAt) {
				flush()
			}
		case <-ticker.C:
			if !time.Now().Before(retryAt) {
				flush()
			}
		}
	}
}

// Limits of the result writer while the database refuses writes.
const (
	maxPending      = 4096             // rows held before the checks are made to wait
	maxRetryPause   = 5 * time.Second  // longest pause between two attempts
	finalSaveBudget = 30 * time.Second // how long the last rows are retried at the end of a run
)

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

// ErrNotFound is returned by Manager lookups when the id is unknown.
var ErrNotFound = errors.New("worker not found")

// ErrWorkerBusy is returned when an operation requires the worker to be idle.
var ErrWorkerBusy = errors.New("worker is busy")

// ErrNotReady is returned when Start is called on a worker without queued input.
var ErrNotReady = errors.New("worker is not ready")

func newID() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

func (m *Manager) fromRow(row store.Worker) (*Worker, error) {
	cfg := checker.DefaultConfig()
	if row.ConfigJSON != "" {
		if err := json.Unmarshal([]byte(row.ConfigJSON), &cfg); err != nil {
			return nil, fmt.Errorf("decode config: %w", err)
		}
	}
	return &Worker{
		id:        row.ID,
		name:      row.Name,
		cfg:       cfg,
		status:    Status(row.Status),
		total:     row.Total,
		startedAt: row.StartedAt,
		createdAt: row.CreatedAt,
		cancel:    func() {},
		manager:   m,
	}, nil
}

func sortWorkers(out []*Worker) {
	// Simple insertion sort (small N expected).
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j].createdAt.Before(out[j-1].createdAt); j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
}
