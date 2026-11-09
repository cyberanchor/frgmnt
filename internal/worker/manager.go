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
	"path/filepath"
	"sync"
	"time"

	"github.com/cyberanchor/frgmnt/internal/checker"
	"github.com/cyberanchor/frgmnt/internal/logbus"
	"github.com/cyberanchor/frgmnt/internal/store"
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
	w.stop()
	w.wait(stopTimeout)
	if err := m.store.DeleteWorker(ctx, id); err != nil {
		return err
	}
	m.bus.Publish(logbus.LevelInfo, id, "worker deleted")
	return nil
}

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
	n, err := m.store.ReplaceInputsFrom(ctx, id, each)
	if err != nil {
		return 0, err
	}
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
	w.stop()
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
		w.stop()
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
func (w *Worker) stop() {
	w.mu.RLock()
	cancel := w.cancel
	w.mu.RUnlock()
	cancel()
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
	go m.persister(ctx, w, out, batchDone)

	chk.Run(ctx, in, out)
	blocked := chk.Blocked()
	if blocked {
		// Run gave up early; the producer still waits to hand over a name.
		w.stop()
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

	if blocked {
		m.bus.Publish(logbus.LevelError, w.id, fmt.Sprintf(
			"stopped: Fragment answered %d requests in a row with a Cloudflare challenge "+
				"(HTTP 403, cf-mitigated: challenge), so it blocks this IP. The names without a verdict "+
				"stay pending: press Resume later.", checker.ChallengeLimit))
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

func (m *Manager) persister(ctx context.Context, w *Worker, in <-chan checker.Result, done chan<- struct{}) {
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
	dbCtx := func() (context.Context, context.CancelFunc) {
		return context.WithTimeout(context.Background(), 5*time.Second)
	}
	flush := func() {
		if len(batch) == 0 {
			return
		}
		c, cancel := dbCtx()
		defer cancel()
		// The rows and the per-name "done" flags used by resume mode are
		// written in one transaction (one commit per batch, not one per
		// name). If the write fails, the names stay pending and a resume
		// checks them again.
		if err := m.store.RecordResults(c, batch); err != nil {
			m.bus.Publish(logbus.LevelError, w.id, fmt.Sprintf("persist: %v", err))
		} else {
			w.addSaved(batch)
		}
		batch = batch[:0]
	}

	for {
		select {
		case r, ok := <-in:
			if !ok {
				// Drain done. Persist the tail.
				flush()
				return
			}
			payload, _ := json.Marshal(map[string]any{
				"event":      "result",
				"username":   r.Username,
				"status":     string(r.Status),
				"error":      r.Error,
				"took_ms":    r.Took.Milliseconds(),
				"confidence": r.Confidence,
				"evidence":   r.Evidence,
				"cached":     r.Cached,
				"stale":      r.Stale,
			})
			m.bus.Publish(logbus.LevelDebug, w.id, string(payload))
			batch = append(batch, store.Result{
				WorkerID:   w.id,
				Username:   r.Username,
				Status:     string(r.Status),
				Error:      r.Error,
				TookNs:     r.Took.Nanoseconds(),
				Confidence: r.Confidence,
				Evidence:   r.Evidence,
				Cached:     r.Cached,
			})
			if len(batch) >= batchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

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
