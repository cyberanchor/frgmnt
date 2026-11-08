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
		// Rehydrated workers always start idle/ready — never auto-running.
		queued, _ := m.store.InputCount(ctx, w.id)
		w.total = queued
		if queued > 0 {
			w.setStatus(StatusReady)
		} else {
			w.setStatus(StatusIdle)
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
	w.cancel()
	if err := m.store.DeleteWorker(ctx, id); err != nil {
		return err
	}
	m.bus.Publish(logbus.LevelInfo, id, "worker deleted")
	return nil
}

// SetInput replaces the input list of a worker. Allowed only when the
// worker is not currently running.
func (m *Manager) SetInput(ctx context.Context, id string, names []string) (int64, error) {
	w, err := m.Get(id)
	if err != nil {
		return 0, err
	}
	if w.Status() == StatusRunning {
		return 0, ErrWorkerBusy
	}
	if err := m.store.ReplaceInputs(ctx, id, names); err != nil {
		return 0, err
	}
	w.mu.Lock()
	w.total = int64(len(names))
	w.mu.Unlock()
	if len(names) > 0 {
		w.setStatus(StatusReady)
	} else {
		w.setStatus(StatusIdle)
	}
	m.bus.Publish(logbus.LevelInfo, id, fmt.Sprintf("input loaded: %d usernames", len(names)))
	return int64(len(names)), nil
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
	if !w.compareAndSwapStatus(StatusReady, StatusRunning) &&
		!w.compareAndSwapStatus(StatusDone, StatusRunning) &&
		!w.compareAndSwapStatus(StatusError, StatusRunning) {
		if w.Status() == StatusRunning {
			return ErrWorkerBusy
		}
		return ErrNotReady
	}

	// Use a context derived from the manager-level background context,
	// not the request context, so the worker survives request completion.
	wCtx, cancel := context.WithCancel(context.Background())
	now := time.Now()
	w.mu.Lock()
	w.cancel = cancel
	w.startedAt = now
	w.resume = opt.Resume
	w.mu.Unlock()
	_ = m.store.SetWorkerStartedAt(context.Background(), w.id, now)

	go m.run(wCtx, w)
	return nil
}

// Stop cancels the run loop. Safe on a non-running worker (no-op).
func (m *Manager) Stop(id string) error {
	w, err := m.Get(id)
	if err != nil {
		return err
	}
	w.cancel()
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
	m.bus.Publish(logbus.LevelInfo, id, "results cleared")
	return nil
}

// CloseAll cancels every worker and is invoked on graceful shutdown.
func (m *Manager) CloseAll() {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, w := range m.workers {
		w.cancel()
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
	cancel    context.CancelFunc
	chk       *checker.Checker // non-nil while running
	lastStats checker.Stats    // cached snapshot from the most recent run

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

// Stats returns the live counters while running, or the cached snapshot
// from the most recent run after completion. Zero only when the worker
// has never been started.
func (w *Worker) Stats() checker.Stats {
	w.mu.RLock()
	chk := w.chk
	last := w.lastStats
	w.mu.RUnlock()
	if chk == nil {
		return last
	}
	return chk.Snapshot()
}

// StartedAt returns the wall-clock time of the most recent Start call.
// Zero if the worker has never been started.
func (w *Worker) StartedAt() time.Time {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.startedAt
}

func (w *Worker) setStatus(s Status) {
	w.mu.Lock()
	w.status = s
	w.mu.Unlock()
	if w.manager != nil && w.manager.store != nil {
		_ = w.manager.store.SetWorkerStatus(context.Background(), w.id, string(s))
	}
}

func (w *Worker) compareAndSwapStatus(from, to Status) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.status != from {
		return false
	}
	w.status = to
	if w.manager != nil && w.manager.store != nil {
		_ = w.manager.store.SetWorkerStatus(context.Background(), w.id, string(to))
	}
	return true
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
		if err := m.store.ResetInputsChecked(ctx, w.id); err != nil {
			m.bus.Publish(logbus.LevelError, w.id, fmt.Sprintf("reset checked: %v", err))
		}
		queued = total
		m.bus.Publish(logbus.LevelInfo, w.id, fmt.Sprintf(
			"started: %d names | workers=%d conns=%d retries=%d rate=%d",
			queued, cfg.Workers, cfg.MaxConns, cfg.Retries, cfg.RateRPS))
	}
	chk.SetTotal(queued)

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
		if err := m.store.InsertResults(c, batch); err != nil {
			m.bus.Publish(logbus.LevelError, w.id, fmt.Sprintf("persist: %v", err))
		}
		batch = batch[:0]
	}

	// markChecked records the per-name "done" flag used by resume mode.
	// Errors are intentionally ignored: the row may have been deleted by
	// the user (clear/reset) while a result was in flight.
	markChecked := func(r checker.Result) {
		c, cancel := dbCtx()
		defer cancel()
		_ = m.store.MarkInputChecked(c, w.id, r.Username)
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
			markChecked(r)
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
