// Package store wraps a small SQLite database used to persist workers,
// their input lists, and the streaming results table. Global app settings
// live in a JSON file next to the DB so they are easy to inspect/edit by
// hand. modernc.org/sqlite is the only third-party dependency.
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// Store owns the *sql.DB and JSON-settings file.
type Store struct {
	db          *sql.DB
	dataDir     string
	settingsMu  sync.RWMutex
	settings    Settings
	settingsLoc string
}

// Open initializes the data directory and opens both the SQLite database
// and the settings file. Pass an absolute path; relative paths are
// resolved against the current working directory.
func Open(dataDir string) (*Store, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}
	dsn := "file:" + filepath.Join(dataDir, "data.db") + "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}
	db.SetMaxOpenConns(1)
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping db: %w", err)
	}
	if err := migrate(db); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}

	s := &Store{
		db:          db,
		dataDir:     dataDir,
		settingsLoc: filepath.Join(dataDir, "settings.json"),
	}
	if err := s.loadSettings(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("load settings: %w", err)
	}
	return s, nil
}

// Close releases the underlying DB connection.
func (s *Store) Close() error { return s.db.Close() }

// DataDir returns the directory holding data.db and settings.json.
func (s *Store) DataDir() string { return s.dataDir }

// -----------------------------------------------------------------------------
// Schema
// -----------------------------------------------------------------------------

const schema = `
CREATE TABLE IF NOT EXISTS workers (
    id           TEXT PRIMARY KEY,
    name         TEXT NOT NULL,
    config_json  TEXT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'idle',
    total        INTEGER NOT NULL DEFAULT 0,
    started_at   INTEGER NOT NULL DEFAULT 0,
    created_at   INTEGER NOT NULL,
    updated_at   INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS worker_inputs (
    worker_id    TEXT NOT NULL,
    seq          INTEGER NOT NULL,
    username     TEXT NOT NULL,
    checked      INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (worker_id, seq),
    FOREIGN KEY (worker_id) REFERENCES workers(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_inputs_pending
    ON worker_inputs(worker_id, checked, seq);

CREATE TABLE IF NOT EXISTS results (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    worker_id    TEXT NOT NULL,
    username     TEXT NOT NULL,
    status       TEXT NOT NULL,
    error        TEXT,
    took_ns      INTEGER NOT NULL,
    checked_at   INTEGER NOT NULL,
    confidence   INTEGER NOT NULL DEFAULT 0,
    evidence     TEXT NOT NULL DEFAULT '',
    cached       INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (worker_id) REFERENCES workers(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_results_worker ON results(worker_id, id);
CREATE INDEX IF NOT EXISTS idx_results_status ON results(worker_id, status);
`

// migrate creates the schema and applies idempotent upgrades for older DBs.
func migrate(db *sql.DB) error {
	if _, err := db.Exec(schema); err != nil {
		return err
	}
	// Idempotent column adds — tolerate "duplicate column" errors for
	// already-migrated databases.
	for _, alter := range []string{
		`ALTER TABLE workers ADD COLUMN started_at INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE worker_inputs ADD COLUMN checked INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE results ADD COLUMN confidence INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE results ADD COLUMN evidence TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE results ADD COLUMN cached INTEGER NOT NULL DEFAULT 0`,
	} {
		if _, err := db.Exec(alter); err != nil && !strings.Contains(err.Error(), "duplicate column") {
			return err
		}
	}
	return nil
}

// -----------------------------------------------------------------------------
// Workers
// -----------------------------------------------------------------------------

// Worker is the persisted shape of one tile in the UI.
type Worker struct {
	ID         string    `json:"id"`
	Name       string    `json:"name"`
	ConfigJSON string    `json:"config_json"`
	Status     string    `json:"status"`
	Total      int64     `json:"total"`
	StartedAt  time.Time `json:"started_at"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

// CreateWorker inserts a new worker row.
func (s *Store) CreateWorker(ctx context.Context, w Worker) error {
	now := time.Now().UnixMilli()
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO workers (id, name, config_json, status, total, started_at, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		w.ID, w.Name, w.ConfigJSON, "idle", 0, 0, now, now,
	)
	return err
}

// UpdateWorker overwrites the mutable columns.
func (s *Store) UpdateWorker(ctx context.Context, id, name, configJSON string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE workers SET name = ?, config_json = ?, updated_at = ? WHERE id = ?`,
		name, configJSON, time.Now().UnixMilli(), id,
	)
	return err
}

// SetWorkerStatus is called by the worker runtime to reflect lifecycle changes.
func (s *Store) SetWorkerStatus(ctx context.Context, id, status string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE workers SET status = ?, updated_at = ? WHERE id = ?`,
		status, time.Now().UnixMilli(), id,
	)
	return err
}

// SetWorkerStartedAt persists the most recent Start time so the UI can
// display it after a restart.
func (s *Store) SetWorkerStartedAt(ctx context.Context, id string, t time.Time) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE workers SET started_at = ?, updated_at = ? WHERE id = ?`,
		t.UnixMilli(), time.Now().UnixMilli(), id,
	)
	return err
}

// SetWorkerTotal stores the size of the input queue (used by the UI for %).
func (s *Store) SetWorkerTotal(ctx context.Context, id string, total int64) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE workers SET total = ?, updated_at = ? WHERE id = ?`,
		total, time.Now().UnixMilli(), id,
	)
	return err
}

// DeleteWorker removes the worker row and (via ON DELETE CASCADE) its
// inputs and results.
func (s *Store) DeleteWorker(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM workers WHERE id = ?`, id)
	return err
}

// GetWorker returns one worker or ErrNotFound.
func (s *Store) GetWorker(ctx context.Context, id string) (Worker, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT id, name, config_json, status, total, started_at, created_at, updated_at
		 FROM workers WHERE id = ?`, id)
	return scanWorker(row)
}

// ListWorkers returns every worker, newest first.
func (s *Store) ListWorkers(ctx context.Context) ([]Worker, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, name, config_json, status, total, started_at, created_at, updated_at
		 FROM workers ORDER BY created_at ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Worker
	for rows.Next() {
		w, err := scanWorker(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, w)
	}
	return out, rows.Err()
}

type rowScanner interface{ Scan(...any) error }

func scanWorker(r rowScanner) (Worker, error) {
	var w Worker
	var createdMs, updatedMs, startedMs int64
	err := r.Scan(&w.ID, &w.Name, &w.ConfigJSON, &w.Status, &w.Total, &startedMs, &createdMs, &updatedMs)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Worker{}, ErrNotFound
		}
		return Worker{}, err
	}
	w.CreatedAt = time.UnixMilli(createdMs)
	w.UpdatedAt = time.UnixMilli(updatedMs)
	if startedMs > 0 {
		w.StartedAt = time.UnixMilli(startedMs)
	}
	return w, nil
}

// ErrNotFound is returned by lookups when the requested row does not exist.
var ErrNotFound = errors.New("not found")

// -----------------------------------------------------------------------------
// Worker inputs
// -----------------------------------------------------------------------------

// ReplaceInputs atomically replaces the input list for a worker. The seq
// column preserves the upload order.
func (s *Store) ReplaceInputs(ctx context.Context, workerID string, names []string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `DELETE FROM worker_inputs WHERE worker_id = ?`, workerID); err != nil {
		return err
	}
	stmt, err := tx.PrepareContext(ctx, `INSERT INTO worker_inputs (worker_id, seq, username) VALUES (?, ?, ?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for i, n := range names {
		if _, err := stmt.ExecContext(ctx, workerID, i, n); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE workers SET total = ?, updated_at = ? WHERE id = ?`,
		len(names), time.Now().UnixMilli(), workerID,
	); err != nil {
		return err
	}
	return tx.Commit()
}

// InputCount returns how many usernames are queued for the worker.
func (s *Store) InputCount(ctx context.Context, workerID string) (int64, error) {
	var n int64
	err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM worker_inputs WHERE worker_id = ?`, workerID,
	).Scan(&n)
	return n, err
}

// IterInputs invokes fn for every queued name in upload order.
// fn may return an error to stop iteration early.
func (s *Store) IterInputs(ctx context.Context, workerID string, fn func(name string) error) error {
	return s.iterInputs(ctx, workerID, false, fn)
}

// IterPendingInputs invokes fn for every name that has not yet been
// marked checked (used by resume mode).
func (s *Store) IterPendingInputs(ctx context.Context, workerID string, fn func(name string) error) error {
	return s.iterInputs(ctx, workerID, true, fn)
}

func (s *Store) iterInputs(ctx context.Context, workerID string, pendingOnly bool, fn func(name string) error) error {
	q := `SELECT username FROM worker_inputs WHERE worker_id = ? ORDER BY seq ASC`
	if pendingOnly {
		q = `SELECT username FROM worker_inputs WHERE worker_id = ? AND checked = 0 ORDER BY seq ASC`
	}
	rows, err := s.db.QueryContext(ctx, q, workerID)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return err
		}
		if err := fn(name); err != nil {
			return err
		}
	}
	return rows.Err()
}

// MarkInputChecked flags a username as processed so a future resume skips it.
// Safe to call concurrently from multiple workers (one per row).
func (s *Store) MarkInputChecked(ctx context.Context, workerID, username string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE worker_inputs SET checked = 1 WHERE worker_id = ? AND username = ?`,
		workerID, username,
	)
	return err
}

// ResetInputsChecked clears all checked flags so a fresh run reprocesses
// the entire list.
func (s *Store) ResetInputsChecked(ctx context.Context, workerID string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE worker_inputs SET checked = 0 WHERE worker_id = ?`,
		workerID,
	)
	return err
}

// PendingInputCount returns how many usernames remain unchecked.
func (s *Store) PendingInputCount(ctx context.Context, workerID string) (int64, error) {
	var n int64
	err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM worker_inputs WHERE worker_id = ? AND checked = 0`, workerID,
	).Scan(&n)
	return n, err
}

// -----------------------------------------------------------------------------
// Results
// -----------------------------------------------------------------------------

// Result is the persisted shape of one check outcome.
type Result struct {
	ID         int64     `json:"id"`
	WorkerID   string    `json:"worker_id"`
	Username   string    `json:"username"`
	Status     string    `json:"status"`
	Error      string    `json:"error,omitempty"`
	TookNs     int64     `json:"took_ns"`
	CheckedAt  time.Time `json:"checked_at"`
	Confidence int       `json:"confidence,omitempty"`
	Evidence   string    `json:"evidence,omitempty"`
	Cached     bool      `json:"cached,omitempty"`
}

// InsertResult appends a single row to the results table.
func (s *Store) InsertResult(ctx context.Context, r Result) (int64, error) {
	res, err := s.db.ExecContext(ctx,
		`INSERT INTO results (worker_id, username, status, error, took_ns, checked_at, confidence, evidence, cached)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		r.WorkerID, r.Username, r.Status, r.Error, r.TookNs, time.Now().UnixMilli(),
		r.Confidence, r.Evidence, r.Cached,
	)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// InsertResults appends a batch of rows in a single transaction. The web
// layer batches results from the SSE stream to keep insertion costs low at
// high throughput.
func (s *Store) InsertResults(ctx context.Context, rs []Result) error {
	if len(rs) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stmt, err := tx.PrepareContext(ctx,
		`INSERT INTO results (worker_id, username, status, error, took_ns, checked_at, confidence, evidence, cached)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	now := time.Now().UnixMilli()
	for _, r := range rs {
		if _, err := stmt.ExecContext(ctx, r.WorkerID, r.Username, r.Status, r.Error, r.TookNs, now,
			r.Confidence, r.Evidence, r.Cached); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// ClearResults removes every result for a worker. Called when the worker
// is started fresh (i.e. without resume).
func (s *Store) ClearResults(ctx context.Context, workerID string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM results WHERE worker_id = ?`, workerID)
	return err
}

// ListResults returns up to limit results for a worker. When afterID is
// non-zero only rows with id > afterID are returned (used by the UI for
// incremental polling fallback).
func (s *Store) ListResults(ctx context.Context, workerID string, afterID int64, limit int) ([]Result, error) {
	if limit <= 0 {
		limit = 200
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, worker_id, username, status, error, took_ns, checked_at, confidence, evidence, cached
		 FROM results WHERE worker_id = ? AND id > ?
		 ORDER BY id ASC LIMIT ?`,
		workerID, afterID, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Result
	for rows.Next() {
		var r Result
		var checkedMs int64
		var errStr sql.NullString
		if err := rows.Scan(&r.ID, &r.WorkerID, &r.Username, &r.Status, &errStr, &r.TookNs, &checkedMs,
			&r.Confidence, &r.Evidence, &r.Cached); err != nil {
			return nil, err
		}
		if errStr.Valid {
			r.Error = errStr.String
		}
		r.CheckedAt = time.UnixMilli(checkedMs)
		out = append(out, r)
	}
	return out, rows.Err()
}

// CountByStatus returns a map status -> count for the given worker.
func (s *Store) CountByStatus(ctx context.Context, workerID string) (map[string]int64, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT status, COUNT(*) FROM results WHERE worker_id = ? GROUP BY status`,
		workerID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]int64)
	for rows.Next() {
		var status string
		var n int64
		if err := rows.Scan(&status, &n); err != nil {
			return nil, err
		}
		out[status] = n
	}
	return out, rows.Err()
}

// -----------------------------------------------------------------------------
// Settings (JSON file)
// -----------------------------------------------------------------------------

// Settings holds global user-tunable values. Loaded once at startup and
// written back on every PUT /api/settings. Stored in JSON next to the DB
// for easy manual editing.
type Settings struct {
	Theme           string `json:"theme"`             // "light" or "dark"
	LogLevel        string `json:"log_level"`         // matches logbus.Level strings
	DefaultWorkers  int    `json:"default_workers"`   // workers per new Worker tile
	DefaultRateRPS  int    `json:"default_rate_rps"`  // 0 = unlimited
	DefaultRetries  int    `json:"default_retries"`   // 0..10
	DefaultTimeout  int    `json:"default_timeout_s"` // seconds
	HistoryLimitMax int    `json:"history_limit_max"` // max recent results streamed to UI
}

// DefaultSettings returns sensible defaults for a fresh install.
func DefaultSettings() Settings {
	return Settings{
		Theme:           "dark",
		LogLevel:        "info",
		DefaultWorkers:  50,
		DefaultRateRPS:  0,
		DefaultRetries:  2,
		DefaultTimeout:  15,
		HistoryLimitMax: 500,
	}
}

func (s *Store) loadSettings() error {
	data, err := os.ReadFile(s.settingsLoc)
	if errors.Is(err, os.ErrNotExist) {
		s.settings = DefaultSettings()
		return s.saveSettings()
	}
	if err != nil {
		return err
	}
	var loaded Settings
	if err := json.Unmarshal(data, &loaded); err != nil {
		return err
	}
	// Backfill any missing fields with defaults.
	def := DefaultSettings()
	if loaded.Theme == "" {
		loaded.Theme = def.Theme
	}
	if loaded.LogLevel == "" {
		loaded.LogLevel = def.LogLevel
	}
	if loaded.DefaultWorkers == 0 {
		loaded.DefaultWorkers = def.DefaultWorkers
	}
	if loaded.DefaultRetries == 0 {
		loaded.DefaultRetries = def.DefaultRetries
	}
	if loaded.DefaultTimeout == 0 {
		loaded.DefaultTimeout = def.DefaultTimeout
	}
	if loaded.HistoryLimitMax == 0 {
		loaded.HistoryLimitMax = def.HistoryLimitMax
	}
	s.settings = loaded
	return nil
}

func (s *Store) saveSettings() error {
	data, err := json.MarshalIndent(s.settings, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.settingsLoc + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.settingsLoc)
}

// GetSettings returns a copy of the current settings.
func (s *Store) GetSettings() Settings {
	s.settingsMu.RLock()
	defer s.settingsMu.RUnlock()
	return s.settings
}

// PutSettings overwrites the on-disk settings atomically.
func (s *Store) PutSettings(v Settings) error {
	s.settingsMu.Lock()
	defer s.settingsMu.Unlock()
	s.settings = v
	return s.saveSettings()
}
