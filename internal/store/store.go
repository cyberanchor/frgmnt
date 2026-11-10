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

	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
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
	// synchronous(NORMAL): in WAL mode a commit no longer waits for an
	// fsync of the log (the default FULL did, about 14 ms per commit here,
	// which capped the result writer at ~2,300 rows/s). The database
	// cannot be corrupted this way; a power loss can only undo the last
	// commits, and those names stay pending for Resume.
	//
	// cache_size(-16384): a 16 MB page cache instead of SQLite's 2 MB.
	// Names of a real list come in random order, so every insert lands
	// somewhere else in idx_inputs_user; storing 500,000 shuffled names took
	// 15.7 s with 2 MB and 10.1 s with 16 MB (64 MB gave no more).
	//
	// journal_size_limit(16 MB): after a big transaction (a list of a
	// million names writes ~140 MB of log) the WAL file kept that size on
	// disk for as long as the panel ran; now it is cut back when the log
	// is next reset.
	dsn := "file:" + filepath.Join(dataDir, "data.db") +
		"?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)" +
		"&_pragma=cache_size(-16384)&_pragma=journal_size_limit(16777216)"
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

// IsTransient reports whether a failed write may succeed if it is tried
// again: the database was busy (the only connection stayed taken past the
// deadline, or SQLite reported it busy or locked) or the disk was full.
// A closed database and other errors are permanent.
func IsTransient(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var se *sqlite.Error
	if errors.As(err, &se) {
		switch se.Code() & 0xff { // the primary code of an extended one
		case sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED, sqlite3.SQLITE_FULL:
			return true
		}
	}
	return false
}

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
    net_ns       INTEGER NOT NULL DEFAULT 0,
    wait_ns      INTEGER NOT NULL DEFAULT 0,
    attempts     INTEGER NOT NULL DEFAULT 0,
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
		`ALTER TABLE results ADD COLUMN net_ns INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE results ADD COLUMN wait_ns INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE results ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0`,
	} {
		if _, err := db.Exec(alter); err != nil && !strings.Contains(err.Error(), "duplicate column") {
			return err
		}
	}
	// Indexes on columns that older databases get from the ALTERs above
	// must come after them: created in the schema, idx_inputs_pending made
	// a database without the "checked" column impossible to open.
	if _, err := db.Exec(inputIndexes); err != nil {
		return err
	}
	return nil
}

const inputIndexes = `
CREATE INDEX IF NOT EXISTS idx_inputs_pending
    ON worker_inputs(worker_id, checked, seq);

CREATE INDEX IF NOT EXISTS idx_inputs_user
    ON worker_inputs(worker_id, username);
`

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
// column preserves the upload order. Results of names that are in the new
// list are kept and those names count as checked, so a resume checks only
// the new ones; results of names that are no longer in the list are
// deleted. The old results used to stay as they were: a resume after a new
// list mixed both lists in the results and in the download.
func (s *Store) ReplaceInputs(ctx context.Context, workerID string, names []string) error {
	_, err := s.ReplaceInputsFrom(ctx, workerID, func(add func(name string) error) error {
		for _, n := range names {
			if err := add(n); err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

// ReplaceInputsFrom is ReplaceInputs for a list that is not held in a
// slice: each calls add once per name, in upload order, and the names are
// written in chunks as they come. It returns how many names were stored.
// Everything happens in one transaction, so a failure (or an error from
// each) leaves the previous list as it was.
func (s *Store) ReplaceInputsFrom(ctx context.Context, workerID string, each func(add func(name string) error) error) (int64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `DELETE FROM worker_inputs WHERE worker_id = ?`, workerID); err != nil {
		return 0, err
	}
	ins := newBatchInsert(tx, insertInputsHead, 3, inputChunk)
	defer ins.Close()
	var n int64
	if err := each(func(name string) error {
		if err := ins.Add(ctx, workerID, n, name); err != nil {
			return err
		}
		n++
		return nil
	}); err != nil {
		return 0, err
	}
	if err := ins.Flush(ctx); err != nil {
		return 0, err
	}
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM results WHERE worker_id = ? AND username NOT IN
		     (SELECT username FROM worker_inputs WHERE worker_id = ?)`,
		workerID, workerID,
	); err != nil {
		return 0, err
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE worker_inputs SET checked = 1 WHERE worker_id = ? AND username IN
		     (SELECT username FROM results WHERE worker_id = ?)`,
		workerID, workerID,
	); err != nil {
		return 0, err
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE workers SET total = ?, updated_at = ? WHERE id = ?`,
		n, time.Now().UnixMilli(), workerID,
	); err != nil {
		return 0, err
	}
	return n, tx.Commit()
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

// iterPageSize is how many input rows iterInputs reads per query.
const iterPageSize = 500

// iterInputs reads the inputs page by page and calls fn only after the
// page's rows are closed. The database has a single connection: holding a
// cursor open while fn blocks (the worker's producer waits for room in its
// channel) used to lock out the result writer, and a list longer than the
// channel buffers (~2,000 names) stalled the whole run.
func (s *Store) iterInputs(ctx context.Context, workerID string, pendingOnly bool, fn func(name string) error) error {
	q := `SELECT seq, username FROM worker_inputs WHERE worker_id = ? AND seq > ? ORDER BY seq ASC LIMIT ?`
	if pendingOnly {
		q = `SELECT seq, username FROM worker_inputs WHERE worker_id = ? AND checked = 0 AND seq > ? ORDER BY seq ASC LIMIT ?`
	}
	last := int64(-1)
	page := make([]string, 0, iterPageSize)
	for {
		page = page[:0]
		rows, err := s.db.QueryContext(ctx, q, workerID, last, iterPageSize)
		if err != nil {
			return err
		}
		for rows.Next() {
			var name string
			if err := rows.Scan(&last, &name); err != nil {
				rows.Close()
				return err
			}
			page = append(page, name)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		for _, name := range page {
			if err := fn(name); err != nil {
				return err
			}
		}
		if len(page) < iterPageSize {
			return nil
		}
	}
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

// RequeueErrors makes the names whose result is an error pending again and
// deletes those results, in one transaction, so a resume checks them again:
// an error (a network failure, a Cloudflare challenge) is not a verdict.
// It returns how many names were requeued.
func (s *Store) RequeueErrors(ctx context.Context, workerID string) (int64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx,
		`UPDATE worker_inputs SET checked = 0
		 WHERE worker_id = ? AND username IN
		     (SELECT username FROM results WHERE worker_id = ? AND status = 'error')`,
		workerID, workerID,
	); err != nil {
		return 0, err
	}
	res, err := tx.ExecContext(ctx, `DELETE FROM results WHERE worker_id = ? AND status = 'error'`, workerID)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, tx.Commit()
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

	// The split of TookNs (checker.Result): the time of the name's own
	// requests, the time it waited (a -rate start, a pause, a retry delay,
	// a probe), and its HTTP tries.
	NetNs    int64 `json:"net_ns,omitempty"`
	WaitNs   int64 `json:"wait_ns,omitempty"`
	Attempts int   `json:"attempts,omitempty"`
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
	return s.insertResults(ctx, rs, false)
}

// RecordResults appends a batch of rows and flags the matching inputs as
// checked, in one transaction. Resume relies on the flag, so keeping both in
// the same commit means a crash or a failed write can never leave a name
// marked done without its result row: the name stays pending instead.
func (s *Store) RecordResults(ctx context.Context, rs []Result) error {
	return s.insertResults(ctx, rs, true)
}

func (s *Store) insertResults(ctx context.Context, rs []Result, markChecked bool) error {
	if len(rs) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	ins := newBatchInsert(tx, insertResultsHead, 12, resultChunk)
	defer ins.Close()
	now := time.Now().UnixMilli()
	for _, r := range rs {
		if err := ins.Add(ctx, r.WorkerID, r.Username, r.Status, r.Error, r.TookNs, now,
			r.Confidence, r.Evidence, r.Cached, r.NetNs, r.WaitNs, r.Attempts); err != nil {
			return err
		}
	}
	if err := ins.Flush(ctx); err != nil {
		return err
	}
	if markChecked {
		if err := markInputsChecked(ctx, tx, rs); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// -----------------------------------------------------------------------------
// Multi-row statements
// -----------------------------------------------------------------------------

// modernc's SQLite is pure Go and pays a lot per statement, so a prepared
// single-row INSERT run once per row was the bottleneck of both writers:
// 200,000 names took 7.3 s, and a results batch of 32 rows cost 64
// statements (an INSERT and an UPDATE per row). One INSERT now carries a
// chunk of rows and one UPDATE flags a chunk of names. The chunks stay far
// below SQLite's limit of 32,766 parameters per statement.
const (
	inputChunk  = 200 // rows per INSERT INTO worker_inputs (3 parameters each)
	resultChunk = 64  // rows per INSERT INTO results (12 parameters each), names per checked-flag UPDATE

	insertInputsHead  = `INSERT INTO worker_inputs (worker_id, seq, username)`
	insertResultsHead = `INSERT INTO results (worker_id, username, status, error, took_ns, checked_at, confidence, evidence, cached, net_ns, wait_ns, attempts)`
)

// valuesSQL returns head followed by " VALUES (?,…),(?,…)…" for rows rows
// of cols parameters each.
func valuesSQL(head string, rows, cols int) string {
	group := "(" + strings.TrimSuffix(strings.Repeat("?,", cols), ",") + ")"
	var b strings.Builder
	b.Grow(len(head) + 8 + rows*(len(group)+1))
	b.WriteString(head)
	b.WriteString(" VALUES ")
	for i := 0; i < rows; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(group)
	}
	return b.String()
}

// batchInsert queues rows and writes them with one multi-row INSERT per
// chunk. The statement for a full chunk is prepared on first use and
// reused; a shorter tail (Flush) runs unprepared. Not safe for concurrent
// use.
type batchInsert struct {
	tx    *sql.Tx
	head  string
	cols  int
	chunk int
	full  *sql.Stmt
	args  []any
}

func newBatchInsert(tx *sql.Tx, head string, cols, chunk int) *batchInsert {
	return &batchInsert{tx: tx, head: head, cols: cols, chunk: chunk, args: make([]any, 0, cols*chunk)}
}

// Add queues one row (cols values) and writes the chunk once it is full.
func (b *batchInsert) Add(ctx context.Context, vals ...any) error {
	b.args = append(b.args, vals...)
	if len(b.args) < b.cols*b.chunk {
		return nil
	}
	if b.full == nil {
		stmt, err := b.tx.PrepareContext(ctx, valuesSQL(b.head, b.chunk, b.cols))
		if err != nil {
			return err
		}
		b.full = stmt
	}
	_, err := b.full.ExecContext(ctx, b.args...)
	b.args = b.args[:0]
	return err
}

// Flush writes the queued rows of an incomplete chunk.
func (b *batchInsert) Flush(ctx context.Context) error {
	n := len(b.args) / b.cols
	if n == 0 {
		return nil
	}
	_, err := b.tx.ExecContext(ctx, valuesSQL(b.head, n, b.cols), b.args...)
	b.args = b.args[:0]
	return err
}

// Close releases the prepared statement, if any.
func (b *batchInsert) Close() {
	if b.full != nil {
		_ = b.full.Close()
	}
}

// markCheckedSQL flags n names of one worker as checked. The IN list is
// resolved through idx_inputs_user, one index lookup per name, like the
// single-name form.
func markCheckedSQL(n int) string {
	return `UPDATE worker_inputs SET checked = 1 WHERE worker_id = ? AND username IN (` +
		strings.TrimSuffix(strings.Repeat("?,", n), ",") + `)`
}

// markInputsChecked flags the inputs of rs as checked, up to resultChunk
// names per statement. Rows are grouped by worker, in order.
func markInputsChecked(ctx context.Context, tx *sql.Tx, rs []Result) error {
	args := make([]any, 0, resultChunk+1)
	for start := 0; start < len(rs); {
		wid := rs[start].WorkerID
		end := start
		for end < len(rs) && end-start < resultChunk && rs[end].WorkerID == wid {
			end++
		}
		args = append(args[:0], wid)
		for _, r := range rs[start:end] {
			args = append(args, r.Username)
		}
		if _, err := tx.ExecContext(ctx, markCheckedSQL(end-start), args...); err != nil {
			return err
		}
		start = end
	}
	return nil
}

// ClearResults removes every result for a worker and makes every name
// pending again, in one transaction, so the next run (fresh or resume)
// checks the whole list. Clearing used to keep the "checked" flags: Resume
// after Clear said "nothing left to do" and the worker had no results.
func (s *Store) ClearResults(ctx context.Context, workerID string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `DELETE FROM results WHERE worker_id = ?`, workerID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE worker_inputs SET checked = 0 WHERE worker_id = ?`, workerID); err != nil {
		return err
	}
	return tx.Commit()
}

// ListResults returns up to limit results for a worker. When afterID is
// non-zero only rows with id > afterID are returned (used by the UI for
// incremental polling fallback).
func (s *Store) ListResults(ctx context.Context, workerID string, afterID int64, limit int) ([]Result, error) {
	if limit <= 0 {
		limit = 200
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, worker_id, username, status, error, took_ns, checked_at, confidence, evidence, cached,
		        net_ns, wait_ns, attempts
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
			&r.Confidence, &r.Evidence, &r.Cached, &r.NetNs, &r.WaitNs, &r.Attempts); err != nil {
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
	// Decode onto the defaults, so fields missing from the file keep them.
	// default_retries and default_rate_rps can legitimately be 0 (no
	// retries, no rate limit), so a 0 in the file is kept as it is.
	def := DefaultSettings()
	loaded := def
	if err := json.Unmarshal(data, &loaded); err != nil {
		return err
	}
	// Values that cannot be empty or zero fall back to the defaults.
	if loaded.Theme == "" {
		loaded.Theme = def.Theme
	}
	if loaded.LogLevel == "" {
		loaded.LogLevel = def.LogLevel
	}
	if loaded.LogLevel == "trace" {
		// The trace level is gone (its detail is in a worker's trace
		// file); a saved "trace" means debug.
		loaded.LogLevel = "debug"
	}
	if loaded.DefaultWorkers == 0 {
		loaded.DefaultWorkers = def.DefaultWorkers
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
