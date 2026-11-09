// Regression tests for bugs found in the v4 review. Each test describes the
// old behaviour it guards against.
package store

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func pendingNames(t *testing.T, s *Store, workerID string) []string {
	t.Helper()
	var out []string
	if err := s.IterPendingInputs(context.Background(), workerID, func(n string) error {
		out = append(out, n)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return out
}

// The web persister used to write the result rows in one transaction and
// the "checked" flag of every name in a separate commit of its own. A stop
// or crash between the two left names marked done without a result, and
// Resume skipped them. RecordResults writes both in one transaction.
func TestRecordResultsMarksInputsCheckedAtomically(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	if err := s.CreateWorker(ctx, Worker{ID: "w", Name: "w", ConfigJSON: "{}"}); err != nil {
		t.Fatal(err)
	}
	if err := s.ReplaceInputs(ctx, "w", []string{"alice", "bob", "carol"}); err != nil {
		t.Fatal(err)
	}

	if err := s.RecordResults(ctx, []Result{
		{WorkerID: "w", Username: "alice", Status: "free"},
		{WorkerID: "w", Username: "carol", Status: "sold"},
	}); err != nil {
		t.Fatal(err)
	}
	if got := pendingNames(t, s, "w"); !equalStrSlice(got, []string{"bob"}) {
		t.Fatalf("pending after RecordResults = %v, want [bob]", got)
	}

	// A batch that fails half-way (the second row breaks the foreign key)
	// must not leave bob flagged without his row.
	err := s.RecordResults(ctx, []Result{
		{WorkerID: "w", Username: "bob", Status: "free"},
		{WorkerID: "no-such-worker", Username: "x", Status: "free"},
	})
	if err == nil {
		t.Fatal("RecordResults with a broken row returned no error")
	}
	if got := pendingNames(t, s, "w"); !equalStrSlice(got, []string{"bob"}) {
		t.Errorf("pending after a failed batch = %v, want [bob] (flag written without its row)", got)
	}
	counts, err := s.CountByStatus(ctx, "w")
	if err != nil {
		t.Fatal(err)
	}
	if counts["free"] != 1 || counts["sold"] != 1 {
		t.Errorf("result counts = %v, want free=1 sold=1", counts)
	}
}

// Flagging a name looked it up by username without an index, so every
// result scanned all inputs of the worker.
func TestCheckedFlagUpdateUsesIndex(t *testing.T) {
	s := openTestStore(t)
	// The statement the result writer runs: many names per UPDATE.
	rows, err := s.db.Query(`EXPLAIN QUERY PLAN `+markCheckedSQL(3), "w", "alice", "bob", "carol")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var plan []string
	for rows.Next() {
		var id, parent, notused int
		var detail string
		if err := rows.Scan(&id, &parent, &notused, &detail); err != nil {
			t.Fatal(err)
		}
		plan = append(plan, detail)
	}
	if p := strings.Join(plan, " / "); !strings.Contains(p, "idx_inputs_user") {
		t.Errorf("checked-flag update does not use idx_inputs_user: %s", p)
	}
}

// Settings documents default_retries as 0..10, but loading the file
// replaced a stored 0 with the default 2, so "no retries" did not survive a
// restart.
func TestSettingsZeroRetriesSurvivesReload(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	v := s.GetSettings()
	v.DefaultRetries = 0
	if err := s.PutSettings(v); err != nil {
		t.Fatal(err)
	}
	_ = s.Close()

	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if got := s2.GetSettings().DefaultRetries; got != 0 {
		t.Errorf("default_retries after restart = %d, want 0", got)
	}
}

// A settings file from an older version without some keys still gets the
// defaults for them.
func TestSettingsMissingKeysGetDefaults(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "settings.json"), []byte(`{"theme":"light"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	got, def := s.GetSettings(), DefaultSettings()
	def.Theme = "light"
	if got != def {
		t.Errorf("settings = %+v, want %+v", got, def)
	}
}

// migrate() created idx_inputs_pending(worker_id, checked, seq) in the
// schema, before the ALTER TABLE that adds "checked" to older databases, so
// a database from before that column could not be opened at all ("no such
// column: checked").
func TestMigrateFromSchemaWithoutCheckedColumn(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", "file:"+filepath.Join(dir, "data.db"))
	if err != nil {
		t.Fatal(err)
	}
	old := `
CREATE TABLE workers (id TEXT PRIMARY KEY, name TEXT NOT NULL, config_json TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'idle', total INTEGER NOT NULL DEFAULT 0,
  created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL);
CREATE TABLE worker_inputs (worker_id TEXT NOT NULL, seq INTEGER NOT NULL, username TEXT NOT NULL,
  PRIMARY KEY (worker_id, seq));
CREATE TABLE results (id INTEGER PRIMARY KEY AUTOINCREMENT, worker_id TEXT NOT NULL,
  username TEXT NOT NULL, status TEXT NOT NULL, error TEXT, took_ns INTEGER NOT NULL,
  checked_at INTEGER NOT NULL);
INSERT INTO workers VALUES ('w1','old','{}','idle',1,1,1);
INSERT INTO worker_inputs VALUES ('w1',0,'alice');`
	if _, err := db.Exec(old); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()

	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open on a database without the checked column: %v", err)
	}
	defer s.Close()
	if got := pendingNames(t, s, "w1"); !equalStrSlice(got, []string{"alice"}) {
		t.Errorf("pending after migration = %v, want [alice]", got)
	}
}

// A new list kept every result of the old one: a resume mixed both lists
// in the results and in the download. Now the results of names that are
// still in the list are kept (and count as checked); the rest are deleted.
func TestReplaceInputsKeepsResultsOfNamesInTheNewList(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	if err := s.CreateWorker(ctx, Worker{ID: "w", Name: "w", ConfigJSON: "{}"}); err != nil {
		t.Fatal(err)
	}
	if err := s.ReplaceInputs(ctx, "w", []string{"alpha", "bravo", "charlie"}); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordResults(ctx, []Result{
		{WorkerID: "w", Username: "alpha", Status: "free"},
		{WorkerID: "w", Username: "bravo", Status: "taken"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.ReplaceInputs(ctx, "w", []string{"bravo", "charlie", "delta"}); err != nil {
		t.Fatal(err)
	}
	rs, err := s.ListResults(ctx, "w", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(rs) != 1 || rs[0].Username != "bravo" {
		t.Errorf("results after the new list: %+v, want only bravo", rs)
	}
	if got := pendingNames(t, s, "w"); !equalStrSlice(got, []string{"charlie", "delta"}) {
		t.Errorf("pending after the new list: %v, want [charlie delta]", got)
	}
}

// Clear deleted the results but kept the "checked" flags, so Resume after
// Clear said "nothing left to do" and the worker stayed without results.
func TestClearResultsMakesNamesPending(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	if err := s.CreateWorker(ctx, Worker{ID: "w", Name: "w", ConfigJSON: "{}"}); err != nil {
		t.Fatal(err)
	}
	if err := s.ReplaceInputs(ctx, "w", []string{"alpha", "bravo"}); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordResults(ctx, []Result{{WorkerID: "w", Username: "alpha", Status: "free"}}); err != nil {
		t.Fatal(err)
	}
	if err := s.ClearResults(ctx, "w"); err != nil {
		t.Fatal(err)
	}
	if got := pendingNames(t, s, "w"); !equalStrSlice(got, []string{"alpha", "bravo"}) {
		t.Errorf("pending after Clear: %v, want both names", got)
	}
}

// Inputs and results are written with multi-row statements in chunks
// (inputChunk, resultChunk). Sizes around the chunk boundaries must keep
// every row, in order, and flag exactly the names that got a result.
func TestBatchedWritesKeepEveryRowInOrder(t *testing.T) {
	ctx := context.Background()
	for _, n := range []int{1, inputChunk - 1, inputChunk, inputChunk + 1, 2*inputChunk + 7} {
		s := openTestStore(t)
		if err := s.CreateWorker(ctx, Worker{ID: "w", Name: "w", ConfigJSON: "{}"}); err != nil {
			t.Fatal(err)
		}
		names := make([]string, n)
		for i := range names {
			names[i] = "name" + strings.Repeat("x", i%3) + string(rune('a'+i%26)) + itoa(i)
		}
		if err := s.ReplaceInputs(ctx, "w", names); err != nil {
			t.Fatalf("n=%d: %v", n, err)
		}
		if got := pendingNames(t, s, "w"); !equalStrSlice(got, names) {
			t.Fatalf("n=%d: %d pending names, want all %d in upload order", n, len(got), n)
		}
		// Results for every other name, a batch larger than resultChunk.
		var rs []Result
		for i := 0; i < n; i += 2 {
			rs = append(rs, Result{WorkerID: "w", Username: names[i], Status: "free", Evidence: "e" + itoa(i)})
		}
		if err := s.RecordResults(ctx, rs); err != nil {
			t.Fatalf("n=%d: %v", n, err)
		}
		got, err := s.ListResults(ctx, "w", 0, 10*n)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != len(rs) {
			t.Fatalf("n=%d: %d result rows, want %d", n, len(got), len(rs))
		}
		for i, r := range got {
			if r.Username != rs[i].Username || r.Evidence != rs[i].Evidence {
				t.Fatalf("n=%d: row %d = %s/%s, want %s/%s", n, i, r.Username, r.Evidence, rs[i].Username, rs[i].Evidence)
			}
		}
		var want []string
		for i := 1; i < n; i += 2 {
			want = append(want, names[i])
		}
		if got := pendingNames(t, s, "w"); !equalStrSlice(got, want) {
			t.Fatalf("n=%d: %d pending after results, want %d", n, len(got), len(want))
		}
	}
}

func itoa(i int) string { return strconv.Itoa(i) }

// The result writer retries a failed write only when it can pass: waiting
// for the busy connection past the deadline is transient, a closed
// database is not (retrying it stalled a shutdown for 30 s).
func TestIsTransient(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	if err := s.CreateWorker(ctx, Worker{ID: "w", Name: "w", ConfigJSON: "{}"}); err != nil {
		t.Fatal(err)
	}
	// Hold the only connection in a transaction; a second write times out.
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	short, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	err = s.RecordResults(short, []Result{{WorkerID: "w", Username: "alice", Status: "free"}})
	_ = tx.Rollback()
	if err == nil || !IsTransient(err) {
		t.Errorf("write while the database is busy: %v, transient=%v; want a transient error", err, IsTransient(err))
	}
	_ = s.Close()
	err = s.RecordResults(ctx, []Result{{WorkerID: "w", Username: "alice", Status: "free"}})
	if err == nil || IsTransient(err) {
		t.Errorf("write to a closed database: %v, transient=%v; want a permanent error", err, IsTransient(err))
	}
}

// A result row kept only the whole time of a name (took_ns). It keeps the
// split too now, network and waiting time and the number of tries, also
// in a database made before these columns existed.
func TestResultsKeepNetworkWaitAndTries(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", "file:"+filepath.Join(dir, "data.db"))
	if err != nil {
		t.Fatal(err)
	}
	old := `
CREATE TABLE workers (id TEXT PRIMARY KEY, name TEXT NOT NULL, config_json TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'idle', total INTEGER NOT NULL DEFAULT 0,
  created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL);
CREATE TABLE worker_inputs (worker_id TEXT NOT NULL, seq INTEGER NOT NULL, username TEXT NOT NULL,
  PRIMARY KEY (worker_id, seq));
CREATE TABLE results (id INTEGER PRIMARY KEY AUTOINCREMENT, worker_id TEXT NOT NULL,
  username TEXT NOT NULL, status TEXT NOT NULL, error TEXT, took_ns INTEGER NOT NULL,
  checked_at INTEGER NOT NULL);
INSERT INTO workers VALUES ('w1','old','{}','idle',2,1,1);
INSERT INTO worker_inputs VALUES ('w1',0,'alice');
INSERT INTO worker_inputs VALUES ('w1',1,'bobby');
INSERT INTO results (worker_id, username, status, took_ns, checked_at) VALUES ('w1','alice','free',5,1);`
	if _, err := db.Exec(old); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()

	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open on an old database: %v", err)
	}
	defer s.Close()
	if err := s.RecordResults(context.Background(), []Result{{WorkerID: "w1", Username: "bobby", Status: "taken",
		TookNs: 900, NetNs: 200, WaitNs: 700, Attempts: 2}}); err != nil {
		t.Fatal(err)
	}
	rs, err := s.ListResults(context.Background(), "w1", 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rs) != 2 {
		t.Fatalf("%d rows, want 2", len(rs))
	}
	if r := rs[0]; r.Username != "alice" || r.NetNs != 0 || r.Attempts != 0 {
		t.Errorf("old row %+v, want the new columns at 0", r)
	}
	if r := rs[1]; r.NetNs != 200 || r.WaitNs != 700 || r.Attempts != 2 || r.TookNs != 900 {
		t.Errorf("new row %+v, want net 200, wait 700, 2 tries", r)
	}
}
