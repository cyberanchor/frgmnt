// Regression tests for bugs found in the v4 review. Each test describes the
// old behaviour it guards against.
package store

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
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
	rows, err := s.db.Query(`EXPLAIN QUERY PLAN
		UPDATE worker_inputs SET checked = 1 WHERE worker_id = ? AND username = ?`, "w", "alice")
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
