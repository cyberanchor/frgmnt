// Integration tests for the persistence layer. These open a real SQLite
// file in a t.TempDir() to make sure the schema, migrations, and resume
// helpers all line up.
package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	dir := t.TempDir()
	s, err := Open(filepath.Join(dir, "data"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestWorkerCRUDAndStartedAt(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	w := Worker{ID: "w1", Name: "alpha", ConfigJSON: `{"workers":4}`}
	if err := s.CreateWorker(ctx, w); err != nil {
		t.Fatalf("CreateWorker: %v", err)
	}

	got, err := s.GetWorker(ctx, "w1")
	if err != nil {
		t.Fatalf("GetWorker: %v", err)
	}
	if got.Name != "alpha" {
		t.Errorf("Name = %q", got.Name)
	}
	if !got.StartedAt.IsZero() {
		t.Errorf("StartedAt should default to zero, got %v", got.StartedAt)
	}

	now := time.Now().Truncate(time.Millisecond)
	if err := s.SetWorkerStartedAt(ctx, "w1", now); err != nil {
		t.Fatalf("SetWorkerStartedAt: %v", err)
	}
	got, err = s.GetWorker(ctx, "w1")
	if err != nil {
		t.Fatalf("GetWorker after SetStartedAt: %v", err)
	}
	if !got.StartedAt.Equal(now) {
		t.Errorf("StartedAt persisted = %v, want %v", got.StartedAt, now)
	}

	if err := s.DeleteWorker(ctx, "w1"); err != nil {
		t.Fatalf("DeleteWorker: %v", err)
	}
	if _, err := s.GetWorker(ctx, "w1"); err != ErrNotFound {
		t.Errorf("after delete: err = %v, want ErrNotFound", err)
	}
}

func TestInputsAndResumeFlags(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	if err := s.CreateWorker(ctx, Worker{ID: "w2", Name: "b"}); err != nil {
		t.Fatalf("CreateWorker: %v", err)
	}
	names := []string{"alice", "bob", "carol", "dan"}
	if err := s.ReplaceInputs(ctx, "w2", names); err != nil {
		t.Fatalf("ReplaceInputs: %v", err)
	}
	if n, _ := s.InputCount(ctx, "w2"); n != int64(len(names)) {
		t.Fatalf("InputCount = %d, want %d", n, len(names))
	}
	if n, _ := s.PendingInputCount(ctx, "w2"); n != int64(len(names)) {
		t.Fatalf("PendingInputCount = %d, want %d", n, len(names))
	}

	// Mark two as checked, then verify pending iter only emits the rest.
	if err := s.MarkInputChecked(ctx, "w2", "alice"); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkInputChecked(ctx, "w2", "carol"); err != nil {
		t.Fatal(err)
	}
	if n, _ := s.PendingInputCount(ctx, "w2"); n != 2 {
		t.Fatalf("PendingInputCount after marks = %d, want 2", n)
	}
	var pending []string
	if err := s.IterPendingInputs(ctx, "w2", func(n string) error {
		pending = append(pending, n)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	want := []string{"bob", "dan"}
	if !equalStrSlice(pending, want) {
		t.Fatalf("pending = %v, want %v", pending, want)
	}

	// Reset clears the marks.
	if err := s.ResetInputsChecked(ctx, "w2"); err != nil {
		t.Fatal(err)
	}
	if n, _ := s.PendingInputCount(ctx, "w2"); n != int64(len(names)) {
		t.Fatalf("PendingInputCount after reset = %d, want %d", n, len(names))
	}
}

func TestInsertAndListResults(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	if err := s.CreateWorker(ctx, Worker{ID: "w3"}); err != nil {
		t.Fatal(err)
	}
	rs := []Result{
		{WorkerID: "w3", Username: "alice", Status: "free", TookNs: 1000},
		{WorkerID: "w3", Username: "bob", Status: "taken", TookNs: 1100},
		{WorkerID: "w3", Username: "carol", Status: "sold", TookNs: 1200},
	}
	if err := s.InsertResults(ctx, rs); err != nil {
		t.Fatalf("InsertResults: %v", err)
	}
	got, err := s.ListResults(ctx, "w3", 0, 100)
	if err != nil {
		t.Fatalf("ListResults: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d rows, want 3", len(got))
	}

	counts, err := s.CountByStatus(ctx, "w3")
	if err != nil {
		t.Fatal(err)
	}
	for _, st := range []string{"free", "taken", "sold"} {
		if counts[st] != 1 {
			t.Errorf("counts[%q] = %d, want 1", st, counts[st])
		}
	}

	if err := s.ClearResults(ctx, "w3"); err != nil {
		t.Fatal(err)
	}
	got, _ = s.ListResults(ctx, "w3", 0, 100)
	if len(got) != 0 {
		t.Errorf("after clear: got %d rows, want 0", len(got))
	}
}

func TestSettingsRoundTrip(t *testing.T) {
	s := openTestStore(t)
	v := s.GetSettings()
	if v.LogLevel == "" {
		t.Fatalf("GetSettings returned empty LogLevel; defaults missing")
	}
	v.LogLevel = "debug"
	v.Theme = "light"
	v.DefaultWorkers = 17
	if err := s.PutSettings(v); err != nil {
		t.Fatalf("PutSettings: %v", err)
	}
	got := s.GetSettings()
	if got.LogLevel != "debug" || got.Theme != "light" || got.DefaultWorkers != 17 {
		t.Fatalf("settings round-trip mismatch: %+v", got)
	}
}

func equalStrSlice(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
