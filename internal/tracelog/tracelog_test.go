package tracelog

import (
	"compress/gzip"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cyberanchor/frgmnt/internal/checker"
)

func read(t *testing.T, paths ...string) []checker.TraceEvent {
	t.Helper()
	events, err := ReadAll(paths...)
	if err != nil {
		t.Fatal(err)
	}
	return events
}

// checkSeq fails unless the events are numbered one after another.
func checkSeq(t *testing.T, events []checker.TraceEvent) {
	t.Helper()
	for i := 1; i < len(events); i++ {
		if events[i].Seq != events[i-1].Seq+1 {
			t.Fatalf("seq %d after %d at line %d", events[i].Seq, events[i-1].Seq, i+1)
		}
	}
}

// Every worker of a run writes to one Writer: the lines stay whole and in
// one numbered order, the file is private, Close counts the events, and a
// second Open with Append keeps the first run.
func TestWriterConcurrentAppend(t *testing.T) {
	path := filepath.Join(t.TempDir(), "trace.jsonl")
	w, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 250; i++ {
				w.Write(checker.TraceEvent{Kind: "attempt", Name: fmt.Sprintf("g%di%d", g, i), Evidence: strings.Repeat("x", 100)})
			}
		}(g)
	}
	wg.Wait()
	st, err := w.Close()
	if st.Events != 2000 || st.Dropped != 0 || err != nil {
		t.Fatalf("Close = %+v, %v; want 2000 events", st, err)
	}
	if fi, _ := os.Stat(path); st.Bytes != fi.Size() {
		t.Errorf("Stats.Bytes %d, the file has %d", st.Bytes, fi.Size())
	}
	events := read(t, path)
	if len(events) != 2000 || events[0].Seq != 1 {
		t.Fatalf("%d lines starting at seq %d, want 2000 from 1", len(events), events[0].Seq)
	}
	checkSeq(t, events)
	if fi, _ := os.Stat(path); fi.Mode().Perm() != 0o600 {
		t.Errorf("mode %v, want 0600", fi.Mode().Perm())
	}
	w, err = Open(path, Options{Append: true})
	if err != nil {
		t.Fatal(err)
	}
	w.Write(checker.TraceEvent{Kind: "run_start", Info: map[string]any{"version": "x"}, Body: "<p>a & b</p>"})
	_, _ = w.Close()
	events = read(t, path)
	if last := events[len(events)-1]; len(events) != 2001 || last.Info["schema"] != float64(Schema) {
		t.Errorf("%d lines after appending, last %+v; want 2001 with the schema in run_start", len(events), last)
	}
	// Pages stay readable in the file: HTML is not escaped as \u003c.
	if raw, _ := os.ReadFile(path); !strings.Contains(string(raw), `"body":"<p>a & b</p>"`) {
		t.Error("the page is escaped in the file")
	}
}

// A path ending in .gz is written with gzip; appending adds a member and
// the file still reads as one stream.
func TestWriterGzip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "trace.jsonl.gz")
	var written int64
	for run := 0; run < 2; run++ {
		w, err := Open(path, Options{Append: run > 0})
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 100; i++ {
			w.Write(checker.TraceEvent{Kind: "attempt", Name: fmt.Sprintf("r%dn%d", run, i), Evidence: "<html>"})
		}
		st, err := w.Close()
		if err != nil {
			t.Fatal(err)
		}
		written += st.Bytes
	}
	// Stats.Bytes counts what this Writer wrote, the last gzip block
	// included: both runs add up to the file.
	if fi, _ := os.Stat(path); written != fi.Size() {
		t.Errorf("Stats.Bytes add up to %d, the file has %d", written, fi.Size())
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := gzip.NewReader(f); err != nil {
		t.Fatalf("not gzip: %v", err)
	}
	events := read(t, path)
	if len(events) != 200 || events[0].Evidence != "<html>" {
		t.Fatalf("%d events, first %+v", len(events), events[0])
	}
	raw, _ := os.ReadFile(path)
	if strings.Contains(string(raw), "r0n1") {
		t.Error("the file is not compressed")
	}
}

// With a limit the trace keeps its tail in two segments: the newest events
// are all there, the oldest are dropped, the current file starts with a
// segment event that repeats run_start, and the numbering goes on.
func TestWriterKeepsTheTail(t *testing.T) {
	for _, name := range []string{"trace.jsonl", "trace.jsonl.gz"} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, name)
			// gzip emits its output in blocks, so a compressed segment can
			// pass its half of the limit by about one block.
			const limit = 64 << 10
			w, err := Open(path, Options{MaxBytes: limit})
			if err != nil {
				t.Fatal(err)
			}
			w.Write(checker.TraceEvent{Kind: "run_start", Info: map[string]any{"version": "test"}})
			const n = 20000
			for i := 0; i < n; i++ {
				w.Write(checker.TraceEvent{Kind: "attempt", Name: fmt.Sprintf("name%05d", i), Evidence: strings.Repeat("y", 60)})
			}
			w.Write(checker.TraceEvent{Kind: "run_end"})
			st, err := w.Close()
			if err != nil || st.Rotations < 2 {
				t.Fatalf("Close = %+v, %v; want rotations", st, err)
			}
			files := Files(path)
			if len(files) != 2 || files[0] != OlderPath(path) {
				t.Fatalf("files %v", files)
			}
			var total int64
			for _, f := range files {
				fi, _ := os.Stat(f)
				total += fi.Size()
			}
			if total > limit+limit/2 {
				t.Errorf("%d bytes on disk with a %d limit", total, limit)
			}
			events := read(t, files...)
			checkSeq(t, events)
			if last := events[len(events)-1]; last.Kind != "run_end" {
				t.Errorf("last event %+v, want run_end (the tail is kept)", last)
			}
			if events[0].Kind == "run_start" {
				t.Error("the start of the run is still there; nothing was dropped")
			}
			cur := read(t, path)
			if cur[0].Kind != "segment" || cur[0].Info["version"] != "test" || cur[0].Info["schema"] != float64(Schema) {
				t.Errorf("the current file starts with %+v, want a segment repeating run_start", cur[0])
			}
		})
	}
}

// A new trace (not appending) removes the older segment of an earlier run.
func TestWriterNewTraceDropsTheOldSegment(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "trace.jsonl")
	if err := os.WriteFile(OlderPath(path), []byte(`{"kind":"old"}`+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	w, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	w.Write(checker.TraceEvent{Kind: "run_start"})
	_, _ = w.Close()
	if files := Files(path); len(files) != 1 || files[0] != path {
		t.Errorf("files %v, want only %s", files, path)
	}
}

// When the disk runs low the trace stops with a trace_stopped line; later
// events are counted as dropped and the run is not disturbed.
func TestWriterStopsOnALowDisk(t *testing.T) {
	oldFree, oldEvery := freeSpace, flushEvery
	t.Cleanup(func() { freeSpace, flushEvery = oldFree, oldEvery })
	var mu sync.Mutex
	free := int64(10 << 30)
	freeSpace = func(string) (int64, bool) {
		mu.Lock()
		defer mu.Unlock()
		return free, true
	}
	flushEvery = 10 * time.Millisecond
	path := filepath.Join(t.TempDir(), "trace.jsonl")
	w, err := Open(path, Options{MinFree: 256 << 20})
	if err != nil {
		t.Fatal(err)
	}
	w.Write(checker.TraceEvent{Kind: "attempt", Name: "before"})
	mu.Lock()
	free = 100 << 20
	mu.Unlock()
	deadline := time.Now().Add(3 * time.Second)
	for w.Stats().Stopped == "" && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	for i := 0; i < 10; i++ {
		w.Write(checker.TraceEvent{Kind: "attempt", Name: "after"})
	}
	st, err := w.Close()
	if err != nil || !strings.Contains(st.Stopped, "less than 256 MB free") || st.Dropped != 10 {
		t.Fatalf("Close = %+v, %v", st, err)
	}
	events := read(t, path)
	if len(events) != 2 || events[0].Name != "before" || events[1].Kind != "trace_stopped" {
		t.Errorf("events %+v", events)
	}
}

// A full queue makes the writer wait instead of losing events; after
// Close, Write is safe and counted as dropped; Close can run twice.
func TestWriterAfterClose(t *testing.T) {
	w, err := Open(filepath.Join(t.TempDir(), "trace.jsonl"), Options{})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3*queueLen; i++ {
		w.Write(checker.TraceEvent{Kind: "attempt"})
	}
	st, err := w.Close()
	if err != nil || st.Events != 3*queueLen {
		t.Fatalf("Close = %+v, %v", st, err)
	}
	w.Write(checker.TraceEvent{Kind: "late"})
	if st, _ := w.Close(); st.Dropped != 1 || st.Events != 3*queueLen {
		t.Errorf("after Close: %+v, want 1 dropped", st)
	}
}

func TestOlderPath(t *testing.T) {
	for in, want := range map[string]string{
		"run.jsonl.gz":        "run.1.jsonl.gz",
		"/a/b/w01.jsonl":      "/a/b/w01.1.jsonl",
		"trace":               "trace.1",
		"dir.d/.hidden.jsonl": "dir.d/.hidden.1.jsonl",
		"dir.d/.hidden":       "dir.d/.hidden.1",
		"traces/abc.jsonl.gz": "traces/abc.1.jsonl.gz",
	} {
		if got := OlderPath(in); got != want {
			t.Errorf("OlderPath(%q) = %q, want %q", in, got, want)
		}
	}
}
