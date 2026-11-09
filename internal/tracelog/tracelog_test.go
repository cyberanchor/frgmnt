package tracelog

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/cyberanchor/frgmnt/internal/checker"
)

func readLines(t *testing.T, path string) []checker.TraceEvent {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var out []checker.TraceEvent
	for i, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		var e checker.TraceEvent
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			t.Fatalf("line %d: %v: %q", i+1, err, line)
		}
		out = append(out, e)
	}
	return out
}

// Every worker of a run writes to one Writer: the lines must stay whole.
// The file is private, Close counts the events, and a second Open with
// appendTo keeps the first run.
func TestWriterConcurrentAppend(t *testing.T) {
	path := filepath.Join(t.TempDir(), "trace.jsonl")
	w, err := Open(path, false, 0)
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
	if n, dropped, err := w.Close(); n != 2000 || dropped != 0 || err != nil {
		t.Fatalf("Close = %d, %d, %v; want 2000, 0, nil", n, dropped, err)
	}
	if got := len(readLines(t, path)); got != 2000 {
		t.Fatalf("%d lines, want 2000", got)
	}
	if fi, _ := os.Stat(path); fi.Mode().Perm() != 0o600 {
		t.Errorf("mode %v, want 0600", fi.Mode().Perm())
	}
	w, err = Open(path, true, 0)
	if err != nil {
		t.Fatal(err)
	}
	w.Write(checker.TraceEvent{Kind: "run_start"})
	_, _, _ = w.Close()
	if got := len(readLines(t, path)); got != 2001 {
		t.Errorf("%d lines after appending, want 2001", got)
	}
}

// With a limit the file stops at about that size: the last line says the
// trace was cut, the events after it are counted as dropped.
func TestWriterLimit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "trace.jsonl")
	w, err := Open(path, false, 4<<10)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 200; i++ {
		w.Write(checker.TraceEvent{Kind: "attempt", Name: fmt.Sprintf("name%03d", i), Evidence: strings.Repeat("y", 50)})
	}
	n, dropped, err := w.Close()
	if err != nil || n == 0 || dropped == 0 || n+dropped != 200 {
		t.Fatalf("Close = %d written, %d dropped, %v", n, dropped, err)
	}
	fi, _ := os.Stat(path)
	if fi.Size() > 4<<10+512 {
		t.Errorf("file is %d bytes with a 4 KB limit", fi.Size())
	}
	lines := readLines(t, path)
	if last := lines[len(lines)-1]; last.Kind != "trace_truncated" || last.Message == "" {
		t.Errorf("last line %+v, want trace_truncated", last)
	}
}
