// Regression tests for bugs found in the v4 review. Each test describes the
// old behaviour it guards against.
package cli

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// hitServer is a fake Fragment that counts requests per name.
type hitServer struct {
	*httptest.Server
	delay atomic.Int64 // nanoseconds
	mu    sync.Mutex
	hits  map[string]int
}

func newHitServer(t *testing.T, h func(w http.ResponseWriter, r *http.Request, name string)) *hitServer {
	t.Helper()
	s := &hitServer{hits: map[string]int{}}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(r.URL.Path, "/username/")
		s.mu.Lock()
		s.hits[name]++
		s.mu.Unlock()
		if d := time.Duration(s.delay.Load()); d > 0 {
			select {
			case <-time.After(d):
			case <-r.Context().Done():
				return
			}
		}
		h(w, r, name)
	}))
	t.Cleanup(s.Close)
	return s
}

func (s *hitServer) count(name string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hits[name]
}

func freeHandler(w http.ResponseWriter, r *http.Request, name string) {
	http.Redirect(w, r, "/?query="+name, http.StatusFound)
}

// Ctrl+C: the checks in flight were written to results.txt as
// "error (cancelled)", and -resume treats every name in the file as done,
// so they were never checked (92 of 300 names in this test).
func TestSIGINTThenResumeChecksInterruptedNames(t *testing.T) {
	srv := newHitServer(t, freeHandler)
	srv.delay.Store(int64(400 * time.Millisecond))
	dir := t.TempDir()
	in := filepath.Join(dir, "names.txt")
	out := filepath.Join(dir, "results.txt")
	var b strings.Builder
	for i := 0; i < 300; i++ {
		fmt.Fprintf(&b, "name%03d\n", i)
	}
	if err := os.WriteFile(in, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}

	go func() {
		time.Sleep(900 * time.Millisecond)
		_ = syscall.Kill(os.Getpid(), syscall.SIGINT)
	}()
	args := []string{"-input", in, "-output", out, "-workers", "50", "-retries", "0",
		"-base-url", srv.URL, "-no-cache", "-quiet"}
	if code := Main(args); code != 0 {
		t.Fatalf("first run exit=%d", code)
	}
	raw, _ := os.ReadFile(out)
	if n := strings.Count(string(raw), "(cancelled)"); n > 0 {
		t.Errorf("Ctrl+C wrote %d 'error (cancelled)' lines", n)
	}

	srv.delay.Store(0)
	if code := Main(append(args, "-resume", "-append")); code != 0 {
		t.Fatalf("resume exit=%d", code)
	}
	raw, _ = os.ReadFile(out)
	done := map[string]int{}
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		if name, status, ok := strings.Cut(line, " | "); ok && status == "free" {
			done[strings.TrimPrefix(name, "@")]++
		}
	}
	missing := 0
	for i := 0; i < 300; i++ {
		if done[fmt.Sprintf("name%03d", i)] != 1 {
			missing++
		}
	}
	if missing > 0 {
		t.Errorf("after Ctrl+C and -resume, %d of 300 names have no 'free' line (or more than one)", missing)
	}
}

// A results file written by an older version may already contain
// "error (cancelled)" lines; -resume must check those names again.
func TestResumeRechecksOldCancelledLines(t *testing.T) {
	path := filepath.Join(t.TempDir(), "results.txt")
	body := "@alice | free\n@bob | error (cancelled)\n@carol | error (HTTP 403)\n"
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	set, err := loadResumeSet(path)
	if err != nil {
		t.Fatal(err)
	}
	_, alice := set["alice"]
	_, bob := set["bob"]
	_, carol := set["carol"]
	if !alice || bob || !carol {
		t.Errorf("resume set = %v, want alice and carol, not bob", set)
	}
}
