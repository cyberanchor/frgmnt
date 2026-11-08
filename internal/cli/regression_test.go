// Regression tests for bugs found in the v4 review. Each test describes the
// old behaviour it guards against.
package cli

import (
	"context"
	"fmt"
	"io"
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

// checkWith runs checkOne against a fake Fragment with the given handler.
func checkWith(t *testing.T, retries int, h func(w http.ResponseWriter, r *http.Request, name string)) (Result, *hitServer) {
	t.Helper()
	srv := newHitServer(t, h)
	old := fragmentURLTemplate
	fragmentURLTemplate = srv.URL + "/username/%s"
	t.Cleanup(func() { fragmentURLTemplate = old })
	client := newHTTPClient(httpConfig{maxConns: 1, timeout: time.Second})
	r := checkOne(context.Background(), "alice", client, newStats(), checkConfig{retries: retries, retryDelay: time.Millisecond})
	return r, srv
}

// The web engine treats a 200 Cloudflare interstitial as a retryable error;
// the console engine reported it as a final "unknown" after one request.
func TestChallengeIsRetriedAsError(t *testing.T) {
	r, srv := checkWith(t, 2, func(w http.ResponseWriter, r *http.Request, name string) {
		fmt.Fprint(w, `<html><head><title>Just a moment...</title></head><script>window._cf_chl_opt={}</script></html>`)
	})
	if r.Status != StatusError || srv.count("alice") != 3 {
		t.Errorf("challenge page: status=%q after %d requests, want error after 3", r.Status, srv.count("alice"))
	}
}

// The web engine reads the Location of 301/302/303/307/308; the console
// engine only of 301/302, so a 307 to /?query= (free) became "HTTP 307".
func TestRedirectCodesLikeWeb(t *testing.T) {
	for _, code := range []int{http.StatusSeeOther, http.StatusTemporaryRedirect, http.StatusPermanentRedirect} {
		r, _ := checkWith(t, 0, func(w http.ResponseWriter, r *http.Request, name string) {
			http.Redirect(w, r, "/?query="+name, code)
		})
		if r.Status != StatusFree {
			t.Errorf("%d -> /?query=: got %q (%s), want free", code, r.Status, r.Error)
		}
	}
}

// -retries was not validated: with -1 the attempt loop never ran, no request
// was made and every name became "error (retries exhausted: )".
func TestNegativeRetriesStillMakesARequest(t *testing.T) {
	r, srv := checkWith(t, -1, freeHandler)
	if srv.count("alice") != 1 || r.Status != StatusFree {
		t.Errorf("retries=-1: %d requests, result %+v; want 1 request and free", srv.count("alice"), r)
	}
}

// -readlimit went straight into make([]byte, n): a huge value panicked.
func TestHugeReadLimitFlagIsClamped(t *testing.T) {
	srv := newHitServer(t, func(w http.ResponseWriter, r *http.Request, name string) {
		fmt.Fprint(w, `<span class="tm-section-header-status tm-status-unavail">Sold</span>`)
	})
	defer func(old int) { readLimit = old }(readLimit)
	dir := t.TempDir()
	in := filepath.Join(dir, "names.txt")
	out := filepath.Join(dir, "results.txt")
	if err := os.WriteFile(in, []byte("alice\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	code := Main([]string{"-input", in, "-output", out, "-base-url", srv.URL, "-no-cache", "-quiet",
		"-readlimit", "4611686018427387904"})
	raw, _ := os.ReadFile(out)
	if code != 0 || strings.TrimSpace(string(raw)) != "@alice | sold" {
		t.Errorf("exit=%d output=%q, want 0 and @alice | sold", code, raw)
	}
}

// Results could not be written (here: ENOSPC from /dev/full), the error was
// only logged and the exit code was still 0, so scripts took the run as done.
func TestExitCodeOnOutputWriteFailure(t *testing.T) {
	if _, err := os.Stat("/dev/full"); err != nil {
		t.Skip("no /dev/full")
	}
	srv := newHitServer(t, freeHandler)
	in := filepath.Join(t.TempDir(), "names.txt")
	if err := os.WriteFile(in, []byte("alice\nbob\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	code := Main([]string{"-input", in, "-output", "/dev/full", "-base-url", srv.URL, "-no-cache", "-quiet", "-workers", "2"})
	if code == 0 {
		t.Error("results could not be written (ENOSPC) but the exit code is 0")
	}
}

// With the network down, the console engine served any stale cache entry
// (a days-old auction came back as "auction") and wrote it like a fresh
// result, with nothing in the line saying it came from the cache.
func TestStaleCacheOnlyForSoldAndTaken(t *testing.T) {
	cache := newPageCache(t.TempDir(), time.Nanosecond, time.Nanosecond) // everything is stale at once
	auction := []byte(cliPage("On auction", "avail", `<button class="js-place-bid-btn">Place bid</button>`))
	sold := []byte(cliPage("Sold", "unavail", `<div>Purchased on 19 Nov 2022</div>`))
	for name, body := range map[string][]byte{"oldauction": auction, "oldsold": sold} {
		st, _, found, conf, ev := parseStatusInfo(body)
		if !found {
			t.Fatalf("fixture for %s did not parse", name)
		}
		cache.put(name, "https://fragment.com/username/"+name, st, conf, ev, body)
	}
	time.Sleep(time.Millisecond)

	srv := httptest.NewServer(http.NotFoundHandler())
	srv.Close() // network down
	old := fragmentURLTemplate
	fragmentURLTemplate = srv.URL + "/username/%s"
	defer func() { fragmentURLTemplate = old }()
	client := newHTTPClient(httpConfig{maxConns: 1, timeout: time.Second})
	cfg := checkConfig{retries: 0, retryDelay: time.Millisecond, cache: cache}

	if r := checkOne(context.Background(), "oldauction", client, newStats(), cfg); r.Status != StatusError {
		t.Errorf("stale auction with the network down = %+v, want an error", r)
	}
	r := checkOne(context.Background(), "oldsold", client, newStats(), cfg)
	if r.Status != StatusSold || !strings.Contains(r.Error, "stale cache") {
		t.Errorf("stale sold with the network down = %+v, want sold marked as stale cache", r)
	}
}

// `frgmnt check -version` printed a hard-coded "fragment_checker 3.1".
func TestVersionUsesProgName(t *testing.T) {
	defer func(old string) { ProgName = old }(ProgName)
	ProgName = "frgmnt check"
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout := os.Stdout
	os.Stdout = w
	code := Main([]string{"-version"})
	os.Stdout = stdout
	_ = w.Close()
	out, _ := io.ReadAll(r)
	if code != 0 || strings.TrimSpace(string(out)) != "frgmnt check "+Version {
		t.Errorf("-version printed %q (exit %d), want %q", out, code, "frgmnt check "+Version)
	}
}
