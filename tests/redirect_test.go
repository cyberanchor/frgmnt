package tests

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cyberanchor/frgmnt/internal/checker"
)

// useFragmentServer points the checker at a local fake Fragment and restores
// the global template afterwards. These tests are intentionally not parallel:
// FragmentURLTemplate is a process-wide test hook.
func useFragmentServer(t *testing.T, srv *httptest.Server) {
	t.Helper()
	old := checker.FragmentURLTemplate
	checker.FragmentURLTemplate = srv.URL + "/username/%s"
	t.Cleanup(func() { checker.FragmentURLTemplate = old })
}

func testChecker(t *testing.T, cfg checker.Config) *checker.Checker {
	t.Helper()
	cfg.Workers = 1
	cfg.Retries = 2
	cfg.RetryDelay = time.Millisecond
	cfg.BlockCooldown = time.Millisecond
	cfg.Timeout = 2 * time.Second
	c := checker.New(cfg)
	t.Cleanup(c.Close)
	return c
}

func TestFreeRedirect(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/?query=free_name", http.StatusFound)
	}))
	defer srv.Close()
	useFragmentServer(t, srv)

	c := testChecker(t, checker.Config{})
	res := c.CheckOne(context.Background(), "free_name")
	if res.Status != checker.StatusFree {
		t.Fatalf("status = %q, want free (%s)", res.Status, res.Evidence)
	}
	if res.Confidence < 90 {
		t.Fatalf("confidence = %d, want >= 90", res.Confidence)
	}
}

// The first response is a bare 302 to "/" plus a session cookie; the second
// succeeds only if the cookie jar replays it. This is the throttle/session
// recovery path observed on the real site.
func TestCookieRetryRecoversFromBareRedirect(t *testing.T) {
	soldBody := fixture(t, "sold.html")
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&calls, 1)
		if n == 1 {
			http.SetCookie(w, &http.Cookie{Name: "stel_ssid", Value: "abc", Path: "/"})
			http.Redirect(w, r, "/", http.StatusFound)
			return
		}
		if c, err := r.Cookie("stel_ssid"); err != nil || c.Value != "abc" {
			http.Redirect(w, r, "/", http.StatusFound)
			return
		}
		_, _ = w.Write(soldBody)
	}))
	defer srv.Close()
	useFragmentServer(t, srv)

	res := testChecker(t, checker.Config{}).CheckOne(context.Background(), "adam")
	if res.Status != checker.StatusSold {
		t.Fatalf("status = %q, want sold (evidence: %s)", res.Status, res.Evidence)
	}
	if got := atomic.LoadInt32(&calls); got < 2 {
		t.Fatalf("server calls = %d, want >= 2", got)
	}
}

// When every attempt (and the health probe) is redirected to "/", the client
// is throttled: the verdict must be an error, never a false "reserved".
func TestThrottledClientBecomesError(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		http.Redirect(w, r, "/", http.StatusFound)
	}))
	defer srv.Close()
	useFragmentServer(t, srv)

	res := testChecker(t, checker.Config{}).CheckOne(context.Background(), "tgname")
	if res.Status != checker.StatusError {
		t.Fatalf("status = %q, want error (throttled)", res.Status)
	}
	if res.Error == "" {
		t.Fatal("expected an explanatory error")
	}
	// 3 attempts + 2 probes: one at the first "302 /" (it decides that all
	// workers pause), one for the verdict.
	if got := atomic.LoadInt32(&calls); got != 5 {
		t.Fatalf("server calls = %d, want 5", got)
	}
}

// A genuinely reserved name stays "/" while the random probe resolves, so
// the reserved verdict is allowed with moderate confidence.
func TestGenuineReservedWithHealthyProbe(t *testing.T) {
	soldBody := fixture(t, "sold.html")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/username/zzprobe") {
			_, _ = w.Write(soldBody)
			return
		}
		http.Redirect(w, r, "/", http.StatusFound)
	}))
	defer srv.Close()
	useFragmentServer(t, srv)

	res := testChecker(t, checker.Config{}).CheckOne(context.Background(), "tgname")
	if res.Status != checker.StatusReserved {
		t.Fatalf("status = %q, want reserved (%s)", res.Status, res.Evidence)
	}
	if res.Confidence != 60 {
		t.Fatalf("confidence = %d, want 60", res.Confidence)
	}
}

// Every bare "302 /" paused all workers for BlockCooldown (3 s) before the
// next attempt, so each reserved name in a list stalled the whole run for
// about 3 s per attempt: 7 reserved names took 9 s. Now a probe decides:
// with a healthy probe nobody pauses.
func TestReservedNamesDoNotPauseOthers(t *testing.T) {
	var probes int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(r.URL.Path, "/username/")
		switch {
		case strings.HasPrefix(name, "zzprobe"):
			atomic.AddInt32(&probes, 1)
			http.Redirect(w, r, "/?query="+name, http.StatusFound)
		case strings.HasPrefix(name, "rsv"):
			http.Redirect(w, r, "/", http.StatusFound)
		default:
			http.Redirect(w, r, "/?query="+name, http.StatusFound)
		}
	}))
	defer srv.Close()
	useFragmentServer(t, srv)

	c := checker.New(checker.Config{Workers: 8, Retries: 2, RetryDelay: 50 * time.Millisecond,
		BlockCooldown: 3 * time.Second, Timeout: 2 * time.Second})
	defer c.Close()
	in := make(chan string, 64)
	for i := 0; i < 7; i++ {
		in <- "rsvname" + string(rune('a'+i))
	}
	for i := 0; i < 50; i++ {
		in <- "freename" + string(rune('a'+i%26)) + string(rune('a'+i/26))
	}
	close(in)
	out := make(chan checker.Result, 64)
	start := time.Now()
	c.Run(context.Background(), in, out)
	took := time.Since(start)
	close(out)
	counts := map[checker.Status]int{}
	for r := range out {
		counts[r.Status]++
	}
	if counts[checker.StatusReserved] != 7 || counts[checker.StatusFree] != 50 {
		t.Errorf("counts %v, want 7 reserved and 50 free", counts)
	}
	if took > 2*time.Second {
		t.Errorf("7 reserved names among 57 took %v; a reserved name must not pause the others", took)
	}
	if p := atomic.LoadInt32(&probes); p > 14 {
		t.Errorf("%d probes for 7 reserved names, want at most 2 per name", p)
	}
}
