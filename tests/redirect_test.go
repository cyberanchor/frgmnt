package tests

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
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
	if cfg.ThrottleMax == 0 {
		cfg.ThrottleMax = 100 * time.Millisecond
	}
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
// is throttled: the verdict must never be a false "reserved". The name used
// to end as an error after its three attempts, all spent within one pause
// ("persistent 302 /": 150 such errors in one real run). Throttled tries do
// not count now; the checker pauses (longer each time) and, when the
// throttling lasts ThrottleMax, stops without a verdict for the name.
func TestThrottledClientStopsWithoutAVerdict(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		http.Redirect(w, r, "/", http.StatusFound)
	}))
	defer srv.Close()
	useFragmentServer(t, srv)

	c := testChecker(t, checker.Config{})
	res := c.CheckOne(context.Background(), "tgname")
	if res.Status != checker.StatusError || !strings.Contains(res.Error, "throttling") {
		t.Fatalf("result %q (%s), want an error that says Fragment kept throttling", res.Status, res.Error)
	}
	if !c.Blocked() || c.BlockedBy() != checker.BlockedByThrottle {
		t.Fatalf("Blocked=%v BlockedBy=%v, want blocked by throttling", c.Blocked(), c.BlockedBy())
	}
	if p := c.Snapshot().Pauses; p < 2 {
		t.Errorf("%d pauses, want the pause to repeat while throttled", p)
	}
	if got := atomic.LoadInt32(&calls); got < 2 {
		t.Errorf("server calls = %d, want the name and the probe at least", got)
	}
}

// throttleServer redirects every username to "/" (the probe too) until
// throttleFor has passed since the first request, then answers like
// Fragment: names free, probes free. It records when each request came.
type throttleServer struct {
	mu    sync.Mutex
	first time.Time
	hits  []time.Duration // since the first request
	names []string
}

func newThrottleServer(t *testing.T, throttleFor time.Duration) *throttleServer {
	t.Helper()
	ts := &throttleServer{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(r.URL.Path, "/username/")
		ts.mu.Lock()
		if ts.first.IsZero() {
			ts.first = time.Now()
		}
		since := time.Since(ts.first)
		ts.hits = append(ts.hits, since)
		ts.names = append(ts.names, name)
		ts.mu.Unlock()
		if since < throttleFor {
			http.Redirect(w, r, "/", http.StatusFound)
			return
		}
		http.Redirect(w, r, "/?query="+name, http.StatusFound)
	}))
	t.Cleanup(srv.Close)
	useFragmentServer(t, srv)
	return ts
}

// With no retries at all, a name checked while Fragment throttles got
// "persistent 302 /" on its one attempt. Tries during throttling do not
// count now: the name waits and gets its real verdict when it is over.
func TestThrottleDoesNotSpendAttempts(t *testing.T) {
	newThrottleServer(t, 300*time.Millisecond)
	c := checker.New(checker.Config{Workers: 1, Retries: 0, BlockCooldown: 50 * time.Millisecond,
		ThrottleMax: 10 * time.Second, Timeout: 2 * time.Second})
	defer c.Close()
	res := c.CheckOne(context.Background(), "tgname")
	if res.Status != checker.StatusFree {
		t.Fatalf("status %q (%s), want free once the throttling is over", res.Status, res.Error)
	}
	if c.Blocked() {
		t.Fatal("blocked after the throttling ended")
	}
}

// The shared pause was a fixed 3 s, so a long throttled spell got a try
// (and a probe) every 3 s. It doubles each round now (50 ms, 100, 200, 400
// ms here), so a 1 s spell takes a handful of tries.
func TestThrottlePauseGrows(t *testing.T) {
	ts := newThrottleServer(t, time.Second)
	c := checker.New(checker.Config{Workers: 1, Retries: 0, BlockCooldown: 50 * time.Millisecond,
		ThrottleMax: 10 * time.Second, Timeout: 2 * time.Second})
	defer c.Close()
	if res := c.CheckOne(context.Background(), "tgname"); res.Status != checker.StatusFree {
		t.Fatalf("status %q (%s), want free", res.Status, res.Error)
	}
	ts.mu.Lock()
	defer ts.mu.Unlock()
	var tries []time.Duration
	for i, n := range ts.names {
		if n == "tgname" {
			tries = append(tries, ts.hits[i])
		}
	}
	// A fixed 50 ms pause would give ~20 tries in the throttled second.
	if len(tries) > 8 {
		t.Errorf("%d tries of the name during a 1 s throttle, want few: the pause must grow (%v)", len(tries), tries)
	}
	// A try is never early: the k-th pause lasts at least 50ms*2^(k-1).
	for k := 1; k < len(tries)-1; k++ {
		if gap, min := tries[k+1]-tries[k], 50*time.Millisecond<<(k-1)-10*time.Millisecond; gap < min && tries[k] < time.Second {
			t.Errorf("pause %d lasted %v, want at least %v (%v)", k, gap, min, tries)
		}
	}
	if p := c.Snapshot().Pauses; p < 3 {
		t.Errorf("%d pauses, want several growing ones", p)
	}
}

// A throttled spell longer than ThrottleMax stops the whole run: Run takes
// no more names and gives no verdict for the rest, which stay pending (the
// console exits with 3, the panel shows an error and Resume goes on).
func TestPersistentThrottleStopsRun(t *testing.T) {
	ts := newThrottleServer(t, time.Hour)
	c := checker.New(checker.Config{Workers: 4, MaxConns: 8, Retries: 2, BlockCooldown: 20 * time.Millisecond,
		ThrottleMax: 300 * time.Millisecond, Timeout: 2 * time.Second})
	defer c.Close()
	in := make(chan string, 64)
	for i := 0; i < 40; i++ {
		in <- "throttled" + string(rune('a'+i%26)) + string(rune('a'+i/26))
	}
	close(in)
	out := make(chan checker.Result, 64)
	done := make(chan struct{})
	go func() { c.Run(context.Background(), in, out); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not stop")
	}
	close(out)
	n := 0
	for r := range out {
		n++
		t.Errorf("result for %s: %s (%s); a throttled name must stay pending", r.Username, r.Status, r.Error)
	}
	if !c.Blocked() || c.BlockedBy() != checker.BlockedByThrottle {
		t.Fatalf("Blocked=%v BlockedBy=%v, want blocked by throttling", c.Blocked(), c.BlockedBy())
	}
	ts.mu.Lock()
	sent := len(ts.hits)
	ts.mu.Unlock()
	// After the first pause one request at a time tests the water, so a
	// stopped run sent few requests, not 40 names times 3 attempts.
	if sent > 30 {
		t.Errorf("%d requests before the stop, want few", sent)
	}
	if len(in) == 0 && n == 0 {
		t.Logf("%d requests, %d names left unread", sent, len(in))
	}
}

// HTTP 429 made only the worker that got it sleep (Retry-After, up to
// RLWaitMax) while the others kept sending, and the try used up an
// attempt. Now every worker waits for Retry-After, the try does not count,
// and after the pause one request goes first.
func TestTooManyRequestsPausesEveryWorker(t *testing.T) {
	var mu sync.Mutex
	var first time.Time
	var hits []time.Duration
	var n int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(r.URL.Path, "/username/")
		k := atomic.AddInt32(&n, 1)
		mu.Lock()
		if first.IsZero() {
			first = time.Now()
		}
		hits = append(hits, time.Since(first))
		mu.Unlock()
		switch k {
		case 1:
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		case 2:
			time.Sleep(200 * time.Millisecond) // the first request after the pause is slow
		}
		http.Redirect(w, r, "/?query="+name, http.StatusFound)
	}))
	defer srv.Close()
	useFragmentServer(t, srv)
	c := checker.New(checker.Config{Workers: 4, MaxConns: 8, Retries: 0, Timeout: 5 * time.Second})
	defer c.Close()
	in := make(chan string, 8)
	for i := 0; i < 8; i++ {
		in <- "manyreq" + string(rune('a'+i))
	}
	close(in)
	out := make(chan checker.Result, 8)
	c.Run(context.Background(), in, out)
	close(out)
	free := 0
	for r := range out {
		if r.Status == checker.StatusFree {
			free++
		} else {
			t.Errorf("%s: %s (%s)", r.Username, r.Status, r.Error)
		}
	}
	if free != 8 {
		t.Fatalf("%d of 8 free; with Retries 0 the 429 try must not count", free)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(hits) < 3 || hits[1] < 950*time.Millisecond {
		t.Fatalf("requests at %v: after the 429 no worker may send for 1 s", hits)
	}
	// The first request after the pause goes alone: the next one starts
	// after its (slow) answer.
	if hits[2]-hits[1] < 190*time.Millisecond {
		t.Errorf("requests at %v: after the pause one request must go first", hits)
	}
	if got := c.Snapshot().RateLimit; got != 1 {
		t.Errorf("RateLimit = %d, want 1", got)
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
