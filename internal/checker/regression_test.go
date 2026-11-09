// Regression tests for bugs found in the v4 review. Each test describes the
// old behaviour it guards against.
package checker

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// withServer points FragmentURLTemplate at a local handler for one test.
func withServer(t *testing.T, h http.HandlerFunc) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(h)
	old := FragmentURLTemplate
	FragmentURLTemplate = srv.URL + "/username/%s"
	t.Cleanup(func() { FragmentURLTemplate = old; srv.Close() })
	return srv
}

// After a cancel, the checks that were in flight came back as
// Status=error/Error="cancelled", were counted in Stats.Errors and were
// still sent on out (select picked randomly between ctx.Done and out<-r).
// The web worker then stored them as results and flagged the names done.
func TestCancelledChecksAreNotEmitted(t *testing.T) {
	withServer(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(2 * time.Second):
		case <-r.Context().Done():
			return
		}
		http.Redirect(w, r, "/?query=x", http.StatusFound)
	})
	emitted, errors := 0, int64(0)
	for round := 0; round < 5; round++ {
		c := New(Config{Workers: 20, Retries: 0, Timeout: 5 * time.Second})
		in := make(chan string, 64)
		out := make(chan Result, 64)
		for i := 0; i < 40; i++ {
			in <- fmt.Sprintf("name%02d", i)
		}
		close(in)
		ctx, cancel := context.WithCancel(context.Background())
		go func() { time.Sleep(150 * time.Millisecond); cancel() }()
		c.Run(ctx, in, out)
		close(out)
		for r := range out {
			if r.Status == StatusError {
				emitted++
			}
		}
		errors += c.Snapshot().Errors
		c.Close()
	}
	if emitted > 0 || errors > 0 {
		t.Errorf("over 5 cancelled runs: %d interrupted checks were emitted as errors, Stats.Errors=%d", emitted, errors)
	}
}

// read_limit came straight from the REST API into make([]byte, n). A huge
// value panicked inside a worker goroutine and took the whole web process
// down, with every other worker.
func TestHugeReadLimitDoesNotPanic(t *testing.T) {
	withServer(t, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `<span class="tm-section-header-status tm-status-unavail">Sold</span>`)
	})
	c := New(Config{ReadLimit: 1 << 62, Retries: 0})
	defer c.Close()
	defer func() {
		if p := recover(); p != nil {
			t.Errorf("CheckOne panicked: %v", p)
		}
	}()
	if r := c.CheckOne(context.Background(), "adam"); r.Status != StatusSold {
		t.Errorf("status = %s (%s), want sold", r.Status, r.Error)
	}
}

func TestNormalizeConfigClampsUpperBounds(t *testing.T) {
	c := normalizeConfig(Config{Workers: 1 << 30, MaxConns: 1 << 30, RateRPS: 1 << 62, Retries: 1 << 20, ReadLimit: 1 << 62})
	if c.Workers != MaxWorkers || c.MaxConns != MaxConnLimit || c.RateRPS != MaxRateRPS ||
		c.Retries != MaxRetries || c.ReadLimit != MaxReadLimit {
		t.Errorf("not clamped: %+v", c)
	}
	if err := (Config{ReadLimit: 1 << 62}).Validate(); err == nil {
		t.Error("Validate accepted read_limit=1<<62")
	}
	if err := DefaultConfig().Validate(); err != nil {
		t.Errorf("Validate rejected the default config: %v", err)
	}
}

// waitBlock had a loop meant to re-read the shared deadline, but both
// select branches returned, so a cooldown extended by another worker while
// we slept was ignored and the request went out during the pause.
func TestWaitBlockHonoursExtendedCooldown(t *testing.T) {
	c := New(Config{BlockCooldown: 50 * time.Millisecond})
	defer c.Close()
	start := time.Now()
	c.triggerBlock() // deadline = start + 50ms
	done := make(chan time.Duration, 1)
	go func() {
		_ = c.waitBlock(context.Background())
		done <- time.Since(start)
	}()
	time.Sleep(10 * time.Millisecond)
	c.cfg.BlockCooldown = 300 * time.Millisecond
	c.triggerBlock() // another worker saw "302 /": deadline = start + ~310ms
	if got := <-done; got < 250*time.Millisecond {
		t.Errorf("waitBlock returned after %v, but the shared cooldown was extended to ~310ms", got.Round(time.Millisecond))
	}
}

// README: "A stale sold/taken entry is used as a fallback when the network
// fails." The code served any stale status, so a three-day-old auction (or
// "for sale") came back as the current answer when Fragment was unreachable.
func TestStaleAuctionNotUsedAsFallback(t *testing.T) {
	srv := withServer(t, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `<h2><span class="tm-section-header-status tm-status-avail">On auction</span></h2>`+
			`<time class="tm-countdown-timer">2 days</time><button class="js-place-bid-btn">Place bid</button>`)
	})
	c := New(Config{CacheDir: t.TempDir(), Retries: 0, RetryDelay: time.Millisecond, Timeout: time.Second})
	defer c.Close()
	if r := c.CheckOne(context.Background(), "oldauction"); r.Status != StatusAuction {
		t.Fatalf("first check = %+v", r)
	}
	// Age the entry by three days.
	_, meta := c.cache.paths("oldauction")
	raw, err := os.ReadFile(meta)
	if err != nil {
		t.Fatal(err)
	}
	var ent cacheEntry
	if err := json.Unmarshal(raw, &ent); err != nil {
		t.Fatal(err)
	}
	ent.FetchedAt = time.Now().Add(-72 * time.Hour)
	raw, _ = json.Marshal(ent)
	if err := os.WriteFile(meta, raw, 0o644); err != nil {
		t.Fatal(err)
	}

	srv.Close() // network down
	r := c.CheckOne(context.Background(), "oldauction")
	if r.Status != StatusError || r.Stale {
		t.Errorf("3-day-old auction served as %q (stale=%v, err=%q), want an error", r.Status, r.Stale, r.Error)
	}
}

// The textarea path (ParseNames) accepts any separator layout, but the file
// upload path (ParseNamesReader) read lines with a 1 MiB limit: a
// comma-separated export on one line gave 0 names and "token too long".
func TestParseNamesReaderLongSingleLine(t *testing.T) {
	parts := make([]string, 150000)
	for i := range parts {
		parts[i] = fmt.Sprintf("user%06d", i)
	}
	blob := strings.Join(parts, ",") // ~1.6 MB, one line
	a, _ := ParseNames(blob)
	b, _, err := ParseNamesReader(strings.NewReader(blob))
	if err != nil || len(b) != len(a) || len(a) != len(parts) {
		t.Errorf("ParseNames found %d names; ParseNamesReader found %d, err=%v", len(a), len(b), err)
	}
}

// ParseNamesReader must give exactly what ParseNames gives.
func FuzzParseNamesReaderMatchesParseNames(f *testing.F) {
	for _, seed := range []string{
		"alice\nbob\r\ncarol", "@Alice, @alice;BOB\tdan", "  ", "a\vb c\u00a0d",
		"bad-name ok_name x", strings.Repeat("a", 5000) + " b", "ab\xffcd ef", "@@x @ y",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, blob string) {
		a, ra := ParseNames(blob)
		b, rb, err := ParseNamesReader(strings.NewReader(blob))
		if err != nil {
			t.Fatal(err)
		}
		if len(a) == 0 {
			a = nil
		}
		if len(b) == 0 {
			b = nil
		}
		if !reflect.DeepEqual(a, b) || ra != rb {
			t.Errorf("ParseNames %q -> %v (rejected %d), ParseNamesReader -> %v (rejected %d)", blob, a, ra, b, rb)
		}
		// The panel's upload path keeps the names in a NameList instead.
		list, rc, err := CollectNames(strings.NewReader(blob))
		if err != nil {
			t.Fatal(err)
		}
		var c []string
		_ = list.Each(func(name string) error { c = append(c, name); return nil })
		if !reflect.DeepEqual(a, c) || ra != rc || list.Len() != len(c) {
			t.Errorf("ParseNames %q -> %v (rejected %d), CollectNames -> %v (rejected %d, Len %d)", blob, a, ra, c, rc, list.Len())
		}
	})
}

// With a rate limit set, a second Close panicked: "close of closed channel".
func TestCheckerCloseTwiceDoesNotPanic(t *testing.T) {
	c := New(Config{RateRPS: 5})
	c.Close()
	defer func() {
		if p := recover(); p != nil {
			t.Errorf("second Close panicked: %v", p)
		}
	}()
	c.Close()
}

// runNames runs a Checker over names and returns what it emitted and the
// names it left unread.
func runNames(t *testing.T, cfg Config, names []string) (c *Checker, results []Result, unread int) {
	t.Helper()
	c = New(cfg)
	t.Cleanup(c.Close)
	in := make(chan string, len(names))
	for _, n := range names {
		in <- n
	}
	close(in)
	out := make(chan Result, len(names))
	c.Run(context.Background(), in, out)
	close(out)
	for r := range out {
		results = append(results, r)
	}
	return c, results, len(in)
}

func seq(prefix string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("%s%03d", prefix, i)
	}
	return out
}

// Since 2026-11-08 Fragment answers every /username/ request from this
// client with HTTP 403 and "cf-mitigated: challenge". That was a plain
// "HTTP 403" error and the run went on: 300 names "done" in two seconds,
// every one an error, and Resume treated them as checked.
func TestCloudflareBlockStopsRun(t *testing.T) {
	var hits atomic.Int64
	withServer(t, func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Cf-Mitigated", "challenge")
		w.WriteHeader(http.StatusForbidden)
		fmt.Fprint(w, `<html><head><title>Just a moment...</title></head></html>`)
	})
	cfg := DefaultConfig()
	cfg.Workers = 4
	cfg.Retries = 2
	cfg.RetryDelay = time.Millisecond
	c, results, unread := runNames(t, cfg, seq("name", 300))
	if !c.Blocked() {
		t.Fatal("Blocked() = false after 300 challenges")
	}
	// Until a first answer that is not a challenge, requests go out one at
	// a time, so a client that is blocked from the start sends exactly
	// ChallengeLimit of them (it used to send one per worker at once).
	if h := hits.Load(); h != ChallengeLimit {
		t.Errorf("%d requests, want %d", h, ChallengeLimit)
	}
	// Only names that got a challenge come back (as errors); the ones never
	// sent stay in the input for a later resume.
	if len(results) == 0 || len(results) > ChallengeLimit || unread+len(results) >= 300 && unread == 0 {
		t.Errorf("results=%d unread=%d: the run did not stop early", len(results), unread)
	}
	for _, r := range results {
		if r.Status != StatusError || !strings.Contains(r.Error, "cloudflare challenge (HTTP 403)") {
			t.Errorf("%s: %s (%s), want an error naming the challenge", r.Username, r.Status, r.Error)
		}
	}
	if got := c.Snapshot().Challenges; got != hits.Load() {
		t.Errorf("Stats.Challenges = %d, want %d", got, hits.Load())
	}
}

// A challenge now and then is not a block: the count of challenges in a
// row starts over with every other answer.
func TestChallengeRunResetsOnAnswer(t *testing.T) {
	withServer(t, func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(r.URL.Path, "/username/")
		if strings.HasPrefix(name, "chal") {
			w.Header().Set("Cf-Mitigated", "challenge")
			w.WriteHeader(http.StatusForbidden)
			return
		}
		http.Redirect(w, r, "/?query="+name, http.StatusFound)
	})
	var names []string
	for i := 0; i < 15; i++ {
		names = append(names, fmt.Sprintf("chal%03d", i), fmt.Sprintf("free%03d", i))
	}
	cfg := DefaultConfig()
	cfg.Workers = 1
	cfg.Retries = 2 // three challenges in a row per "chal" name, below the limit
	cfg.RetryDelay = time.Millisecond
	c, results, unread := runNames(t, cfg, names)
	if c.Blocked() || unread != 0 || len(results) != len(names) {
		t.Errorf("blocked=%v results=%d unread=%d, want every name checked", c.Blocked(), len(results), unread)
	}
}

// A proxy in front of Fragment may drop the cf-mitigated header; the
// challenge page in a 403 or 503 still gives it away. A plain 403 is not
// a challenge.
func TestChallengePageWithoutHeader(t *testing.T) {
	withServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		if strings.HasSuffix(r.URL.Path, "chal") {
			fmt.Fprint(w, `<html><head><title>Just a moment...</title></head><script>window._cf_chl_opt={}</script></html>`)
		}
	})
	cfg := DefaultConfig()
	cfg.Retries = 0
	c := New(cfg)
	defer c.Close()
	if r := c.CheckOne(context.Background(), "pagechal"); !strings.Contains(r.Error, "cloudflare challenge (HTTP 403)") {
		t.Errorf("403 with a challenge page: %+v", r)
	}
	if r := c.CheckOne(context.Background(), "plain"); r.Error != "HTTP 403" {
		t.Errorf("plain 403: %+v, want HTTP 403", r)
	}
	if got := c.Snapshot().Challenges; got != 1 {
		t.Errorf("Stats.Challenges = %d, want 1", got)
	}
}

// The transport had fixed 10 s dial, TLS and response-header timeouts, so
// Config.Timeout (the console's -timeout) above 10 s did not help a slow
// Fragment: -timeout 30s gave up after 10 s with "timeout awaiting
// response headers". TestSlowHeadersWithinTimeout is the end-to-end case.
func TestTransportTimeoutsFollowConfig(t *testing.T) {
	for _, d := range []time.Duration{2 * time.Second, 30 * time.Second, 90 * time.Second} {
		c := New(Config{Timeout: d})
		tr := c.client.Transport.(*http.Transport)
		if tr.ResponseHeaderTimeout != d || tr.TLSHandshakeTimeout != d || c.client.Timeout != d {
			t.Errorf("Timeout %v: header %v, TLS %v, client %v", d, tr.ResponseHeaderTimeout, tr.TLSHandshakeTimeout, c.client.Timeout)
		}
		c.Close()
	}
}

// End to end: headers that come after 10.5 s are fine with a 13 s timeout.
func TestSlowHeadersWithinTimeout(t *testing.T) {
	if testing.Short() {
		t.Skip("takes 10 s")
	}
	withServer(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(10500 * time.Millisecond):
		case <-r.Context().Done():
			return
		}
		http.Redirect(w, r, "/?query=slowname", http.StatusFound)
	})
	c := New(Config{Timeout: 13 * time.Second, Retries: 0})
	defer c.Close()
	if r := c.CheckOne(context.Background(), "slowname"); r.Status != StatusFree {
		t.Errorf("headers after 10.5 s with a 13 s timeout: %s (%s), want free", r.Status, r.Error)
	}
}

// Both modes read lists with ScanNames. The console used to take one name
// per line ("alice, bob" was a single invalid name), the web panel split
// on spaces, commas and semicolons.
func TestScanNamesRules(t *testing.T) {
	cases := map[string][]string{
		"alice, bobby;carol\tdave\r\n@Erin  frank\n":   {"alice", "bobby", "carol", "dave", "erin", "frank"},
		"@alice | free\n@bobby | error (HTTP 403)\nca": {"alice", "bobby", "ca"}, // a results file reads back as its names
		"one|two three\nfour":                          {"one", "four"},
		"":                                             nil,
		"@ , ;\n":                                      nil,
	}
	for in, want := range cases {
		var got []string
		if err := ScanNames(strings.NewReader(in), func(n string) error { got = append(got, n); return nil }); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("ScanNames(%q) = %q, want %q", in, got, want)
		}
	}
	long := strings.Repeat("x", 3*maxNameToken)
	var got []string
	_ = ScanNames(strings.NewReader(long+",durov"), func(n string) error { got = append(got, n); return nil })
	if len(got) != 2 || len(got[0]) != maxNameToken || got[1] != "durov" {
		t.Errorf("a %d-byte token: got %d tokens, first %d bytes", len(long), len(got), len(got[0]))
	}
}

// The limiter was a bucket of rps tokens filled by a ticker. After an idle
// period (a throttle cool-down) the bucket was full, and the requests that
// had waited went out at once: 21 in the first 100 ms after a 3 s pause
// at -rate 10. Starts keep the 1/rate spacing after an idle period now.
func TestRateLimiterSpacesStartsAfterIdle(t *testing.T) {
	rl := newRateLimiter(20) // one start per 50 ms
	defer rl.Close()
	ctx := context.Background()
	if err := rl.Wait(ctx, nil); err != nil {
		t.Fatal(err)
	}
	time.Sleep(400 * time.Millisecond) // a bucket would hold 8 tokens now
	begin := time.Now()
	var mu sync.Mutex
	var starts []time.Duration
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := rl.Wait(ctx, nil); err != nil {
				t.Error(err)
			}
			mu.Lock()
			starts = append(starts, time.Since(begin))
			mu.Unlock()
		}()
	}
	wg.Wait()
	sort.Slice(starts, func(i, j int) bool { return starts[i] < starts[j] })
	// A start can be late (a busy machine) but never early: the k-th one
	// is due k*50 ms after begin.
	for k, d := range starts {
		if due := time.Duration(k)*50*time.Millisecond - 5*time.Millisecond; d < due {
			t.Errorf("start %d after %v, due no earlier than %v (starts %v)", k, d, due, starts)
			break
		}
	}
	// Stop gives up the wait at once.
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	for i := 0; i < 3; i++ {
		if err := rl.Wait(cctx, nil); err == nil && i > 0 {
			t.Errorf("Wait %d with a cancelled context returned nil", i)
		}
	}
}

// -rate was taken once per name before the check: the retry of a name went
// out right after its first try, on top of the limit, and names that need
// no request (invalid ones, answers from the cache) used up a start each.
// Every request takes a start now, and only requests do.
func TestRateCountsRequestsNotNames(t *testing.T) {
	var mu sync.Mutex
	var hits []time.Duration
	tries := map[string]int{}
	begin := time.Now()
	withServer(t, func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(r.URL.Path, "/username/")
		mu.Lock()
		hits = append(hits, time.Since(begin))
		tries[name]++
		first := tries[name] == 1
		mu.Unlock()
		if first {
			w.WriteHeader(http.StatusServiceUnavailable) // retried after 1 ms
			return
		}
		http.Redirect(w, r, "/?query="+name, http.StatusFound)
	})
	c := New(Config{Workers: 4, RateRPS: 20, Retries: 1, RetryDelay: time.Millisecond, Timeout: 2 * time.Second})
	defer c.Close()
	in := make(chan string, 64)
	for i := 0; i < 5; i++ {
		in <- fmt.Sprintf("valid%d", i) // two requests each
	}
	for i := 0; i < 20; i++ {
		in <- fmt.Sprintf("1bad%d", i) // invalid: no request
	}
	close(in)
	out := make(chan Result, 64)
	c.Run(context.Background(), in, out)
	took := time.Since(begin)
	close(out)
	free := 0
	for r := range out {
		if r.Status == StatusFree {
			free++
		}
	}
	if free != 5 || len(hits) != 10 {
		t.Fatalf("%d free and %d requests, want 5 and 10", free, len(hits))
	}
	sort.Slice(hits, func(i, j int) bool { return hits[i] < hits[j] })
	for k, d := range hits {
		if due := hits[0] + time.Duration(k)*50*time.Millisecond - 5*time.Millisecond; d < due {
			t.Errorf("request %d at %v, due no earlier than %v: a retry went past -rate (%v)", k, d, due, hits)
			break
		}
	}
	// 10 requests at 20/s take ~450 ms; a start for each of the 25 names
	// took ~1.2 s.
	if took > 900*time.Millisecond {
		t.Errorf("run took %v; the 20 invalid names must not wait for starts", took)
	}
}

// Until the first answer, requests go out one at a time. A request that had
// queued for that first slot stayed in the queue after the first answer
// opened the gate, so the requests of the start still went one by one: on
// the local stand-in, with the second request slow (2 s), 30 names took
// 4.0 s instead of 2.2 s. Waiters wake up when the gate opens now.
func TestGateWakesWaitersWhenItOpens(t *testing.T) {
	var n atomic.Int32
	withServer(t, func(w http.ResponseWriter, r *http.Request) {
		if n.Add(1) > 1 {
			time.Sleep(500 * time.Millisecond) // every request but the first is slow
		}
		name := strings.TrimPrefix(r.URL.Path, "/username/")
		http.Redirect(w, r, "/?query="+name, http.StatusFound)
	})
	// MaxConns 8: the test server speaks HTTP/1.1, one request per
	// connection at a time.
	c := New(Config{Workers: 5, MaxConns: 8, Retries: 0, Timeout: 5 * time.Second})
	defer c.Close()
	in := make(chan string, 5)
	for i := 0; i < 5; i++ {
		in <- fmt.Sprintf("gatename%d", i)
	}
	close(in)
	out := make(chan Result, 5)
	start := time.Now()
	c.Run(context.Background(), in, out)
	took := time.Since(start)
	close(out)
	for r := range out {
		if r.Status != StatusFree {
			t.Fatalf("%s: %s %s", r.Username, r.Status, r.Error)
		}
	}
	// One fast request, then four slow ones together: ~0.5 s. In single
	// file the four took ~2 s.
	if took > 1200*time.Millisecond {
		t.Errorf("5 names took %v; after the first answer the waiting requests must go together", took)
	}
}

// A result had one figure, Took, the whole time of the name, so the -debug
// timings mixed the network with the queue: a retry delay, a -rate start,
// a pause. Net and Wait split it now, and Attempts counts the tries.
func TestResultSplitsNetworkAndWaiting(t *testing.T) {
	var n atomic.Int32
	withServer(t, func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(r.URL.Path, "/username/")
		if name == "splitname" && n.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable) // retried after RetryDelay
			return
		}
		if name == "splitname" {
			time.Sleep(80 * time.Millisecond)
		}
		http.Redirect(w, r, "/?query="+name, http.StatusFound)
	})
	c := New(Config{Workers: 1, Retries: 1, RetryDelay: 300 * time.Millisecond, Timeout: 2 * time.Second})
	defer c.Close()
	res := c.CheckOne(context.Background(), "splitname")
	if res.Status != StatusFree || res.Attempts != 2 {
		t.Fatalf("%s after %d tries (%s), want free after 2", res.Status, res.Attempts, res.Error)
	}
	if res.Net < 80*time.Millisecond || res.Net > 250*time.Millisecond {
		t.Errorf("Net = %v, want the ~80 ms of the two requests", res.Net)
	}
	if res.Wait < 280*time.Millisecond {
		t.Errorf("Wait = %v, want the 300 ms retry delay in it", res.Wait)
	}
	if res.Took != res.Net+res.Wait {
		t.Errorf("Took %v != Net %v + Wait %v", res.Took, res.Net, res.Wait)
	}
	if inv := c.CheckOne(context.Background(), "1bad"); inv.Net != 0 || inv.Attempts != 0 {
		t.Errorf("invalid name: Net %v, %d tries; want no request", inv.Net, inv.Attempts)
	}

	// A -rate start is waiting too: with one worker at 4/s each name after
	// the first waits ~250 ms for its start.
	r := New(Config{Workers: 1, RateRPS: 4, Retries: 0, Timeout: 2 * time.Second})
	defer r.Close()
	in := make(chan string, 3)
	in <- "ratename0"
	in <- "ratename1"
	in <- "ratename2"
	close(in)
	out := make(chan Result, 3)
	r.Run(context.Background(), in, out)
	close(out)
	waited := 0
	for res := range out {
		if res.Net > 100*time.Millisecond {
			t.Errorf("%s: Net %v for an instant answer: the wait for the start leaked into it", res.Username, res.Net)
		}
		if res.Wait >= 200*time.Millisecond {
			waited++
		}
	}
	if waited < 2 {
		t.Errorf("%d names waited >= 200 ms for a start, want 2", waited)
	}
}

// logCapture collects Debugf and Tracef lines.
type logCapture struct {
	mu    sync.Mutex
	lines []string
}

func (l *logCapture) add(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, fmt.Sprintf(format, args...))
}

func (l *logCapture) text() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Join(l.lines, "\n")
}

// -debug printed the HTTP code and the verdict of a try but no timings (the
// timing line existed only at -trace), tries of one name were not told
// apart, and the shared throttle probe printed nothing at any level. Now
// every try ends with one line: its number, the outcome, the waits before
// it and the phases of the request; the probe gets the same line.
func TestDebugLinePerTryAndProbe(t *testing.T) {
	var n atomic.Int32
	withServer(t, func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(r.URL.Path, "/username/")
		switch {
		case strings.HasPrefix(name, "zzprobe"):
			http.Redirect(w, r, "/?query="+name, http.StatusFound)
		case name == "rsvname":
			http.Redirect(w, r, "/", http.StatusFound)
		case n.Add(1) == 1:
			w.WriteHeader(http.StatusServiceUnavailable)
		default:
			http.Redirect(w, r, "/?query="+name, http.StatusFound)
		}
	})
	var debug logCapture
	c := New(Config{Workers: 1, Retries: 1, RetryDelay: time.Millisecond, Timeout: 2 * time.Second})
	defer c.Close()
	c.SetLogger(Logger{Debugf: debug.add}) // -debug only, no -trace
	if res := c.CheckOne(context.Background(), "retryname"); res.Status != StatusFree {
		t.Fatalf("retryname: %s %s", res.Status, res.Error)
	}
	if res := c.CheckOne(context.Background(), "rsvname"); res.Status != StatusReserved {
		t.Fatalf("rsvname: %s %s", res.Status, res.Error)
	}
	out := debug.text()
	for _, want := range []string{
		"@retryname #1 | -> error (will retry): HTTP 503 | 503 HTTP/1.1 |",
		"@retryname #2 | -> free | 302 HTTP/1.1 |",
		"@rsvname #1 | -> redirect to / (reserved or throttled) |",
		"| -> healthy | 302 HTTP/1.1 |",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("no %q in the -debug output:\n%s", want, out)
		}
	}
	if !strings.Contains(out, "probe zzprobe") || !strings.Contains(out, "net=") || !strings.Contains(out, "ttfb=") {
		t.Errorf("the -debug lines lack the probe or the timings:\n%s", out)
	}
}

// -trace printed every header of an answer as it was, so the session
// cookie (Set-Cookie: stel_ssid=...) landed in the log. Values are hidden
// now, in the answer and in the request (only the names of the cookies
// sent are shown), and the request itself is printed.
func TestTraceHidesCookieValues(t *testing.T) {
	withServer(t, func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(r.URL.Path, "/username/")
		http.SetCookie(w, &http.Cookie{Name: "stel_ssid", Value: "SECRET123", Path: "/", HttpOnly: true})
		http.Redirect(w, r, "/?query="+name, http.StatusFound)
	})
	var lines logCapture
	c := New(Config{Workers: 1, Timeout: 2 * time.Second})
	defer c.Close()
	c.SetLogger(Logger{Debugf: lines.add, Tracef: lines.add})
	_ = c.CheckOne(context.Background(), "cookiename")
	_ = c.CheckOne(context.Background(), "cookiename2") // sends the cookie back
	out := lines.text()
	if strings.Contains(out, "SECRET123") {
		t.Fatalf("a cookie value reached the trace:\n%s", out)
	}
	for _, want := range []string{"Set-Cookie: stel_ssid=…; Path=/; HttpOnly", "-> GET http", "User-Agent: ",
		"cookies: none", "cookies: stel_ssid (values hidden)"} {
		if !strings.Contains(out, want) {
			t.Errorf("no %q in the trace:\n%s", want, out)
		}
	}
}

func TestMaskSetCookies(t *testing.T) {
	got := maskSetCookies([]string{"a=1; Path=/", "b=two", "noequals; Secure", ""})
	want := []string{"a=…; Path=/", "b=…", "noequals; Secure", ""}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("maskSetCookies = %q, want %q", got, want)
	}
}
