// Regression tests for bugs found in the v4 review. Each test describes the
// old behaviour it guards against.
package cli

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/cyberanchor/frgmnt/internal/checker"
	"github.com/cyberanchor/frgmnt/internal/tracelog"
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

// -resume skipped every name in the results file, also the ones that only
// have an error: "error (cancelled)" lines of older versions, and every
// "error (HTTP 403)" of a run that Cloudflare blocked. An error is not a
// verdict, so those names must be checked again.
func TestResumeRechecksErrorLines(t *testing.T) {
	path := filepath.Join(t.TempDir(), "results.txt")
	body := "@alice | free\n@bob | error (cancelled)\n@carol | error (HTTP 403)\n" +
		"@dave | error (cloudflare challenge (HTTP 403))\n@dave | taken\n"
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	set, err := loadResumeSet(path)
	if err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]bool{"alice": true, "bob": false, "carol": false, "dave": true} {
		if got := set.Has(name); got != want {
			t.Errorf("%s in the resume set: %v, want %v (set %v)", name, got, want, set)
		}
	}
}

// A blocked run (every answer a Cloudflare challenge) went through the
// whole list in seconds, wrote an error for every name and exited with 0.
// Now it stops after checker.ChallengeLimit challenges in a row, exits
// with 3, and -resume later checks every name without a verdict.
func TestBlockedRunExitCodeAndResume(t *testing.T) {
	var blocked atomic.Bool
	blocked.Store(true)
	srv := newHitServer(t, func(w http.ResponseWriter, r *http.Request, name string) {
		if blocked.Load() {
			w.Header().Set("Cf-Mitigated", "challenge")
			w.WriteHeader(http.StatusForbidden)
			return
		}
		freeHandler(w, r, name)
	})
	dir := t.TempDir()
	in := filepath.Join(dir, "names.txt")
	out := filepath.Join(dir, "results.txt")
	var b strings.Builder
	for i := 0; i < 200; i++ {
		fmt.Fprintf(&b, "name%03d\n", i)
	}
	if err := os.WriteFile(in, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	args := []string{"-input", in, "-output", out, "-base-url", srv.URL, "-no-cache", "-quiet",
		"-workers", "4", "-retries", "0"}
	if code := Main(args); code != 3 {
		t.Errorf("blocked run: exit code %d, want 3", code)
	}
	raw, _ := os.ReadFile(out)
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(lines) >= 200 {
		t.Errorf("blocked run wrote %d lines, want it to stop early", len(lines))
	}

	blocked.Store(false)
	if code := Main(append(args, "-resume")); code != 0 {
		t.Errorf("resume: exit code %d, want 0", code)
	}
	raw, _ = os.ReadFile(out)
	free := map[string]int{}
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		if name, status, ok := strings.Cut(line, " | "); ok && status == "free" {
			free[name]++
		}
	}
	for i := 0; i < 200; i++ {
		if n := free[fmt.Sprintf("@name%03d", i)]; n != 1 {
			t.Errorf("name%03d has %d free lines after -resume, want 1", i, n)
		}
	}
}

// runOne checks the single name "alice" with Main against a fake Fragment
// and returns the line written for it.
func runOne(t *testing.T, h func(w http.ResponseWriter, r *http.Request, name string), args ...string) (string, *hitServer) {
	t.Helper()
	srv := newHitServer(t, h)
	dir := t.TempDir()
	in := filepath.Join(dir, "names.txt")
	out := filepath.Join(dir, "results.txt")
	if err := os.WriteFile(in, []byte("alice\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	base := []string{"-input", in, "-output", out, "-base-url", srv.URL, "-no-cache", "-quiet",
		"-workers", "1", "-retry-delay", "1ms"}
	Main(append(base, args...))
	raw, _ := os.ReadFile(out)
	return strings.TrimSpace(string(raw)), srv
}

// The web engine treats a 200 Cloudflare interstitial as a retryable error;
// the console engine reported it as a final "unknown" after one request.
func TestChallengeIsRetriedAsError(t *testing.T) {
	line, srv := runOne(t, func(w http.ResponseWriter, r *http.Request, name string) {
		fmt.Fprint(w, `<html><head><title>Just a moment...</title></head><script>window._cf_chl_opt={}</script></html>`)
	}, "-retries", "2")
	if !strings.HasPrefix(line, "@alice | error") || srv.count("alice") != 3 {
		t.Errorf("challenge page: %q after %d requests, want an error after 3", line, srv.count("alice"))
	}
}

// The web engine reads the Location of 301/302/303/307/308; the console
// engine only of 301/302, so a 307 to /?query= (free) became "HTTP 307".
func TestRedirectCodesLikeWeb(t *testing.T) {
	for _, code := range []int{http.StatusSeeOther, http.StatusTemporaryRedirect, http.StatusPermanentRedirect} {
		line, _ := runOne(t, func(w http.ResponseWriter, r *http.Request, name string) {
			http.Redirect(w, r, "/?query="+name, code)
		}, "-retries", "0")
		if line != "@alice | free" {
			t.Errorf("%d -> /?query=: got %q, want @alice | free", code, line)
		}
	}
}

// -retries was not validated: with -1 the attempt loop never ran, no request
// was made and every name became "error (retries exhausted: )".
func TestNegativeRetriesStillMakesARequest(t *testing.T) {
	line, srv := runOne(t, freeHandler, "-retries", "-1")
	if srv.count("alice") != 1 || line != "@alice | free" {
		t.Errorf("retries=-1: %d requests, line %q; want 1 request and free", srv.count("alice"), line)
	}
}

// -readlimit went straight into make([]byte, n): a huge value panicked.
func TestHugeReadLimitFlagIsClamped(t *testing.T) {
	srv := newHitServer(t, func(w http.ResponseWriter, r *http.Request, name string) {
		fmt.Fprint(w, `<span class="tm-section-header-status tm-status-unavail">Sold</span>`)
	})
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
	page := func(label, class, extra string) string {
		return `<html><body><h2><span class="tm-section-header-status tm-status-` + class + `">` +
			label + `</span></h2>` + extra + `</body></html>`
	}
	pages := map[string]string{
		"oldauction": page("On auction", "avail", `<button class="js-place-bid-btn">Place bid</button>`),
		"oldsold":    page("Sold", "unavail", `<div>Purchased on 19 Nov 2022</div>`),
	}
	srv := newHitServer(t, func(w http.ResponseWriter, r *http.Request, name string) {
		fmt.Fprint(w, pages[name])
	})
	dir := t.TempDir()
	in := filepath.Join(dir, "names.txt")
	out := filepath.Join(dir, "results.txt")
	cacheDir := filepath.Join(dir, "cache")
	if err := os.WriteFile(in, []byte("oldauction\noldsold\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	// Every entry is stale at once.
	args := []string{"-input", in, "-output", out, "-cache-dir", cacheDir, "-quiet", "-workers", "1",
		"-retries", "0", "-cache-ttl", "1ns", "-cache-sold-ttl", "1ns"}
	if code := Main(append(args, "-base-url", srv.URL)); code != 0 {
		t.Fatalf("first run (fills the cache) exit=%d", code)
	}
	srv.Close() // network down
	time.Sleep(time.Millisecond)
	Main(append(args, "-base-url", srv.URL))
	raw, _ := os.ReadFile(out)
	got := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		if name, rest, ok := strings.Cut(line, " | "); ok {
			got[name] = rest
		}
	}
	if !strings.HasPrefix(got["@oldauction"], "error") {
		t.Errorf("stale auction with the network down = %q, want an error", got["@oldauction"])
	}
	if s := got["@oldsold"]; !strings.HasPrefix(s, "sold") || !strings.Contains(s, "stale cache") {
		t.Errorf("stale sold with the network down = %q, want sold marked as stale cache", s)
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

// A typo in -filter was reported only after the output file had been opened
// with O_TRUNC, so "-filter fre" wiped the results of the previous run and
// then exited.
func TestFilterTypoKeepsOutputFile(t *testing.T) {
	dir := t.TempDir()
	in := filepath.Join(dir, "names.txt")
	out := filepath.Join(dir, "results.txt")
	const old = "@alice | free\n@bobby | taken\n"
	if err := os.WriteFile(in, []byte("alice\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(out, []byte(old), 0o644); err != nil {
		t.Fatal(err)
	}
	code := Main([]string{"-input", in, "-output", out, "-filter", "fre", "-no-cache", "-base-url", "http://127.0.0.1:1"})
	if code == 0 {
		t.Error("an unknown -filter status exited with 0")
	}
	if got, _ := os.ReadFile(out); string(got) != old {
		t.Errorf("results file after the failed run: %q, want it unchanged", got)
	}
}

// -input and -output pointing at one file: opening the output truncated
// the list before the first name was read, so the run checked 0 names,
// exited with 0 and left an empty file.
func TestSameInputAndOutputRefused(t *testing.T) {
	dir := t.TempDir()
	in := filepath.Join(dir, "names.txt")
	const list = "alice\nbobby\n"
	if err := os.WriteFile(in, []byte(list), 0o644); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link.txt")
	if err := os.Symlink(in, link); err != nil {
		t.Fatal(err)
	}
	for _, out := range []string{in, filepath.Join(dir, ".", "names.txt"), link} {
		for _, extra := range [][]string{nil, {"-resume"}, {"-append"}} {
			args := append([]string{"-input", in, "-output", out, "-no-cache", "-base-url", "http://127.0.0.1:1"}, extra...)
			if code := Main(args); code == 0 {
				t.Errorf("-output %s %v: exit code 0, want an error", out, extra)
			}
			if got, _ := os.ReadFile(in); string(got) != list {
				t.Fatalf("-output %s %v: the input list became %q", out, extra, got)
			}
		}
	}
}

// Names that cannot be Telegram usernames ("ab", "1abcde", 40 letters)
// passed the old 1-64 character check and were sent to Fragment, which
// redirects them to /?query= like a free name: the results file said
// "free" for all of them. Now they get an "invalid" error and no request.
func TestImpossibleNamesAreNotReportedFree(t *testing.T) {
	srv := newHitServer(t, freeHandler)
	dir := t.TempDir()
	in := filepath.Join(dir, "names.txt")
	out := filepath.Join(dir, "results.txt")
	bad := []string{"ab", "abc", "1abcde", "_abcde", "abcde_", strings.Repeat("a", 40)}
	if err := os.WriteFile(in, []byte(strings.Join(append(bad, "kyqb"), "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	Main([]string{"-input", in, "-output", out, "-base-url", srv.URL, "-no-cache", "-quiet", "-workers", "2"})
	raw, _ := os.ReadFile(out)
	got := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		if name, rest, ok := strings.Cut(line, " | "); ok {
			got[strings.TrimPrefix(name, "@")] = rest
		}
	}
	for _, n := range bad {
		if !strings.HasPrefix(got[n], "error (invalid: ") {
			t.Errorf("%s: %q, want an invalid-name error", n, got[n])
		}
		if c := srv.count(n); c != 0 {
			t.Errorf("%s was requested %d times, want 0", n, c)
		}
	}
	if got["kyqb"] != "free" {
		t.Errorf("kyqb (a real 4-character collectible): %q, want free", got["kyqb"])
	}
}

// "-input -" on a pipe that stays open without data: Ctrl+C printed
// "shutting down gracefully" and Main never returned, because it waited
// for the producer, which sat in a read of stdin. A second Ctrl+C was
// swallowed too, so only kill -9 ended it.
func TestSIGINTWithIdleStdinReturns(t *testing.T) {
	srv := newHitServer(t, freeHandler)
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	defer w.Close() // open and idle until the end of the test
	if _, err := w.WriteString("alice\nbobby\n"); err != nil {
		t.Fatal(err)
	}
	oldStdin := os.Stdin
	os.Stdin = r
	defer func() { os.Stdin = oldStdin }()

	out := filepath.Join(t.TempDir(), "results.txt")
	done := make(chan int, 1)
	go func() {
		done <- Main([]string{"-input", "-", "-output", out, "-base-url", srv.URL, "-no-cache", "-quiet", "-workers", "2"})
	}()
	time.Sleep(500 * time.Millisecond) // both names are checked; the read of stdin blocks
	_ = syscall.Kill(os.Getpid(), syscall.SIGINT)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Main did not return within 5 s after Ctrl+C with an idle stdin")
	}
	raw, _ := os.ReadFile(out)
	if got := strings.Count(string(raw), "| free"); got != 2 {
		t.Errorf("results file after Ctrl+C: %q, want the 2 names checked before it", raw)
	}
}

// With -filter only the chosen statuses reach the results file, and
// -resume only knew the names in it: "-filter free -resume" checked every
// taken (or sold, ...) name again on each run. The names with a verdict
// are now listed in <output>.checked as well.
func TestFilterResumeSkipsFilteredNames(t *testing.T) {
	taken := `<html><body><h2><span class="tm-section-header-status tm-status-taken">Taken</span></h2>` +
		`<button class="js-make-offer-btn">Make an offer</button></body></html>`
	srv := newHitServer(t, func(w http.ResponseWriter, r *http.Request, name string) {
		if strings.HasPrefix(name, "taken") {
			fmt.Fprint(w, taken)
			return
		}
		freeHandler(w, r, name)
	})
	dir := t.TempDir()
	in := filepath.Join(dir, "names.txt")
	out := filepath.Join(dir, "free.txt")
	if err := os.WriteFile(in, []byte("takenone\nfreeone\ntakentwo\nfreetwo\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	args := []string{"-input", in, "-output", out, "-base-url", srv.URL, "-no-cache", "-quiet", "-workers", "2", "-filter", "free"}
	if code := Main(args); code != 0 {
		t.Fatalf("first run exit=%d", code)
	}
	if code := Main(append(args, "-resume")); code != 0 {
		t.Fatalf("resume exit=%d", code)
	}
	for _, n := range []string{"takenone", "freeone", "takentwo", "freetwo"} {
		if c := srv.count(n); c != 1 {
			t.Errorf("%s requested %d times over a run and a -resume, want 1", n, c)
		}
	}
	raw, _ := os.ReadFile(out)
	if got := strings.TrimSpace(string(raw)); strings.Count(got, "\n") != 1 || strings.Contains(got, "taken") {
		t.Errorf("results file %q, want the two free names only", got)
	}
}

// The console started 500 workers by default, ten times the web panel's
// 50: 500 requests at once from one IP with no rate limit.
func TestDefaultWorkersMatchWeb(t *testing.T) {
	if want := checker.DefaultConfig().Workers; defaultWorkers != want {
		t.Errorf("console default workers = %d, web default = %d", defaultWorkers, want)
	}
}

// The console read one name per line: "alice, bobby" was one name, written
// as "error (invalid: ...)", while the web panel read two names from the
// same text. A results file given as input checked "@name | status" as a
// name too.
func TestConsoleReadsListsLikeWeb(t *testing.T) {
	srv := newHitServer(t, freeHandler)
	dir := t.TempDir()
	in := filepath.Join(dir, "names.txt")
	out := filepath.Join(dir, "results.txt")
	if err := os.WriteFile(in, []byte("alice, bobby;carol\n@Dave | taken\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if code := Main([]string{"-input", in, "-output", out, "-base-url", srv.URL, "-no-cache", "-quiet", "-workers", "2"}); code != 0 {
		t.Fatalf("exit=%d", code)
	}
	raw, _ := os.ReadFile(out)
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	got := map[string]bool{}
	for _, l := range lines {
		got[l] = true
	}
	for _, want := range []string{"@alice | free", "@bobby | free", "@carol | free", "@dave | free"} {
		if !got[want] {
			t.Errorf("missing %q in %q", want, lines)
		}
	}
	if len(lines) != 4 {
		t.Errorf("%d lines, want 4: %q", len(lines), lines)
	}
}

// A run that Fragment kept throttling ("302 /" for every name and for the
// probe) wrote "persistent 302 /" errors for the names whose attempts ran
// out within one pause and went on. With -throttle-max it now stops when
// the throttling lasts that long, exits with 3, writes no lines for the
// names (no verdict), and -resume later checks all of them.
func TestThrottledRunExitCodeAndResume(t *testing.T) {
	var throttled atomic.Bool
	throttled.Store(true)
	srv := newHitServer(t, func(w http.ResponseWriter, r *http.Request, name string) {
		if throttled.Load() {
			http.Redirect(w, r, "/", http.StatusFound)
			return
		}
		freeHandler(w, r, name)
	})
	dir := t.TempDir()
	in := filepath.Join(dir, "names.txt")
	out := filepath.Join(dir, "results.txt")
	var b strings.Builder
	for i := 0; i < 50; i++ {
		fmt.Fprintf(&b, "name%03d\n", i)
	}
	if err := os.WriteFile(in, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	args := []string{"-input", in, "-output", out, "-base-url", srv.URL, "-no-cache", "-quiet",
		"-workers", "4", "-retries", "2", "-throttle-max", "300ms"}
	if code := Main(args); code != 3 {
		t.Errorf("throttled run: exit code %d, want 3", code)
	}
	raw, _ := os.ReadFile(out)
	if s := strings.TrimSpace(string(raw)); s != "" {
		t.Errorf("throttled run wrote %q; a throttled name has no verdict and no line", s)
	}

	throttled.Store(false)
	if code := Main(append(args, "-resume")); code != 0 {
		t.Errorf("resume: exit code %d, want 0", code)
	}
	raw, _ = os.ReadFile(out)
	free := 0
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		if _, status, ok := strings.Cut(line, " | "); ok && status == "free" {
			free++
		}
	}
	if free != 50 {
		t.Errorf("%d free lines after -resume, want 50", free)
	}
}

// The progress line divided by the names read so far: the producer runs
// at most 4096 names ahead of the workers, so a list of 9,995 names showed
// "1290/5396" (1290 checked + 4096 queued + 10 in the workers). A file is
// counted up front now, and the duplicates and -resume skips found so far
// are taken off; from a pipe the size is unknown and marked with "+".
func TestProgressShowsTheListSize(t *testing.T) {
	st := newStats()
	st.listNames = 10000
	atomic.StoreInt64(&st.total, 5396) // queued so far
	atomic.StoreInt64(&st.dropped, 5)  // duplicates seen so far
	if total, known := st.progressTotal(1290); total != 9995 || !known {
		t.Errorf("file: total %d (known %v), want 9995", total, known)
	}
	st.startTime = time.Now().Add(-100 * time.Second) // 1290 names in 100 s: 12.9/s
	line := st.snapshot(checker.Stats{Checked: 1290})
	if !strings.Contains(line, "1290/9995 (12.9 req/s, eta 11m)") {
		t.Errorf("progress line %q, want 1290/9995 with an eta of 11m", line)
	}

	pipe := newStats()
	atomic.StoreInt64(&pipe.total, 5396)
	if total, known := pipe.progressTotal(1290); total != 5396 || known {
		t.Errorf("pipe: total %d (known %v), want 5396 and unknown", total, known)
	}
	if line := pipe.snapshot(checker.Stats{Checked: 1290}); !strings.Contains(line, "1290/5396+") || strings.Contains(line, "eta") {
		t.Errorf("pipe progress line %q, want 1290/5396+ without an eta", line)
	}

	path := filepath.Join(t.TempDir(), "names.txt")
	if err := os.WriteFile(path, []byte("@alice, bob\ncarol | free\nalice\ndave"), 0o644); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if n, ok := countListNames(f); n != 5 || !ok {
		t.Errorf("countListNames = %d, %v; want 5 (alice twice)", n, ok)
	}
	rest, _ := io.ReadAll(f)
	if !strings.HasPrefix(string(rest), "@alice") {
		t.Errorf("file not rewound after counting: %q", rest)
	}

	for d, want := range map[time.Duration]string{45 * time.Second: "45s", 12 * time.Minute: "12m",
		3*time.Hour + 5*time.Minute: "3h05m"} {
		if got := formatETA(d); got != want {
			t.Errorf("formatETA(%v) = %q, want %q", d, got, want)
		}
	}
}

// lockedBuffer is a bytes.Buffer safe for the writer goroutine and the
// test to use together.
type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// Results reached the screen and the file only every 2 s (the flush
// ticker), so at network speed they showed up in bursts. A result is
// flushed as soon as no other one is waiting now.
func TestResultsAreWrittenWhenTheyArrive(t *testing.T) {
	var file, stdout lockedBuffer
	out := outputs{file: bufio.NewWriter(&file), stdout: bufio.NewWriter(&stdout)}
	results := make(chan checker.Result, 4)
	done := make(chan error, 1)
	go func() { done <- writeResults(results, out, nil, false, newStats()) }()
	results <- checker.Result{Username: "alice", Status: StatusFree}
	deadline := time.Now().Add(time.Second) // well before the 2 s ticker
	for !strings.Contains(stdout.String(), "@alice | free") || !strings.Contains(file.String(), "@alice | free") {
		if time.Now().After(deadline) {
			t.Fatalf("after 1 s: stdout %q, file %q; the result must be written at once", stdout.String(), file.String())
		}
		time.Sleep(5 * time.Millisecond)
	}
	close(results)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// The histogram of the -debug summary: quantiles at most ~9% high, never
// above the largest sample.
func TestDurHistQuantiles(t *testing.T) {
	var h durHist
	for i := 1; i <= 1000; i++ {
		h.add(time.Duration(i) * time.Millisecond)
	}
	for _, c := range []struct {
		q    float64
		want time.Duration
	}{{0.50, 500 * time.Millisecond}, {0.95, 950 * time.Millisecond}, {0.99, 990 * time.Millisecond}} {
		got := h.quantile(c.q)
		if got < c.want || got > c.want*110/100 || got > time.Second {
			t.Errorf("p%.0f = %v, want %v .. +10%% (and at most the max)", c.q*100, got, c.want)
		}
	}
	if h.count() != 1000 || h.quantile(1) != time.Second {
		t.Errorf("n=%d max=%v", h.count(), h.quantile(1))
	}
	var empty durHist
	if empty.quantile(0.5) != 0 {
		t.Error("empty histogram")
	}
	for d, want := range map[time.Duration]string{400 * time.Microsecond: "0.4ms", 37 * time.Millisecond: "37ms",
		1200 * time.Millisecond: "1.2s", 125 * time.Second: "2m05s"} {
		if got := fmtDur(d); got != want {
			t.Errorf("fmtDur(%v) = %q, want %q", d, got, want)
		}
	}
}

// The -debug summary gave avg/min/max of the whole time of a name
// ("Timing FREE"), which counted the wait for a -rate start as latency.
// It shows the network time and the waiting apart now.
func TestDebugSummaryShowsNetworkAndWaiting(t *testing.T) {
	srv := newHitServer(t, func(w http.ResponseWriter, r *http.Request, name string) { freeHandler(w, r, name) })
	dir := t.TempDir()
	in := filepath.Join(dir, "names.txt")
	if err := os.WriteFile(in, []byte("name0001\nname0002\nname0003\nname0004\nname0005\nname0006\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	log.SetOutput(&logs)
	defer log.SetOutput(os.Stderr)
	defer func(l LogLevel) { logLevel = l }(logLevel)
	code := Main([]string{"-input", in, "-output", filepath.Join(dir, "out.txt"), "-base-url", srv.URL,
		"-no-cache", "-workers", "2", "-rate", "5", "-debug"})
	if code != 0 {
		t.Fatalf("exit code %d", code)
	}
	out := logs.String()
	if strings.Contains(out, "Timing FREE") {
		t.Error("the old avg/min/max line is still printed")
	}
	pick := func(label string) time.Duration {
		t.Helper()
		m := regexp.MustCompile(regexp.QuoteMeta(label) + `[^\n]*?p50=([0-9.]+)(ms|s)`).FindStringSubmatch(out)
		if m == nil {
			t.Fatalf("no %q line in:\n%s", label, out)
		}
		v, _ := strconv.ParseFloat(m[1], 64)
		if m[2] == "s" {
			v *= 1000
		}
		return time.Duration(v * float64(time.Millisecond))
	}
	if net := pick("Network per name, redirects"); net > 100*time.Millisecond {
		t.Errorf("network p50 %v for a local server: the wait for starts is in it", net)
	}
	if wait := pick("Waiting per name"); wait < 150*time.Millisecond {
		t.Errorf("waiting p50 %v; at -rate 5 with 2 workers names wait for their start", wait)
	}
	if !strings.Contains(out, "Tries per name: 1: 6 |") {
		t.Errorf("no tries line for 6 names in:\n%s", out)
	}
}

// -trace writes the structured trace as JSON Lines: run_start with the
// settings, an event for every try (a retried name has two) and for every
// name, run_end with the counters; the file is private (0600) and -resume
// appends a second run to it.
func TestTraceFileJSONLines(t *testing.T) {
	var mu sync.Mutex
	tries := map[string]int{}
	srv := newHitServer(t, func(w http.ResponseWriter, r *http.Request, name string) {
		mu.Lock()
		tries[name]++
		n := tries[name]
		mu.Unlock()
		if name == "retryme" && n == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		freeHandler(w, r, name)
	})
	dir := t.TempDir()
	in := filepath.Join(dir, "names.txt")
	out := filepath.Join(dir, "results.txt")
	tracePath := filepath.Join(dir, "trace.jsonl")
	if err := os.WriteFile(in, []byte("alice\nretryme\ncarol\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	args := []string{"-input", in, "-output", out, "-base-url", srv.URL, "-no-cache", "-quiet",
		"-workers", "2", "-retry-delay", "10ms", "-trace", tracePath}
	if code := Main(args); code != 0 {
		t.Fatalf("exit code %d", code)
	}
	if fi, err := os.Stat(tracePath); err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("trace file: %v, mode %v; want 0600", err, fi.Mode().Perm())
	}
	read := func() []checker.TraceEvent {
		t.Helper()
		raw, err := os.ReadFile(tracePath)
		if err != nil {
			t.Fatal(err)
		}
		var events []checker.TraceEvent
		for i, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
			var e checker.TraceEvent
			if err := json.Unmarshal([]byte(line), &e); err != nil {
				t.Fatalf("line %d is not JSON: %v: %s", i+1, err, line)
			}
			events = append(events, e)
		}
		return events
	}
	events := read()
	if events[0].Kind != "run_start" || events[0].Info["version"] != Version || events[0].Info["workers"] != float64(2) {
		t.Errorf("first event: %+v", events[0])
	}
	if last := events[len(events)-1]; last.Kind != "run_end" || last.Info["checked"] != float64(3) || last.Info["exit_code"] != float64(0) {
		t.Errorf("last event: %+v", last)
	}
	count := map[string]int{}
	for _, e := range events {
		count[e.Kind+":"+e.Name]++
	}
	for _, want := range []string{"attempt:alice", "result:alice", "result:retryme", "attempt:carol", "result:carol"} {
		if count[want] != 1 {
			t.Errorf("%s: %d events, want 1 (%v)", want, count[want], count)
		}
	}
	if count["attempt:retryme"] != 2 {
		t.Errorf("retryme: %d attempt events, want 2", count["attempt:retryme"])
	}

	if code := Main(append(args, "-resume")); code != 0 {
		t.Fatalf("resume: exit code %d", code)
	}
	starts := 0
	for _, e := range read() {
		if e.Kind == "run_start" {
			starts++
		}
	}
	if starts != 2 {
		t.Errorf("%d run_start events after -resume, want 2 (appended)", starts)
	}
}

// At -debug the progress line shows the queues and what the workers do;
// at the normal level it stays as it was.
func TestProgressShowsThePipelineAtDebug(t *testing.T) {
	e := checker.Stats{Checked: 10, Busy: 5, InFlight: 2, WaitRate: 3}
	q := queues{in: func() int { return 4096 }, out: func() int { return 1 }}
	got := pipelineLine(e, q, 8)
	want := "queue in=4096 out=1 | workers net=2 gate=0 pause=0 rate=3 retry=0 probe=0 idle=3"
	if got != want {
		t.Errorf("pipelineLine = %q, want %q", got, want)
	}
	info := pipelineInfo(e, q, 8, 2.5)
	if info["queue_in"] != 4096 || info["wait_rate"] != int64(3) || info["inst_rps"] != 2.5 {
		t.Errorf("pipelineInfo = %v", info)
	}
}

// A run with -trace that lasts over a second has "pipeline" events.
func TestTraceFileHasPipelineEvents(t *testing.T) {
	srv := newHitServer(t, func(w http.ResponseWriter, r *http.Request, name string) { freeHandler(w, r, name) })
	dir := t.TempDir()
	in := filepath.Join(dir, "names.txt")
	tracePath := filepath.Join(dir, "trace.jsonl")
	if err := os.WriteFile(in, []byte("name0001\nname0002\nname0003\nname0004\nname0005\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if code := Main([]string{"-input", in, "-output", filepath.Join(dir, "out.txt"), "-base-url", srv.URL,
		"-no-cache", "-quiet", "-workers", "2", "-rate", "3", "-trace", tracePath}); code != 0 {
		t.Fatalf("exit code %d", code)
	}
	raw, err := os.ReadFile(tracePath)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		var e checker.TraceEvent
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			t.Fatalf("not JSON: %q", line)
		}
		if e.Kind == "pipeline" {
			n++
			if _, ok := e.Info["wait_rate"]; !ok || e.Info["workers"] != float64(2) {
				t.Errorf("pipeline info %v", e.Info)
			}
		}
	}
	if n == 0 {
		t.Error("no pipeline event in a run of about 1.5 s")
	}
}

// -trace turned -debug on (and printed every step of every request as
// text). The two are independent now: -trace FILE writes the structured
// trace and prints no debug lines, -debug prints the lines and writes no
// file. The old spelling -trace-file still works.
func TestTraceAndDebugAreIndependent(t *testing.T) {
	srv := newHitServer(t, func(w http.ResponseWriter, r *http.Request, name string) { freeHandler(w, r, name) })
	dir := t.TempDir()
	in := filepath.Join(dir, "names.txt")
	if err := os.WriteFile(in, []byte("alice\nbob\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	defer func(l LogLevel) { logLevel = l }(logLevel)
	run := func(extra ...string) string {
		t.Helper()
		var logs lockedBuffer
		log.SetOutput(&logs)
		defer log.SetOutput(os.Stderr)
		args := append([]string{"-input", in, "-output", filepath.Join(dir, "out.txt"), "-base-url", srv.URL,
			"-no-cache", "-quiet", "-workers", "1"}, extra...)
		if code := Main(args); code != 0 {
			t.Fatalf("%v: exit code %d\n%s", extra, code, logs.String())
		}
		return logs.String()
	}
	traceOnly := filepath.Join(dir, "only.jsonl")
	if out := run("-trace", traceOnly); strings.Contains(out, "[DEBUG]") || strings.Contains(out, "[TRACE]") {
		t.Errorf("-trace alone printed debug lines:\n%s", out)
	}
	if fi, err := os.Stat(traceOnly); err != nil || fi.Size() == 0 {
		t.Fatalf("-trace wrote no file: %v", err)
	}
	if out := run("-debug"); !strings.Contains(out, "[DEBUG] @alice #1 | -> free") || strings.Contains(out, "[TRACE]") {
		t.Errorf("-debug lines missing or trace lines printed:\n%s", out)
	}
	if matches, _ := filepath.Glob(filepath.Join(dir, "*.jsonl")); len(matches) != 1 {
		t.Errorf("trace files after the -debug run: %v, want only %s", matches, traceOnly)
	}
	oldSpelling := filepath.Join(dir, "old.jsonl")
	if out := run("-trace-file", oldSpelling); !strings.Contains(out, "use -trace "+oldSpelling) {
		t.Errorf("no hint for the old spelling:\n%s", out)
	}
	if _, err := os.Stat(oldSpelling); err != nil {
		t.Errorf("-trace-file wrote no trace: %v", err)
	}
}

// -trace was a boolean, so scripts say "-trace -workers 200" or
// "-trace=true". Taking "-workers" or "true" as the file name would write a
// trace nobody asked for and lose the rest of the command line; the run is
// refused before any file is touched.
func TestOldBooleanTraceIsRefused(t *testing.T) {
	dir := t.TempDir()
	in := filepath.Join(dir, "names.txt")
	out := filepath.Join(dir, "out.txt")
	if err := os.WriteFile(in, []byte("alice\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(out, []byte("@old | free\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	var logs lockedBuffer
	log.SetOutput(&logs)
	defer log.SetOutput(os.Stderr)
	for _, args := range [][]string{
		{"-input", in, "-output", out, "-trace", "-workers", "2"},
		{"-input", in, "-output", out, "-trace=true"},
	} {
		if code := Main(args); code != 2 {
			t.Errorf("%v: exit code %d, want 2", args, code)
		}
	}
	if !strings.Contains(logs.String(), "-trace takes the path of the trace file") {
		t.Errorf("no explanation in the log:\n%s", logs.String())
	}
	if got, _ := os.ReadFile(out); string(got) != "@old | free\n" {
		t.Errorf("the results file changed: %q", got)
	}
	for _, name := range []string{"-workers", "true"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err == nil {
			t.Errorf("a trace file %q was created", name)
		}
	}
}

// The trace is opened with O_TRUNC, so "-trace results.txt" (or the input,
// or the checked list) would wipe that file. Such a run is refused.
func TestTraceCannotBeAnotherFileOfTheRun(t *testing.T) {
	dir := t.TempDir()
	in := filepath.Join(dir, "names.txt")
	out := filepath.Join(dir, "out.txt")
	for path, body := range map[string]string{in: "alice\n", out: "@old | free\n", out + ".checked": "old\n"} {
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	var logs lockedBuffer
	log.SetOutput(&logs)
	defer log.SetOutput(os.Stderr)
	sep := string(os.PathSeparator)
	for _, trace := range []string{out, in, out + ".checked", dir + sep + "." + sep + "out.txt"} {
		if code := Main([]string{"-input", in, "-output", out, "-filter", "free", "-trace", trace}); code != 2 {
			t.Errorf("-trace %s: exit code %d, want 2", trace, code)
		}
	}
	for path, body := range map[string]string{in: "alice\n", out: "@old | free\n", out + ".checked": "old\n"} {
		if got, _ := os.ReadFile(path); string(got) != body {
			t.Errorf("%s changed: %q", path, got)
		}
	}
	if !strings.Contains(logs.String(), "is the same file as -output") {
		t.Errorf("no explanation in the log:\n%s", logs.String())
	}
}

// -h shows -trace FILE and leaves the old spelling -trace-file out.
func TestUsageShowsTraceFile(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	stderr := os.Stderr
	os.Stderr = w
	code := Main([]string{"-h"})
	os.Stderr = stderr
	_ = w.Close()
	out, _ := io.ReadAll(r)
	if code != 0 || !strings.Contains(string(out), "-trace FILE") || strings.Contains(string(out), "-trace-file") ||
		strings.Contains(string(out), "implies -debug") {
		t.Errorf("-h (exit %d):\n%s", code, out)
	}
}

// -trace FILE.gz writes gzip. With -trace-max the trace keeps the end of
// the run in two files (it has no limit before 3.0.0 and could fill the
// disk), run_end is always the last line (the progress goroutine is
// waited for), and the lines are numbered without a gap.
func TestTraceGzipKeepsTheEnd(t *testing.T) {
	srv := newHitServer(t, func(w http.ResponseWriter, r *http.Request, name string) { freeHandler(w, r, name) })
	dir := t.TempDir()
	in := filepath.Join(dir, "names.txt")
	var names strings.Builder
	for i := 0; i < 600; i++ {
		fmt.Fprintf(&names, "name%04d\n", i)
	}
	if err := os.WriteFile(in, []byte(names.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	tracePath := filepath.Join(dir, "run.jsonl.gz")
	var logs lockedBuffer
	log.SetOutput(&logs)
	defer log.SetOutput(os.Stderr)
	if code := Main([]string{"-input", in, "-output", filepath.Join(dir, "out.txt"), "-base-url", srv.URL,
		"-no-cache", "-quiet", "-workers", "4", "-trace", tracePath, "-trace-max", "16K"}); code != 0 {
		t.Fatalf("exit code %d\n%s", code, logs.String())
	}
	files := tracelog.Files(tracePath)
	if len(files) != 2 {
		t.Fatalf("trace files %v, want two segments", files)
	}
	events, err := tracelog.ReadAll(files...)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i < len(events); i++ {
		if events[i].Seq != events[i-1].Seq+1 {
			t.Fatalf("seq %d after %d", events[i].Seq, events[i-1].Seq)
		}
	}
	if last := events[len(events)-1]; last.Kind != "run_end" || last.Info["checked"] != float64(600) {
		t.Errorf("last event %+v, want run_end of 600 names", last)
	}
	cur, err := tracelog.ReadAll(tracePath)
	if err != nil {
		t.Fatal(err)
	}
	if cur[0].Kind != "segment" || cur[0].Info["version"] != Version {
		t.Errorf("the newest file starts with %+v, want a segment line repeating run_start", cur[0])
	}
	if !strings.Contains(logs.String(), "the trace keeps the last 16K of the run") {
		t.Errorf("no note about the dropped start:\n%s", logs.String())
	}
}

func TestByteSizeFlag(t *testing.T) {
	for in, want := range map[string]int64{"0": 0, "512K": 512 << 10, "100M": 100 << 20, "1G": 1 << 30,
		"2gb": 2 << 30, "1500": 1500} {
		var b byteSize
		if err := b.Set(in); err != nil || int64(b) != want {
			t.Errorf("Set(%q) = %d, %v; want %d", in, int64(b), err, want)
		}
	}
	for _, in := range []string{"-1", "1T", "abc", "", "9999999999G"} {
		var b byteSize
		if err := b.Set(in); err == nil {
			t.Errorf("Set(%q) accepted", in)
		}
	}
	for b, want := range map[byteSize]string{0: "0", 1 << 30: "1G", 16 << 10: "16K", 1500: "1500"} {
		if got := b.String(); got != want {
			t.Errorf("String(%d) = %q, want %q", int64(b), got, want)
		}
	}
}
