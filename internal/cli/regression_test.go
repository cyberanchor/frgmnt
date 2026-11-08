// Regression tests for bugs found in the v4 review. Each test describes the
// old behaviour it guards against.
package cli

import (
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
		if _, got := set[name]; got != want {
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
