// Package checker is a library version of the v3.1 Fragment.com username
// availability checker. It is decoupled from CLI flags and global state so
// that multiple Checker instances can run concurrently with independent
// configurations (one per web "worker").
package checker

import (
	"bufio"
	"compress/gzip"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptrace"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// FragmentURLTemplate is exported so tests can swap it for an httptest URL.
var FragmentURLTemplate = "https://fragment.com/username/%s"

const (
	defaultUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) " +
		"AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"

	statusMarker      = "tm-section-header-status"
	markerCountdown   = "tm-countdown-timer"
	markerMakeOffer   = "js-make-offer-btn"
	markerPlaceBid    = "js-place-bid-btn"
	markerBuyNow      = "js-buy-now-btn"
	markerPurchased   = "Purchased on"
	statusClassPrefix = "tm-status-"

	// DefaultReadLimit is the body size scanned for the status marker.
	DefaultReadLimit = 16384

	// ChallengeLimit is how many Cloudflare challenges in a row, with no
	// other answer in between, make Run give up: Fragment is then blocking
	// this client, and every further request would fail the same way.
	ChallengeLimit = 10
)

// Status is the resolved availability state of a Fragment username.
type Status string

// Status values returned by the checker.
const (
	StatusFree      Status = "free"
	StatusReserved  Status = "reserved"
	StatusAuction   Status = "auction"
	StatusTaken     Status = "taken"
	StatusSold      Status = "sold"
	StatusAvailable Status = "available"
	StatusUnknown   Status = "unknown"
	StatusError     Status = "error"
)

// AllStatuses lists every Status value, in the order used by UI summaries.
var AllStatuses = []Status{
	StatusFree, StatusReserved, StatusAuction, StatusAvailable,
	StatusTaken, StatusSold, StatusUnknown, StatusError,
}

// Result is the outcome of a single username check.
type Result struct {
	Username string        `json:"username"`
	Status   Status        `json:"status"`
	Error    string        `json:"error,omitempty"`
	Took     time.Duration `json:"took_ns"`

	// Confidence is 0..100 and explains how certain the parser is.
	// Cached is true when the answer came from the on-disk page cache.
	// Stale is true when a stale cache entry was used because the network
	// failed; treat such results as best-effort.
	Confidence int    `json:"confidence,omitempty"`
	Evidence   string `json:"evidence,omitempty"`
	Cached     bool   `json:"cached,omitempty"`
	Stale      bool   `json:"stale,omitempty"`

	// Offset is where the status marker starts in a page fetched from the
	// network (0 otherwise); the console uses it to suggest a -readlimit.
	Offset int `json:"offset,omitempty"`

	// notSent marks a check that never sent its request because the
	// client was found blocked first. Run drops it: the name stays
	// unchecked, like one cut short by a cancel.
	notSent bool
}

// Config is the per-Checker tunables surface. Zero values get sensible
// defaults from NewChecker.
type Config struct {
	Workers     int           `json:"workers"`
	MaxConns    int           `json:"max_conns"`
	RateRPS     int           `json:"rate_rps"`
	Retries     int           `json:"retries"`
	RetryDelay  time.Duration `json:"retry_delay_ns"`
	RLWaitMax   time.Duration `json:"rl_wait_max_ns"`
	Timeout     time.Duration `json:"timeout_ns"`
	ReadLimit   int           `json:"read_limit"`
	UserAgent   string        `json:"user_agent"`
	InsecureTLS bool          `json:"insecure_tls"`

	// Page cache. CacheDir == "" disables it. CacheTTL applies to volatile
	// verdicts (available/auction); negative disables caching them.
	// CacheSoldTTL applies to sold (bought) pages; negative means forever.
	CacheDir     string        `json:"cache_dir,omitempty"`
	CacheTTL     time.Duration `json:"cache_ttl_ns,omitempty"`
	CacheSoldTTL time.Duration `json:"cache_sold_ttl_ns,omitempty"`

	// BlockCooldown is the pause applied to all workers when Fragment
	// answers with a bare "302 /" — a signal that is used both for rare
	// reserved names and for throttling. Retries with a cool-down keep a
	// transient rate limit from being misreported as "reserved".
	BlockCooldown time.Duration `json:"block_cooldown_ns,omitempty"`
}

// Upper bounds for Config values. The web API rejects values above them;
// normalizeConfig clamps anything that still gets through (for example a
// config saved by an older version). Without them a single API call could
// crash the process: read_limit goes straight into make([]byte, n).
const (
	MaxWorkers   = 1000
	MaxConnLimit = 1000
	MaxRateRPS   = 1000
	MaxRetries   = 10
	MaxReadLimit = 1 << 20 // the status markers sit in the first ~8 KB
)

// Validate reports values that are out of range. Zero means "use the
// default" for the counts, so only negative and too large values fail.
func (c Config) Validate() error {
	check := func(name string, v, max int) error {
		if v < 0 || v > max {
			return fmt.Errorf("%s must be between 0 and %d, got %d", name, max, v)
		}
		return nil
	}
	for _, err := range []error{
		check("workers", c.Workers, MaxWorkers),
		check("max_conns", c.MaxConns, MaxConnLimit),
		check("rate_rps", c.RateRPS, MaxRateRPS),
		check("retries", c.Retries, MaxRetries),
		check("read_limit", c.ReadLimit, MaxReadLimit),
	} {
		if err != nil {
			return err
		}
	}
	return nil
}

// DefaultConfig returns the recommended production values.
func DefaultConfig() Config {
	return Config{
		Workers:       50,
		MaxConns:      8,
		RateRPS:       0,
		Retries:       2,
		RetryDelay:    500 * time.Millisecond,
		RLWaitMax:     30 * time.Second,
		Timeout:       15 * time.Second,
		ReadLimit:     DefaultReadLimit,
		UserAgent:     defaultUserAgent,
		BlockCooldown: 3 * time.Second,
	}
}

// Stats is a snapshot of per-Checker counters. Returned by Checker.Snapshot.
type Stats struct {
	Total     int64 `json:"total"`
	Checked   int64 `json:"checked"`
	Free      int64 `json:"free"`
	Reserved  int64 `json:"reserved"`
	Auction   int64 `json:"auction"`
	Taken     int64 `json:"taken"`
	Sold      int64 `json:"sold"`
	Available int64 `json:"available"`
	Unknown   int64 `json:"unknown"`
	Errors    int64 `json:"errors"`
	Retries   int64 `json:"retries"`
	RateLimit int64 `json:"rate_limit"`
	ConnNew   int64 `json:"conn_new"`
	ConnReuse int64 `json:"conn_reuse"`

	CacheHits   int64 `json:"cache_hits"`
	CacheStale  int64 `json:"cache_stale"`
	CacheMisses int64 `json:"cache_misses"`

	// Challenges counts Cloudflare challenge answers (HTTP 403 with
	// cf-mitigated: challenge, or a "Just a moment..." page).
	Challenges int64 `json:"challenges"`
}

// Checker performs concurrent Fragment lookups against a single Config.
// One Checker corresponds to one "Worker" tile in the web UI.
type Checker struct {
	cfg     Config
	client  *http.Client
	limiter *rateLimiter
	stats   *liveStats
	pool    sync.Pool
	cache   *pageCache

	// blockUntil is a unix-nano deadline shared by all workers after a
	// suspicious bare redirect to "/".
	blockUntil atomic.Int64

	// challengeRun counts challenge answers since the last other answer;
	// blocked is set when it reaches ChallengeLimit.
	challengeRun atomic.Int64
	blocked      atomic.Bool

	// Until the first outcome that is not a challenge (an answer or a
	// network error), requests go out one at a time through scout. A
	// client that is already blocked then learns it after ChallengeLimit
	// requests instead of sending one per worker at once.
	opened atomic.Bool
	scout  chan struct{}

	log Logger // optional detail for the console's -debug and -trace
}

// Blocked reports whether Run gave up because Fragment kept answering with
// a Cloudflare challenge. The names Run did not get to are left unread in
// its input channel; callers stop their producer and keep them pending.
func (c *Checker) Blocked() bool { return c.blocked.Load() }

// noteChallenge counts a Cloudflare challenge answer.
func (c *Checker) noteChallenge() {
	atomic.AddInt64(&c.stats.challenges, 1)
	if c.challengeRun.Add(1) >= ChallengeLimit && c.blocked.CompareAndSwap(false, true) {
		c.warnf("Fragment answered %d requests in a row with a Cloudflare challenge: this client is blocked, stopping", ChallengeLimit)
	}
}

// noteAnswer records an answer that is not a challenge: the block, if any,
// was not complete.
func (c *Checker) noteAnswer() { c.challengeRun.Store(0) }

// New returns a Checker ready to Run. Pass DefaultConfig() and adjust as needed.
func New(cfg Config) *Checker {
	cfg = normalizeConfig(cfg)
	c := &Checker{
		cfg:     cfg,
		client:  newHTTPClient(cfg),
		limiter: newRateLimiter(cfg.RateRPS),
		stats:   newLiveStats(),
		cache:   newPageCache(cfg.CacheDir, cfg.CacheTTL, cfg.CacheSoldTTL),
		scout:   make(chan struct{}, 1),
	}
	c.pool.New = func() any {
		b := make([]byte, cfg.ReadLimit)
		return &b
	}
	return c
}

// Config returns the effective configuration after normalization.
func (c *Checker) Config() Config { return c.cfg }

// Close releases internal resources (rate limiter goroutine, idle TCP conns).
// It is safe to call more than once.
func (c *Checker) Close() {
	c.limiter.Close()
	c.client.CloseIdleConnections()
}

// Snapshot returns the current counter state. Safe to call concurrently.
func (c *Checker) Snapshot() Stats { return c.stats.snapshot() }

// SetTotal is called by the producer once the total input size is known.
// It only affects the Stats.Total field for progress display.
func (c *Checker) SetTotal(n int64) { atomic.StoreInt64(&c.stats.total, n) }

// Run consumes usernames from in and emits Results to out. It launches
// cfg.Workers goroutines. It closes out when in is closed AND every worker
// has drained, OR when ctx is cancelled. Run returns when all workers exit.
func (c *Checker) Run(ctx context.Context, in <-chan string, out chan<- Result) {
	var wg sync.WaitGroup
	for i := 0; i < c.cfg.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.worker(ctx, in, out)
		}()
	}
	wg.Wait()
}

func (c *Checker) worker(ctx context.Context, in <-chan string, out chan<- Result) {
	for {
		if c.blocked.Load() {
			return // every request gets a challenge; leave the rest pending
		}
		select {
		case <-ctx.Done():
			return
		case name, ok := <-in:
			if !ok {
				return
			}
			if err := c.limiter.Wait(ctx); err != nil {
				return
			}
			start := time.Now()
			r := c.CheckOne(ctx, name)
			r.Took = time.Since(start)
			if ctx.Err() != nil && r.Status == StatusError {
				// Stop or shutdown cut this check short, so there is no
				// verdict. Reporting it as an error used to store the name
				// as done; dropping it keeps the name pending for Resume.
				return
			}
			if r.notSent {
				return // blocked before the request went out: keep it pending
			}
			c.stats.record(r.Status)
			// Deliver a finished verdict even if ctx was cancelled in the
			// meantime, as long as out has room (select alone picks a
			// random ready case).
			select {
			case out <- r:
				continue
			default:
			}
			select {
			case <-ctx.Done():
				return
			case out <- r:
			}
		}
	}
}

// CheckOne resolves a single username with the configured retry policy.
// Fresh cached pages are answered without any network request; stale cache
// entries are used as a fallback if the network ultimately fails.
func (c *Checker) CheckOne(ctx context.Context, raw string) Result {
	name := strings.ToLower(strings.TrimSpace(strings.TrimPrefix(raw, "@")))
	if err := ValidateUsername(name); err != nil {
		return Result{Username: raw, Status: StatusError, Error: "invalid: " + err.Error()}
	}

	var stale *Result
	if c.cache != nil {
		if ent, body, ok := c.cache.get(name); ok {
			// Re-parse the cached HTML with the current parser so parser
			// improvements apply retroactively to old cache entries.
			pi := ParseStatusDetailed(body)
			st, conf, ev := pi.Status, pi.Confidence, pi.Evidence
			if !pi.Found {
				st, conf, ev = ent.Status, ent.Confidence, ent.Evidence
			}
			res := Result{
				Username:   name,
				Status:     st,
				Confidence: conf,
				Evidence:   "cache; " + ev,
				Cached:     true,
			}
			if c.cache.fresh(st, ent.FetchedAt) {
				atomic.AddInt64(&c.stats.cacheHits, 1)
				c.debugf("cache hit: %s -> %s (confidence=%d; %s)", name, st, conf, ev)
				return res
			}
			atomic.AddInt64(&c.stats.cacheStale, 1)
			c.debugf("cache stale: %s -> %s; re-checking", name, st)
			// Only settled verdicts are served after a network failure: a
			// sold or taken name rarely changes, while an auction or a sale
			// seen days ago is likely over.
			if st == StatusSold || st == StatusTaken {
				res.Stale = true
				stale = &res
			}
		} else {
			atomic.AddInt64(&c.stats.cacheMisses, 1)
		}
	}

	reqURL := fmt.Sprintf(FragmentURLTemplate, url.PathEscape(name))

	var lastErr string
	var lastRes Result
	for attempt := 0; attempt <= c.cfg.Retries; attempt++ {
		if attempt > 0 {
			if c.blocked.Load() {
				break // no point in retrying: every answer is a challenge
			}
			select {
			case <-ctx.Done():
				return Result{Username: name, Status: StatusError, Error: "cancelled"}
			case <-time.After(c.cfg.RetryDelay):
			}
			if c.blocked.Load() {
				break // found blocked during the pause
			}
			atomic.AddInt64(&c.stats.retries, 1)
		}
		res, retry, errStr := c.doRequest(ctx, name, reqURL)
		if !retry {
			return res
		}
		lastErr = errStr
		lastRes = res
	}
	if lastRes.Status == StatusReserved {
		// Distinguish a genuine reserved name from a throttled client by
		// probing a random name that should be free. If the probe is also
		// redirected to "/", the whole client is being limited and we must
		// not report a false "reserved".
		if c.probeHealthy(ctx) {
			c.debugf("@%s: redirect to / persisted after %d attempts; probe healthy -> reserved", name, c.cfg.Retries+1)
			lastRes.Evidence = fmt.Sprintf("redirect=/ after %d attempts; probe healthy -> reserved", c.cfg.Retries+1)
			lastRes.Confidence = 60
			return lastRes
		}
		return Result{Username: name, Status: StatusError,
			Error:    "persistent 302 / (throttled; probe also blocked)",
			Evidence: "redirect=/; probe blocked"}
	}
	if stale != nil {
		c.debugf("network failed (%s); serving stale cache for %s", lastErr, name)
		res := *stale
		res.Confidence = 30
		res.Evidence = "stale-cache; " + res.Evidence
		res.Error = "network failed (" + lastErr + "); served stale cache"
		return res
	}
	return Result{Username: name, Status: StatusError, Error: "retries exhausted: " + lastErr}
}

func (c *Checker) doRequest(ctx context.Context, name, reqURL string) (res Result, retry bool, errStr string) {
	if err := c.waitBlock(ctx); err != nil {
		return Result{Username: name, Status: StatusError, Error: "cancelled"}, false, "cancelled"
	}
	if !c.opened.Load() {
		select {
		case c.scout <- struct{}{}:
			defer func() { <-c.scout }()
		case <-ctx.Done():
			return Result{Username: name, Status: StatusError, Error: "cancelled"}, false, "cancelled"
		}
		if c.blocked.Load() {
			msg := "not sent: Fragment blocks this client (Cloudflare challenge)"
			return Result{Username: name, Status: StatusError, Error: msg, notSent: true}, false, msg
		}
	}
	// Runs before the scout slot is released: a waiting request sees the
	// gate open and goes ahead.
	challenged := false
	defer func() {
		if !challenged && ctx.Err() == nil {
			c.opened.Store(true)
		}
	}()
	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return Result{Username: name, Status: StatusError, Error: err.Error()}, false, err.Error()
	}
	req.Header.Set("User-Agent", c.cfg.UserAgent)
	req.Header.Set("Accept", "text/html,application/xhtml+xml")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")
	req.Header.Set("Accept-Encoding", "gzip")

	start := time.Now()
	ct, tr := c.clientTrace(name, start)
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), ct))

	resp, err := c.client.Do(req)
	if err != nil {
		c.debugf("@%s REQUEST FAILED: %v (%.0fms)", name, err, ms(time.Since(start)))
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			if ctx.Err() != nil {
				return Result{Username: name, Status: StatusError, Error: "cancelled"}, false, "cancelled"
			}
			return Result{Username: name, Status: StatusError, Error: err.Error()}, true, err.Error()
		}
		return Result{Username: name, Status: StatusError, Error: err.Error()}, isRetriableNet(err), err.Error()
	}
	defer resp.Body.Close()
	defer drainBody(resp.Body)

	var bodyReader io.Reader = resp.Body
	switch ce := strings.ToLower(strings.TrimSpace(resp.Header.Get("Content-Encoding"))); ce {
	case "gzip":
		gz, gzErr := gzip.NewReader(resp.Body)
		if gzErr != nil {
			c.debugf("@%s gzip error: %v", name, gzErr)
			return Result{Username: name, Status: StatusError, Error: "gzip: " + gzErr.Error()}, false, gzErr.Error()
		}
		defer gz.Close()
		bodyReader = gz
	case "", "identity":
		// no decompression needed
	default:
		// Accept-Encoding asks for gzip only. Read the raw bytes; the parser
		// will most likely find no marker and report unknown.
		c.debugf("@%s unexpected Content-Encoding: %q — reading raw", name, ce)
	}

	c.debugf("@%s HTTP %d | CE: %s | CT: %s | Server: %s | CF-Ray: %s", name, resp.StatusCode,
		resp.Header.Get("Content-Encoding"), resp.Header.Get("Content-Type"),
		resp.Header.Get("Server"), resp.Header.Get("Cf-Ray"))
	if c.log.Tracef != nil {
		for k, vals := range resp.Header {
			c.tracef("@%s  header  %s: %s", name, k, strings.Join(vals, "; "))
		}
	}

	// Cloudflare marks the answers it replaces with a challenge (HTTP 403
	// today) with this header. They say nothing about the name.
	if strings.EqualFold(strings.TrimSpace(resp.Header.Get("Cf-Mitigated")), "challenge") {
		challenged = true
		return c.challengeResult(name, resp.StatusCode, tr)
	}

	switch resp.StatusCode {
	case 301, 302, 303, 307, 308:
		c.noteAnswer()
		loc := resp.Header.Get("Location")
		c.debugf("@%s REDIRECT -> %s", name, loc)
		switch {
		case strings.Contains(loc, "?query="):
			tr.summary(StatusFree, 0)
			return Result{Username: name, Status: StatusFree, Confidence: 95,
				Evidence: "redirect=" + loc}, false, ""
		case isReservedRedirect(loc):
			// A bare 302 to "/" is how Fragment answers both rare reserved
			// names and rate-limited clients. Do not report it as reserved
			// on the first attempt: pause all workers, then retry.
			c.triggerBlock()
			return Result{Username: name, Status: StatusReserved, Confidence: 35,
				Evidence: "redirect=/ (reserved or throttled)"}, true, "redirect to /"
		default:
			tr.summary(StatusUnknown, 0)
			return Result{Username: name, Status: StatusUnknown, Confidence: 20,
				Evidence: "unexpected redirect=" + loc}, false, ""
		}

	case 429:
		atomic.AddInt64(&c.stats.rateLimit, 1)
		ra := resp.Header.Get("Retry-After")
		wait := parseRetryAfter(ra, c.cfg.RetryDelay)
		if wait > c.cfg.RLWaitMax {
			wait = c.cfg.RLWaitMax
		}
		c.warnf("@%s 429 Retry-After=%q sleeping=%v", name, ra, wait)
		drainBody(resp.Body)
		resp.Body.Close()
		select {
		case <-ctx.Done():
			return Result{Username: name, Status: StatusError, Error: "cancelled"}, false, "cancelled"
		case <-time.After(wait):
		}
		return Result{}, true, "rate limited"

	case 200:
		bufPtr := c.pool.Get().(*[]byte)
		buf := *bufPtr
		if cap(buf) < c.cfg.ReadLimit {
			buf = make([]byte, c.cfg.ReadLimit)
		} else {
			buf = buf[:c.cfg.ReadLimit]
		}
		n, readErr := io.ReadFull(io.LimitReader(bodyReader, int64(c.cfg.ReadLimit)), buf)
		if errors.Is(readErr, io.EOF) || errors.Is(readErr, io.ErrUnexpectedEOF) {
			readErr = nil
		}
		body := buf[:n]
		if readErr != nil {
			*bufPtr = buf
			c.pool.Put(bufPtr)
			tr.summary(StatusError, n)
			return Result{Username: name, Status: StatusError, Error: readErr.Error()}, isRetriableNet(readErr), readErr.Error()
		}
		c.debugf("@%s body: %d bytes read (limit %d)", name, n, c.cfg.ReadLimit)
		pi := ParseStatusDetailed(body)
		if pi.Challenge {
			*bufPtr = buf
			c.pool.Put(bufPtr)
			challenged = true
			c.noteChallenge()
			tr.summary(StatusError, n)
			return Result{Username: name, Status: StatusError, Error: "cloudflare challenge",
				Evidence: pi.Evidence}, true, "cloudflare challenge"
		}
		c.noteAnswer()
		if c.cache != nil && pi.Found {
			// Best-effort: never let a read-only cache directory fail a check.
			_ = c.cache.put(name, reqURL, pi, body)
		}
		if !pi.Found {
			c.debugf("@%s STATUS NOT FOUND in %d bytes (increase -readlimit)", name, n)
			if c.log.Tracef != nil && n > 0 {
				preview := string(body)
				if len(preview) > 300 {
					preview = preview[:300] + "…"
				}
				c.tracef("@%s body preview:\n%s", name, preview)
			}
		} else {
			c.debugf("@%s parsed -> %s (confidence=%d; %s; offset: %d bytes, %.1f%% of readLimit)",
				name, pi.Status, pi.Confidence, pi.Evidence, pi.Offset, float64(pi.Offset)/float64(c.cfg.ReadLimit)*100)
		}
		*bufPtr = buf
		c.pool.Put(bufPtr)
		if !pi.Found {
			tr.summary(StatusUnknown, n)
			return Result{Username: name, Status: StatusUnknown,
				Confidence: pi.Confidence, Evidence: pi.Evidence}, false, ""
		}
		tr.summary(pi.Status, n)
		return Result{Username: name, Status: pi.Status, Offset: pi.Offset,
			Confidence: pi.Confidence, Evidence: pi.Evidence}, false, ""

	default:
		if resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusServiceUnavailable {
			// A proxy may drop cf-mitigated; the page itself still shows.
			head, _ := io.ReadAll(io.LimitReader(bodyReader, 8<<10))
			if looksLikeChallenge(head) {
				challenged = true
				return c.challengeResult(name, resp.StatusCode, tr)
			}
		}
		c.noteAnswer()
		c.debugf("@%s unexpected HTTP %d", name, resp.StatusCode)
		tr.summary(StatusError, 0)
		s := fmt.Sprintf("HTTP %d", resp.StatusCode)
		retryable := resp.StatusCode >= 500 && resp.StatusCode <= 599
		return Result{Username: name, Status: StatusError, Error: s}, retryable, s
	}
}

// challengeResult is the answer to a Cloudflare challenge: an error that is
// retried like the 200 interstitial, and counted toward ChallengeLimit.
func (c *Checker) challengeResult(name string, code int, tr *requestTrace) (Result, bool, string) {
	c.noteChallenge()
	c.debugf("@%s Cloudflare challenge (HTTP %d)", name, code)
	tr.summary(StatusError, 0)
	msg := fmt.Sprintf("cloudflare challenge (HTTP %d)", code)
	return Result{Username: name, Status: StatusError, Error: msg,
		Evidence: "cf-mitigated: challenge"}, true, msg
}

// -----------------------------------------------------------------------------
// Helpers (exported where useful for callers and tests)
// -----------------------------------------------------------------------------

// Telegram username rules: 4 to 32 characters from a-z, 0-9 and "_",
// starting with a letter and not ending with "_". Regular usernames need 5
// characters; the 4-character ones are Fragment collectibles. Anything
// else cannot exist, and Fragment answers it like a free name (a redirect
// to /?query=), so it used to come back as a false "free".
const (
	MinUsernameLen = 4
	MaxUsernameLen = 32
)

// ValidateUsername rejects names that cannot be Telegram usernames. The
// caller has already stripped a leading "@" and lower-cased the value.
func ValidateUsername(name string) error {
	switch {
	case name == "":
		return errors.New("empty")
	case len(name) < MinUsernameLen:
		return fmt.Errorf("shorter than %d characters", MinUsernameLen)
	case len(name) > MaxUsernameLen:
		return fmt.Errorf("longer than %d characters", MaxUsernameLen)
	}
	for i := 0; i < len(name); i++ {
		switch ch := name[i]; {
		case ch >= 'a' && ch <= 'z':
		case ch >= '0' && ch <= '9', ch == '_':
			if i == 0 {
				return errors.New("must start with a letter")
			}
		default:
			return errors.New("only a-z, 0-9 and _ are allowed")
		}
	}
	if name[len(name)-1] == '_' {
		return errors.New("must not end with _")
	}
	return nil
}

func isReservedRedirect(loc string) bool {
	if loc == "/" {
		return true
	}
	if loc == "" {
		return false
	}
	if u, err := url.Parse(loc); err == nil {
		if u.Host == "" || u.Host == "fragment.com" {
			if (u.Path == "" || u.Path == "/") && u.RawQuery == "" {
				return true
			}
		}
	}
	return false
}

func isRetriableNet(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	var nerr net.Error
	if errors.As(err, &nerr) {
		return true
	}
	return true
}

func drainBody(body io.Reader) {
	_, _ = io.Copy(io.Discard, body)
}

func parseRetryAfter(raw string, fallback time.Duration) time.Duration {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return fallback
	}
	if secs, err := strconv.Atoi(raw); err == nil {
		if secs < 0 {
			return fallback
		}
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(raw); err == nil {
		d := time.Until(t)
		if d < 0 {
			return fallback
		}
		return d
	}
	return fallback
}

// -----------------------------------------------------------------------------
// HTTP client + rate limiter
// -----------------------------------------------------------------------------

// newHTTPClient builds the client of one Checker. c.Timeout bounds the
// whole request and every phase of it: the dial, TLS handshake and
// response-header timeouts used to be fixed at 10 s, so a timeout above
// 10 s (-timeout 30s) still gave up after 10 s.
func newHTTPClient(c Config) *http.Client {
	transport := &http.Transport{
		MaxIdleConns:          c.MaxConns,
		MaxIdleConnsPerHost:   c.MaxConns,
		MaxConnsPerHost:       c.MaxConns,
		IdleConnTimeout:       60 * time.Second,
		TLSHandshakeTimeout:   c.Timeout,
		ResponseHeaderTimeout: c.Timeout,
		ExpectContinueTimeout: 1 * time.Second,
		ForceAttemptHTTP2:     true,
		DisableCompression:    true,
		DialContext: (&net.Dialer{
			Timeout:   c.Timeout,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		TLSClientConfig: &tls.Config{
			MinVersion:         tls.VersionTLS12,
			InsecureSkipVerify: c.InsecureTLS,
		},
	}
	jar, _ := cookiejar.New(nil)
	return &http.Client{
		Transport: transport,
		Jar:       jar,
		Timeout:   c.Timeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

type rateLimiter struct {
	tokens chan struct{}
	stop   chan struct{}
	once   sync.Once // Close may be called more than once
}

func newRateLimiter(rps int) *rateLimiter {
	if rps <= 0 {
		return nil
	}
	rl := &rateLimiter{
		tokens: make(chan struct{}, rps),
		stop:   make(chan struct{}),
	}
	interval := time.Second / time.Duration(rps)
	if interval <= 0 {
		interval = 1
	}
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-rl.stop:
				return
			case <-t.C:
				select {
				case rl.tokens <- struct{}{}:
				default:
				}
			}
		}
	}()
	return rl
}

func (rl *rateLimiter) Wait(ctx context.Context) error {
	if rl == nil {
		return nil
	}
	select {
	case <-rl.tokens:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (rl *rateLimiter) Close() {
	if rl == nil {
		return
	}
	rl.once.Do(func() { close(rl.stop) })
}

// -----------------------------------------------------------------------------
// Live stats
// -----------------------------------------------------------------------------

type liveStats struct {
	total     int64
	checked   int64
	free      int64
	reserved  int64
	auction   int64
	taken     int64
	sold      int64
	available int64
	unknown   int64
	errors    int64
	retries   int64
	rateLimit int64
	connNew   int64
	connReuse int64

	cacheHits   int64
	cacheStale  int64
	cacheMisses int64
	challenges  int64
}

func newLiveStats() *liveStats { return &liveStats{} }

func (s *liveStats) record(st Status) {
	switch st {
	case StatusFree:
		atomic.AddInt64(&s.free, 1)
	case StatusReserved:
		atomic.AddInt64(&s.reserved, 1)
	case StatusAuction:
		atomic.AddInt64(&s.auction, 1)
	case StatusTaken:
		atomic.AddInt64(&s.taken, 1)
	case StatusSold:
		atomic.AddInt64(&s.sold, 1)
	case StatusAvailable:
		atomic.AddInt64(&s.available, 1)
	case StatusUnknown:
		atomic.AddInt64(&s.unknown, 1)
	case StatusError:
		atomic.AddInt64(&s.errors, 1)
	}
	atomic.AddInt64(&s.checked, 1)
}

func (s *liveStats) snapshot() Stats {
	return Stats{
		Total:     atomic.LoadInt64(&s.total),
		Checked:   atomic.LoadInt64(&s.checked),
		Free:      atomic.LoadInt64(&s.free),
		Reserved:  atomic.LoadInt64(&s.reserved),
		Auction:   atomic.LoadInt64(&s.auction),
		Taken:     atomic.LoadInt64(&s.taken),
		Sold:      atomic.LoadInt64(&s.sold),
		Available: atomic.LoadInt64(&s.available),
		Unknown:   atomic.LoadInt64(&s.unknown),
		Errors:    atomic.LoadInt64(&s.errors),
		Retries:   atomic.LoadInt64(&s.retries),
		RateLimit: atomic.LoadInt64(&s.rateLimit),
		ConnNew:   atomic.LoadInt64(&s.connNew),
		ConnReuse: atomic.LoadInt64(&s.connReuse),

		CacheHits:   atomic.LoadInt64(&s.cacheHits),
		CacheStale:  atomic.LoadInt64(&s.cacheStale),
		CacheMisses: atomic.LoadInt64(&s.cacheMisses),
		Challenges:  atomic.LoadInt64(&s.challenges),
	}
}

// -----------------------------------------------------------------------------
// Config normalization
// -----------------------------------------------------------------------------

func normalizeConfig(c Config) Config {
	c.Workers = clamp(c.Workers, 1, MaxWorkers)
	c.MaxConns = clamp(c.MaxConns, 1, MaxConnLimit)
	c.ReadLimit = clamp(c.ReadLimit, 1024, MaxReadLimit)
	c.Retries = clamp(c.Retries, 0, MaxRetries)
	if c.RateRPS > MaxRateRPS {
		c.RateRPS = MaxRateRPS
	}
	if c.RetryDelay <= 0 {
		c.RetryDelay = 500 * time.Millisecond
	}
	if c.RLWaitMax <= 0 {
		c.RLWaitMax = 30 * time.Second
	}
	if c.Timeout <= 0 {
		c.Timeout = 15 * time.Second
	}
	if strings.TrimSpace(c.UserAgent) == "" {
		c.UserAgent = defaultUserAgent
	}
	if c.BlockCooldown <= 0 {
		c.BlockCooldown = 3 * time.Second
	}
	return c
}

func clamp(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

// -----------------------------------------------------------------------------
// Input parsing helpers
// -----------------------------------------------------------------------------

// ParseNames extracts and deduplicates valid usernames from a free-form
// blob. Whitespace, commas, semicolons, and newlines act as separators.
// Leading "@" is stripped. Returns (names, rejected) where rejected is
// the number of lines that failed validation.
func ParseNames(blob string) (names []string, rejected int) {
	seen := make(map[string]struct{}, 256)
	sep := func(r rune) bool {
		return r == '\n' || r == '\r' || r == ' ' || r == '\t' || r == ',' || r == ';'
	}
	for _, raw := range strings.FieldsFunc(blob, sep) {
		name := strings.ToLower(strings.TrimSpace(strings.TrimPrefix(raw, "@")))
		if name == "" {
			continue
		}
		if err := ValidateUsername(name); err != nil {
			rejected++
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		names = append(names, name)
	}
	return names, rejected
}

// maxNameToken caps how much of one separator-free token ParseNamesReader
// keeps; anything longer cannot be a valid name and is counted as rejected.
const maxNameToken = 4096

// ParseNamesReader streams names from r with the same rules as ParseNames.
// It splits on the separators byte by byte instead of reading lines, so a
// comma-separated export on a single multi-megabyte line works like one
// name per line (the old line scanner failed on lines over 1 MiB), and
// memory stays O(unique names) rather than O(file size).
func ParseNamesReader(r io.Reader) (names []string, rejected int, err error) {
	br := bufio.NewReaderSize(r, 64*1024)
	seen := make(map[string]struct{}, 256)
	tok := make([]byte, 0, 64)
	tooLong := false
	emit := func() {
		defer func() { tok, tooLong = tok[:0], false }()
		if tooLong {
			rejected++
			return
		}
		name := strings.ToLower(strings.TrimSpace(strings.TrimPrefix(string(tok), "@")))
		if name == "" {
			return
		}
		if ValidateUsername(name) != nil {
			rejected++
			return
		}
		if _, ok := seen[name]; ok {
			return
		}
		seen[name] = struct{}{}
		names = append(names, name)
	}
	for {
		b, rerr := br.ReadByte()
		if rerr != nil {
			emit()
			if rerr == io.EOF {
				return names, rejected, nil
			}
			return names, rejected, rerr
		}
		switch b {
		case '\n', '\r', ' ', '\t', ',', ';': // the separators of ParseNames
			emit()
		default:
			if len(tok) < maxNameToken {
				tok = append(tok, b)
			} else {
				tooLong = true
			}
		}
	}
}

// probeHealthy requests a random username that should resolve to free/taken.
// It returns false when Fragment redirects even the probe to "/", which is
// the signature of IP/session throttling rather than a reserved name.
func (c *Checker) probeHealthy(ctx context.Context) bool {
	name := fmt.Sprintf("zzprobe%d", time.Now().UnixNano()%1_000_000_000_000)
	reqURL := fmt.Sprintf(FragmentURLTemplate, url.PathEscape(name))
	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return false
	}
	req.Header.Set("User-Agent", c.cfg.UserAgent)
	req.Header.Set("Accept", "text/html,application/xhtml+xml")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")
	resp, err := c.client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	defer drainBody(resp.Body)
	if resp.StatusCode == http.StatusOK {
		return true
	}
	if resp.StatusCode >= 300 && resp.StatusCode <= 399 {
		return strings.Contains(resp.Header.Get("Location"), "?query=")
	}
	return false
}

// triggerBlock pauses every worker for cfg.BlockCooldown. It is called when
// Fragment answers with a bare 302 to "/", which happens both for reserved
// names and for clients that are being rate-limited.
func (c *Checker) triggerBlock() {
	d := c.cfg.BlockCooldown
	if d <= 0 {
		return
	}
	until := time.Now().Add(d).UnixNano()
	for {
		old := c.blockUntil.Load()
		if until <= old || c.blockUntil.CompareAndSwap(old, until) {
			return
		}
	}
}

// waitBlock sleeps until the shared cooldown expires, or ctx is cancelled.
// Another worker can push the deadline further while we sleep (a new
// "302 /"), so the deadline is read again after every timer.
func (c *Checker) waitBlock(ctx context.Context) error {
	for {
		until := c.blockUntil.Load()
		if until == 0 {
			return nil
		}
		d := time.Until(time.Unix(0, until))
		if d <= 0 {
			return nil
		}
		t := time.NewTimer(d)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
		}
	}
}
