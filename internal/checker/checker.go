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
	"regexp"
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
}

// New returns a Checker ready to Run. Pass DefaultConfig() and adjust as needed.
func New(cfg Config) *Checker {
	cfg = normalizeConfig(cfg)
	c := &Checker{
		cfg:     cfg,
		client:  newHTTPClient(cfg),
		limiter: newRateLimiter(cfg.RateRPS),
		stats:   newLiveStats(),
		cache:   newPageCache(cfg.CacheDir, cfg.CacheTTL, cfg.CacheSoldTTL),
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
			c.stats.record(r.Status)
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
				return res
			}
			atomic.AddInt64(&c.stats.cacheStale, 1)
			res.Stale = true
			stale = &res
		} else {
			atomic.AddInt64(&c.stats.cacheMisses, 1)
		}
	}

	reqURL := fmt.Sprintf(FragmentURLTemplate, url.PathEscape(name))

	var lastErr string
	var lastRes Result
	for attempt := 0; attempt <= c.cfg.Retries; attempt++ {
		if attempt > 0 {
			atomic.AddInt64(&c.stats.retries, 1)
			select {
			case <-ctx.Done():
				return Result{Username: name, Status: StatusError, Error: "cancelled"}
			case <-time.After(c.cfg.RetryDelay):
			}
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
			lastRes.Evidence = fmt.Sprintf("redirect=/ after %d attempts; probe healthy -> reserved", c.cfg.Retries+1)
			lastRes.Confidence = 60
			return lastRes
		}
		return Result{Username: name, Status: StatusError,
			Error:    "persistent 302 / (throttled; probe also blocked)",
			Evidence: "redirect=/; probe blocked"}
	}
	if stale != nil {
		res := *stale
		res.Confidence = 30
		res.Evidence = "stale-cache; " + res.Evidence
		res.Error = "network failed (" + lastErr + "); served stale cache"
		return res
	}
	return Result{Username: name, Status: StatusError, Error: "retries exhausted: " + lastErr}
}

func (c *Checker) doRequest(ctx context.Context, name, reqURL string) (Result, bool, string) {
	if err := c.waitBlock(ctx); err != nil {
		return Result{Username: name, Status: StatusError, Error: "cancelled"}, false, "cancelled"
	}
	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return Result{Username: name, Status: StatusError, Error: err.Error()}, false, err.Error()
	}
	req.Header.Set("User-Agent", c.cfg.UserAgent)
	req.Header.Set("Accept", "text/html,application/xhtml+xml")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")
	req.Header.Set("Accept-Encoding", "gzip")

	req = req.WithContext(httptrace.WithClientTrace(req.Context(), &httptrace.ClientTrace{
		GotConn: func(info httptrace.GotConnInfo) {
			if info.Reused {
				atomic.AddInt64(&c.stats.connReuse, 1)
			} else {
				atomic.AddInt64(&c.stats.connNew, 1)
			}
		},
	}))

	resp, err := c.client.Do(req)
	if err != nil {
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
	switch strings.ToLower(strings.TrimSpace(resp.Header.Get("Content-Encoding"))) {
	case "gzip":
		gz, gzErr := gzip.NewReader(resp.Body)
		if gzErr != nil {
			return Result{Username: name, Status: StatusError, Error: "gzip: " + gzErr.Error()}, false, gzErr.Error()
		}
		defer gz.Close()
		bodyReader = gz
	case "", "identity":
		// no decompression needed
	}

	switch resp.StatusCode {
	case 301, 302, 303, 307, 308:
		loc := resp.Header.Get("Location")
		switch {
		case strings.Contains(loc, "?query="):
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
			return Result{Username: name, Status: StatusUnknown, Confidence: 20,
				Evidence: "unexpected redirect=" + loc}, false, ""
		}

	case 429:
		atomic.AddInt64(&c.stats.rateLimit, 1)
		wait := parseRetryAfter(resp.Header.Get("Retry-After"), c.cfg.RetryDelay)
		if wait > c.cfg.RLWaitMax {
			wait = c.cfg.RLWaitMax
		}
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
			return Result{Username: name, Status: StatusError, Error: readErr.Error()}, isRetriableNet(readErr), readErr.Error()
		}
		pi := ParseStatusDetailed(body)
		if pi.Challenge {
			*bufPtr = buf
			c.pool.Put(bufPtr)
			return Result{Username: name, Status: StatusError, Error: "cloudflare challenge",
				Evidence: pi.Evidence}, true, "cloudflare challenge"
		}
		if c.cache != nil && pi.Found {
			// Best-effort: never let a read-only cache directory fail a check.
			_ = c.cache.put(name, reqURL, pi, body)
		}
		*bufPtr = buf
		c.pool.Put(bufPtr)
		if !pi.Found {
			return Result{Username: name, Status: StatusUnknown,
				Confidence: pi.Confidence, Evidence: pi.Evidence}, false, ""
		}
		return Result{Username: name, Status: pi.Status,
			Confidence: pi.Confidence, Evidence: pi.Evidence}, false, ""

	default:
		s := fmt.Sprintf("HTTP %d", resp.StatusCode)
		retryable := resp.StatusCode >= 500 && resp.StatusCode <= 599
		return Result{Username: name, Status: StatusError, Error: s}, retryable, s
	}
}

// -----------------------------------------------------------------------------
// Helpers (exported where useful for callers and tests)
// -----------------------------------------------------------------------------

// usernameRe matches Fragment-style usernames: lowercase ASCII letters,
// digits and underscore, length 1..64.
var usernameRe = regexp.MustCompile(`^[a-z0-9_]{1,64}$`)

// ValidateUsername rejects empty/invalid inputs. The caller has already
// stripped a leading "@" and lower-cased the value.
func ValidateUsername(name string) error {
	if name == "" {
		return errors.New("empty")
	}
	if !usernameRe.MatchString(name) {
		return errors.New("invalid characters or length")
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

func newHTTPClient(c Config) *http.Client {
	transport := &http.Transport{
		MaxIdleConns:          c.MaxConns,
		MaxIdleConnsPerHost:   c.MaxConns,
		MaxConnsPerHost:       c.MaxConns,
		IdleConnTimeout:       60 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ForceAttemptHTTP2:     true,
		DisableCompression:    true,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
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
	close(rl.stop)
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
	}
}

// -----------------------------------------------------------------------------
// Config normalization
// -----------------------------------------------------------------------------

func normalizeConfig(c Config) Config {
	if c.Workers < 1 {
		c.Workers = 1
	}
	if c.MaxConns < 1 {
		c.MaxConns = 1
	}
	if c.ReadLimit < 1024 {
		c.ReadLimit = 1024
	}
	if c.Retries < 0 {
		c.Retries = 0
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

// ParseNamesReader streams names from r. Same semantics as ParseNames but
// the input is consumed line by line so very large files use O(unique)
// memory rather than O(total).
func ParseNamesReader(r io.Reader) (names []string, rejected int, err error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64*1024), 1<<20)
	seen := make(map[string]struct{}, 256)
	for scanner.Scan() {
		line := scanner.Text()
		// Each line can still contain multiple names separated by
		// commas/spaces (paste-friendly).
		batch, rej := ParseNames(line)
		rejected += rej
		for _, n := range batch {
			if _, ok := seen[n]; ok {
				continue
			}
			seen[n] = struct{}{}
			names = append(names, n)
		}
	}
	return names, rejected, scanner.Err()
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
			return nil
		}
	}
}
