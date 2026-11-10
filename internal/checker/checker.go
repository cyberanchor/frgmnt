// Package checker is a library version of the v3.1 Fragment.com username
// availability checker. It is decoupled from CLI flags and global state so
// that multiple Checker instances can run concurrently with independent
// configurations (one per web "worker").
package checker

import (
	"bytes"
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
	rpprof "runtime/pprof"
	rtrace "runtime/trace"
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

	// Took splits into Net, the time of the name's own HTTP requests
	// (connecting, sending, waiting for the answer, reading the body), and
	// Wait, everything else: a -rate start, the gate, a shared pause, the
	// delay before a retry, a probe. Took used to be the only figure, so
	// the -debug timings mixed the network with the queue. Attempts counts
	// the HTTP tries of the name, throttled ones included.
	Net      time.Duration `json:"net_ns,omitempty"`
	Wait     time.Duration `json:"wait_ns,omitempty"`
	Attempts int           `json:"attempts,omitempty"`
	// ConnWait is the part of Wait spent waiting for a free connection
	// (all -conns busy); it used to be counted as network time.
	ConnWait time.Duration `json:"conn_wait_ns,omitempty"`
	// Work is the time spent on this machine: parsing the pages, reading
	// and writing the page cache. Took = Net + Wait + Work; parsing and
	// the cache write used to count as network time.
	Work time.Duration `json:"work_ns,omitempty"`

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

	// throttleWait is set by doRequest for HTTP 429: the Retry-After wait
	// that the shared pause must at least cover.
	throttleWait time.Duration
}

// Config is the per-Checker tunables surface. Zero values get sensible
// defaults from NewChecker.
type Config struct {
	Workers     int           `json:"workers"`
	MaxConns    int           `json:"max_conns"`
	RateRPS     int           `json:"rate_rps"` // requests per second to Fragment, retries included; 0 = no limit
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
	// throttles this client: a bare "302 /" (used both for rare reserved
	// names and for throttling) while a probe name is redirected to "/"
	// as well. A reserved name alone does not pause anyone.
	BlockCooldown time.Duration `json:"block_cooldown_ns,omitempty"`

	// ThrottleMax is how long Fragment may keep throttling this client,
	// counted from the first sign after the last normal answer, before
	// Run stops and leaves the rest of the names pending. Meanwhile the
	// shared pause grows from BlockCooldown up to two minutes.
	ThrottleMax time.Duration `json:"throttle_max_ns,omitempty"`

	// Trace is for the web panel: it writes the structured trace of this
	// worker's runs to <data>/traces/<id>.jsonl.gz. The engine does not
	// read it (the console has -trace FILE).
	Trace bool `json:"trace,omitempty"`

	// KeyLog, when set, receives the TLS keys of every connection in the
	// NSS key log format (tls.Config.KeyLogWriter): with a capture of the
	// traffic (tcpdump), Wireshark decrypts every byte, HTTP/2 frames
	// included. Process-wide (-tls-keylog), never in a worker's settings.
	KeyLog io.Writer `json:"-"`

	// TraceBodies says which pages the trace keeps (whole, decoded, up to
	// 1 MB each): "anomalies" (the default, also ""): unknown answers,
	// challenges, unexpected codes and errors; "all": every answer;
	// "none". It only matters when the trace is on.
	TraceBodies string `json:"trace_bodies,omitempty"`
}

// Values of Config.TraceBodies.
const (
	BodiesAnomalies = "anomalies"
	BodiesAll       = "all"
	BodiesNone      = "none"
)

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
// default" for the counts and durations, so only negative and too large
// values fail. The cache TTLs are not checked: a negative one has a
// meaning (do not cache / never expire).
func (c Config) Validate() error {
	check := func(name string, v, max int) error {
		if v < 0 || v > max {
			return fmt.Errorf("%s must be between 0 and %d, got %d", name, max, v)
		}
		return nil
	}
	checkDur := func(name string, v, max time.Duration) error {
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
		checkDur("retry_delay_ns", c.RetryDelay, time.Minute),
		checkDur("rl_wait_max_ns", c.RLWaitMax, 10*time.Minute),
		checkDur("timeout_ns", c.Timeout, 10*time.Minute),
		checkDur("block_cooldown_ns", c.BlockCooldown, 10*time.Minute),
		checkDur("throttle_max_ns", c.ThrottleMax, 24*time.Hour),
	} {
		if err != nil {
			return err
		}
	}
	switch c.TraceBodies {
	case "", BodiesAnomalies, BodiesAll, BodiesNone:
	default:
		return fmt.Errorf("trace_bodies must be anomalies, all or none, got %q", c.TraceBodies)
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
		ThrottleMax:   defaultThrottleMax,
	}
}

// defaultThrottleMax lets a long run sit out a throttled spell: with the
// pause capped at two minutes that costs about two requests per pause.
const defaultThrottleMax = 30 * time.Minute

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
	// CacheErrors counts the misses where an entry was there but could
	// not be read (a broken file, a permission); part of CacheMisses.
	CacheErrors int64 `json:"cache_errors"`

	// Challenges counts Cloudflare challenge answers (HTTP 403 with
	// cf-mitigated: challenge, or a "Just a moment..." page).
	Challenges int64 `json:"challenges"`

	// Pauses counts the shared pauses taken because Fragment throttled
	// this client.
	Pauses int64 `json:"pauses"`

	// What the workers do right now: inside a check (Busy), with a request
	// on the network (InFlight, probes included), or waiting for the gate,
	// a shared pause, a -rate start, the delay before a retry, or the
	// throttle probe. Busy minus the others is time spent on the CPU
	// (parsing, the cache).
	Busy      int64 `json:"busy"`
	InFlight  int64 `json:"in_flight"`
	WaitGate  int64 `json:"wait_gate"`
	WaitPause int64 `json:"wait_pause"`
	WaitRate  int64 `json:"wait_rate"`
	WaitRetry int64 `json:"wait_retry"`
	WaitProbe int64 `json:"wait_probe"`
	// WaitConn counts the requests waiting for a connection: all -conns
	// connections are busy, or one is being opened for them. InFlight
	// includes them.
	WaitConn int64 `json:"wait_conn"`

	// The connections to Fragment: open now, and the bytes on the wire
	// (TLS included) since the start.
	ConnOpen int64 `json:"conn_open"`
	BytesIn  int64 `json:"bytes_in"`
	BytesOut int64 `json:"bytes_out"`
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
	// blocked is set when it reaches ChallengeLimit, or when throttling
	// lasts ThrottleMax (blockKind says which). blockedCh is closed then,
	// to wake the requests that wait.
	challengeRun atomic.Int64
	blocked      atomic.Bool
	blockKind    atomic.Int32
	blockedCh    chan struct{}

	// Throttling: rounds of the shared pause since the last normal
	// answer, and when the first sign of this spell came. throttling is
	// the lock-free "is there anything to reset" for noteHealthy.
	throttleMu     sync.Mutex
	throttleRounds int
	throttleSince  time.Time
	throttling     atomic.Bool

	// The last probe (see probe): when it started and finished, and whether
	// the probe name got a normal answer.
	probeMu      sync.Mutex
	probeStarted time.Time
	probeDone    time.Time
	probeOK      bool

	// Until the first outcome that is not a challenge (an answer or a
	// network error), requests go out one at a time through the gate.
	gate *gate

	connSeq atomic.Int64 // ids of the connections (cid in the trace)
	nameSeq atomic.Int64 // ids of the checks of names (nid)
	trySeq  atomic.Int64 // ids of the HTTP requests, tries and probes (aid)

	log Logger // optional detail: the debug lines and the structured trace
}

// Blocked reports whether Run gave up: Fragment kept answering with a
// Cloudflare challenge, or kept throttling for ThrottleMax (BlockedBy).
// The names Run did not get to are left unread in its input channel;
// callers stop their producer and keep them pending.
func (c *Checker) Blocked() bool { return c.blocked.Load() }

// BlockKind says why Run gave up.
type BlockKind int

// Why Run gave up.
const (
	NotBlocked         BlockKind = iota
	BlockedByChallenge           // ChallengeLimit Cloudflare challenges in a row
	BlockedByThrottle            // Fragment throttled for ThrottleMax
)

// BlockedBy reports why Run gave up, or NotBlocked.
func (c *Checker) BlockedBy() BlockKind { return BlockKind(c.blockKind.Load()) }

// block stops the run: Run takes no more names, and the requests that
// wait (for the gate, the pause or a -rate start) wake up and are not
// sent. Only the first call counts.
func (c *Checker) block(kind BlockKind, format string, args ...any) {
	if c.blocked.CompareAndSwap(false, true) {
		c.blockKind.Store(int32(kind))
		close(c.blockedCh)
		c.warnf(format, args...)
		block := "challenge"
		if kind == BlockedByThrottle {
			block = "throttle"
		}
		c.event(TraceEvent{Kind: "blocked", Block: block, Message: fmt.Sprintf(format, args...)})
	}
}

// openGate and closeGate change the gate and put the change in the trace.
func (c *Checker) openGate() {
	if c.gate.Open() {
		c.event(TraceEvent{Kind: "gate", Gate: "open"})
	}
}

func (c *Checker) closeGate() {
	if c.gate.Close() {
		c.event(TraceEvent{Kind: "gate", Gate: "closed"})
	}
}

// errBlocked ends a wait because the run was stopped (see block).
var errBlocked = errors.New("blocked")

// noteChallenge counts a Cloudflare challenge answer.
func (c *Checker) noteChallenge() {
	atomic.AddInt64(&c.stats.challenges, 1)
	if c.challengeRun.Add(1) >= ChallengeLimit {
		c.block(BlockedByChallenge, "Fragment answered %d requests in a row with a Cloudflare challenge: this client is blocked, stopping", ChallengeLimit)
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
		limiter: newRateLimiter(cfg.RateRPS),
		stats:   newLiveStats(),
		cache:   newPageCache(cfg.CacheDir, cfg.CacheTTL, cfg.CacheSoldTTL),
		gate:    newGate(),

		blockedCh: make(chan struct{}),
	}
	c.client = c.newHTTPClient()
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
		wctx := context.WithValue(ctx, workerKey{}, i+1)
		go func() {
			defer wg.Done()
			// pprof shows the CPU, blocking and goroutines of the checks
			// under this label (plus the host's, e.g. the panel's tile).
			rpprof.Do(wctx, rpprof.Labels("component", "check-worker"), func(ctx context.Context) {
				c.worker(ctx, in, out)
			})
		}()
	}
	wg.Wait()
}

// workerKey carries the number of the worker (from 1) to CheckOne, for the
// trace ("w"); a CheckOne outside Run has none.
type workerKey struct{}

// nameIDs tie the events of one check of a name together: nid numbers the
// check, w the worker that runs it (0 outside Run).
type nameIDs struct {
	nid int64
	w   int
}

// before is what a try of a name waited for after the previous try.
type before struct {
	retryDelay time.Duration // the delay before a retry
	probeWait  time.Duration // the throttle probe (made or waited for)
	reason     string        // why the previous try was not the last
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
			atomic.AddInt64(&c.stats.busy, 1)
			r := c.CheckOne(ctx, name)
			atomic.AddInt64(&c.stats.busy, -1)
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
func (c *Checker) CheckOne(ctx context.Context, raw string) (res Result) {
	start := time.Now()
	if rtrace.IsEnabled() {
		// In an execution trace (-exectrace, /debug/pprof/trace) every
		// check is a task with the regions of its steps.
		var task *rtrace.Task
		ctx, task = rtrace.NewTask(ctx, "check")
		defer task.End()
		rtrace.Log(ctx, "name", raw)
	}
	ids := nameIDs{nid: c.nameSeq.Add(1)}
	ids.w, _ = ctx.Value(workerKey{}).(int)
	// This name's own requests on the network, the wait for a connection
	// (part of Wait), and the work on this machine (parsing, page cache).
	var net, connWait, work time.Duration
	tries := 0
	defer func() {
		res.Took = time.Since(start)
		res.Net = net
		res.Work = work
		res.Wait = max(res.Took-net-work, 0)
		res.ConnWait = connWait
		res.Attempts = tries
		if c.eventsOn() {
			c.event(TraceEvent{Kind: "result", NID: ids.nid, Worker: ids.w, Name: res.Username, Status: res.Status,
				Confidence: res.Confidence, Evidence: res.Evidence, Offset: res.Offset, Error: res.Error,
				Attempts: res.Attempts, TookMs: ms(res.Took), NetMs: ms(res.Net), WaitMs: ms(res.Wait),
				ConnWaitMs: ms(res.ConnWait), WorkMs: ms(res.Work), Cached: res.Cached, Stale: res.Stale, NotSent: res.notSent,
				Cancelled: ctx.Err() != nil && res.Status == StatusError})
		}
	}()
	name := strings.ToLower(strings.TrimSpace(strings.TrimPrefix(raw, "@")))
	if err := ValidateUsername(name); err != nil {
		return Result{Username: raw, Status: StatusError, Error: "invalid: " + err.Error()}
	}

	var stale *Result
	if c.cache != nil {
		readStart := time.Now()
		reg := rtrace.StartRegion(ctx, "cache")
		ent, body, ok, cerr := c.cache.get(name)
		reg.End()
		read := time.Since(readStart)
		work += read
		if ok {
			// Re-parse the cached HTML with the current parser so parser
			// improvements apply retroactively to old cache entries.
			parseStart := time.Now()
			pi := ParseStatusDetailed(body)
			work += time.Since(parseStart)
			st, conf, ev := pi.Status, pi.Confidence, pi.Evidence
			if !pi.Found {
				st, conf, ev = ent.Status, ent.Confidence, ent.Evidence
			}
			cached := Result{
				Username:   name,
				Status:     st,
				Confidence: conf,
				Evidence:   "cache; " + ev,
				Cached:     true,
			}
			age := time.Since(ent.FetchedAt)
			if c.cache.fresh(st, ent.FetchedAt) {
				atomic.AddInt64(&c.stats.cacheHits, 1)
				c.debugf("cache hit: %s -> %s (confidence=%d; %s; %v old)", name, st, conf, ev, age.Round(time.Second))
				c.event(TraceEvent{Kind: "cache", NID: ids.nid, Worker: ids.w, Name: name, Cache: "hit", Status: st,
					AgeMs: ms(age), ReadMs: ms(read)})
				return cached
			}
			atomic.AddInt64(&c.stats.cacheStale, 1)
			c.debugf("cache stale: %s -> %s (%v old); re-checking", name, st, age.Round(time.Second))
			c.event(TraceEvent{Kind: "cache", NID: ids.nid, Worker: ids.w, Name: name, Cache: "stale", Status: st,
				AgeMs: ms(age), ReadMs: ms(read)})
			// Only settled verdicts are served after a network failure: a
			// sold or taken name rarely changes, while an auction or a sale
			// seen days ago is likely over.
			if st == StatusSold || st == StatusTaken {
				cached.Stale = true
				stale = &cached
			}
		} else {
			atomic.AddInt64(&c.stats.cacheMisses, 1)
			e := TraceEvent{Kind: "cache", NID: ids.nid, Worker: ids.w, Name: name, Cache: "miss", ReadMs: ms(read)}
			if cerr != nil {
				// An entry that is there but broken used to be a silent
				// miss: it is checked over the network, and now it shows.
				atomic.AddInt64(&c.stats.cacheErrors, 1)
				e.Cache, e.Error = "error", cerr.Error()
				c.debugf("cache error: %s: %v; checking it over the network", name, cerr)
			}
			c.event(e)
		}
	}

	reqURL := fmt.Sprintf(FragmentURLTemplate, url.PathEscape(name))

	// A try counts against Retries only when it says something about the
	// name; tries while Fragment throttles this client do not (see
	// throttled), so a name waits out the pause and is tried again.
	var next before // what the next try waits for, for its trace event
	probe := func(maxAge time.Duration) bool {
		t := time.Now()
		reg := rtrace.StartRegion(ctx, "probe")
		ok := c.probe(ctx, maxAge, ids)
		reg.End()
		next.probeWait += time.Since(t)
		return ok
	}
	for {
		var lastErr string
		var lastRes Result
		attempts := 0
		for {
			tries++
			r, retry, errStr := c.doRequest(ctx, name, reqURL, tries, ids, next)
			next = before{}
			net += r.Net
			connWait += r.ConnWait
			work += r.Work
			if !retry {
				return r
			}
			throttledNow := r.throttleWait > 0
			if throttledNow {
				next.reason = "throttled: HTTP 429"
			} else if r.Status == StatusReserved && !probe(probeReuse) {
				throttledNow = true
				next.reason = `throttled: "302 /" for the probe too`
			}
			if throttledNow {
				// HTTP 429, or a bare "302 /" while a random free name is
				// redirected to "/" too: the client is throttled.
				if c.throttled(r.throttleWait) {
					return Result{Username: name, Status: StatusError, Error: c.blockedError(), notSent: true}
				}
				continue
			}
			if r.Status == StatusReserved {
				// The probe is healthy, so Fragment serves this client: open
				// the gate that the "302 /" left closed. For a reserved name
				// nobody waits; it is retried in its cookie session (a first
				// "302 /" can also be a session start).
				c.openGate()
			}
			lastErr, lastRes = errStr, r
			attempts++
			if attempts > c.cfg.Retries || c.blocked.Load() {
				break // no point in retrying when every answer is a challenge
			}
			next.reason = errStr
			atomic.AddInt64(&c.stats.waitRetry, 1)
			delayStart := time.Now()
			reg := rtrace.StartRegion(ctx, "retry-delay")
			select {
			case <-ctx.Done():
				reg.End()
				atomic.AddInt64(&c.stats.waitRetry, -1)
				return Result{Username: name, Status: StatusError, Error: "cancelled"}
			case <-time.After(c.cfg.RetryDelay):
			}
			reg.End()
			next.retryDelay = time.Since(delayStart)
			atomic.AddInt64(&c.stats.waitRetry, -1)
			if c.blocked.Load() {
				break // found blocked during the pause
			}
			atomic.AddInt64(&c.stats.retries, 1)
		}
		if lastRes.Status == StatusReserved {
			// Distinguish a genuine reserved name from a throttled client by
			// probing a random name that should be free, now. If the probe
			// is also redirected to "/", throttling began during the tries,
			// so their "302 /" proves nothing: the name is checked again
			// after the pause (it used to end as an error, "persistent 302
			// /"), and never reported as a false "reserved".
			if probe(0) {
				c.debugf("@%s: redirect to / persisted after %d attempts; probe healthy -> reserved", name, c.cfg.Retries+1)
				lastRes.Evidence = fmt.Sprintf("redirect=/ after %d attempts; probe healthy -> reserved", c.cfg.Retries+1)
				lastRes.Confidence = 60
				return lastRes
			}
			if ctx.Err() != nil {
				return Result{Username: name, Status: StatusError, Error: "cancelled"}
			}
			next.reason = `throttled: "302 /" persisted and the probe got it too`
			if c.throttled(0) {
				return Result{Username: name, Status: StatusError, Error: c.blockedError(), notSent: true}
			}
			continue
		}
		if stale != nil {
			c.debugf("network failed (%s); serving stale cache for %s", lastErr, name)
			served := *stale
			served.Confidence = 30
			served.Evidence = "stale-cache; " + served.Evidence
			served.Error = "network failed (" + lastErr + "); served stale cache"
			return served
		}
		return Result{Username: name, Status: StatusError, Error: "retries exhausted: " + lastErr}
	}
}

// admit waits until a request may go out: through the gate (one at a time
// until it is open), after the shared pause, at its -rate start.
//
// -rate counts requests: every attempt takes a start, a retry too. It used
// to be taken once per name before the check, so retries went out on top
// of the limit, and names answered from the cache (or invalid ones) used
// up starts without sending anything. If a pause begins, or the gate is
// closed again, while a request waits, the request starts over: the ones
// that waited do not all go out together when the pause ends.
func (c *Checker) admit(ctx context.Context) (release func(), w waits, err error) {
	for {
		t0 := time.Now()
		atomic.AddInt64(&c.stats.waitGate, 1)
		reg := rtrace.StartRegion(ctx, "gate")
		t, err := c.gate.pass(ctx, c.blockedCh)
		reg.End()
		atomic.AddInt64(&c.stats.waitGate, -1)
		w.gate += time.Since(t0)
		if err != nil {
			return nil, w, err
		}
		t1 := time.Now()
		reg = rtrace.StartRegion(ctx, "pause")
		if c.paused() {
			atomic.AddInt64(&c.stats.waitPause, 1)
			err = c.waitBlock(ctx)
			atomic.AddInt64(&c.stats.waitPause, -1)
		} else {
			err = c.waitBlock(ctx)
		}
		reg.End()
		w.pause += time.Since(t1)
		if err != nil {
			t.release()
			return nil, w, err
		}
		t2 := time.Now()
		atomic.AddInt64(&c.stats.waitRate, 1)
		reg = rtrace.StartRegion(ctx, "rate")
		err = c.limiter.Wait(ctx, c.blockedCh)
		reg.End()
		atomic.AddInt64(&c.stats.waitRate, -1)
		w.rate += time.Since(t2)
		if err != nil {
			t.release()
			return nil, w, err
		}
		if c.blocked.Load() {
			t.release()
			return nil, w, errBlocked
		}
		if c.paused() || (!t.scout && c.gate.closedSince(t)) {
			t.release()
			continue
		}
		w.scout = t.scout
		return t.release, w, nil
	}
}

// blockedError is the error of a check that was not sent because the run
// stopped.
func (c *Checker) blockedError() string {
	if c.BlockedBy() == BlockedByThrottle {
		return "not sent: Fragment kept throttling this client"
	}
	return "not sent: Fragment blocks this client (Cloudflare challenge)"
}

func (c *Checker) doRequest(ctx context.Context, name, reqURL string, try int, ids nameIDs, b before) (res Result, retry bool, errStr string) {
	at := c.newAttempt(kindAttempt, name, try, reqURL)
	at.ids, at.before = ids, b
	// Registered first, so it runs last: the network time of this try
	// includes draining and closing the body; then the try reports itself.
	defer func() {
		at.done()
		tm := at.timing(at.snapshot())
		res.Net, res.ConnWait, res.Work = tm.net, tm.connWait, tm.work
		at.finish(res, retry)
	}()
	release, w, err := c.admit(ctx)
	at.waits = w
	if errors.Is(err, errBlocked) {
		msg := c.blockedError()
		return Result{Username: name, Status: StatusError, Error: msg, notSent: true}, false, msg
	}
	if err != nil {
		return Result{Username: name, Status: StatusError, Error: "cancelled"}, false, "cancelled"
	}
	defer release()
	// Runs before the gate's slot is released; every waiting request wakes
	// up and goes ahead. A bare "302 /" and HTTP 429 leave the gate as it
	// is (hold): they can mean throttling, which CheckOne finds out with
	// the probe.
	challenged, hold := false, false
	defer func() {
		if !challenged && !hold && ctx.Err() == nil {
			c.openGate()
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

	at.begin(req)
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), at.clientTrace()))

	reg := rtrace.StartRegion(ctx, "http")
	resp, err := c.client.Do(req)
	reg.End()
	if err != nil {
		c.debugf("%s REQUEST FAILED: %v (%.0fms)", at.who(), err, ms(time.Since(at.start)))
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			if ctx.Err() != nil {
				return Result{Username: name, Status: StatusError, Error: "cancelled"}, false, "cancelled"
			}
			return Result{Username: name, Status: StatusError, Error: err.Error()}, true, err.Error()
		}
		return Result{Username: name, Status: StatusError, Error: err.Error()}, isRetriableNet(err), err.Error()
	}
	defer resp.Body.Close()
	// The body as it came (compressed), counted. When the try is over, the
	// trace keeps the page if it wants it (Config.TraceBodies), and the
	// rest after what the parser read is drained so the connection can be
	// reused.
	raw := &countReader{r: resp.Body, eof: at.bodyEOF}
	var bodyReader io.Reader = raw
	var gz *gzip.Reader
	defer func() {
		defer rtrace.StartRegion(ctx, "drain").End()
		if at.wantBody(res) {
			at.keepBody(bodyReader)
		}
		if gz != nil {
			gz.Close()
		}
		drainBody(raw)
		at.rawBytes = raw.n
		at.trailer = resp.Trailer
	}()
	at.response(resp)

	switch ce := strings.ToLower(strings.TrimSpace(resp.Header.Get("Content-Encoding"))); ce {
	case "gzip":
		g, gzErr := gzip.NewReader(raw)
		if gzErr != nil {
			c.debugf("%s gzip error: %v", at.who(), gzErr)
			return Result{Username: name, Status: StatusError, Error: "gzip: " + gzErr.Error()}, false, gzErr.Error()
		}
		gz, bodyReader = g, g
	case "", "identity":
		// no decompression needed
	default:
		// Accept-Encoding asks for gzip only. Read the raw bytes; the parser
		// will most likely find no marker and report unknown.
		c.debugf("%s unexpected Content-Encoding: %q — reading raw", at.who(), ce)
	}

	c.debugf("%s HTTP %d %s | CE: %s | CT: %s | Server: %s | CF-Ray: %s", at.who(), resp.StatusCode, resp.Proto,
		resp.Header.Get("Content-Encoding"), resp.Header.Get("Content-Type"),
		resp.Header.Get("Server"), resp.Header.Get("Cf-Ray"))

	// Cloudflare marks the answers it replaces with a challenge (HTTP 403
	// today) with this header. They say nothing about the name.
	if strings.EqualFold(strings.TrimSpace(resp.Header.Get("Cf-Mitigated")), "challenge") {
		challenged = true
		return c.challengeResult(at, resp.StatusCode)
	}

	switch resp.StatusCode {
	case 301, 302, 303, 307, 308:
		c.noteAnswer()
		loc := resp.Header.Get("Location")
		c.debugf("%s REDIRECT -> %s", at.who(), loc)
		switch {
		case strings.Contains(loc, "?query="):
			c.noteHealthy()
			return Result{Username: name, Status: StatusFree, Confidence: 95,
				Evidence: "redirect=" + loc}, false, ""
		case isReservedRedirect(loc):
			// A bare 302 to "/" is how Fragment answers both rare reserved
			// names and rate-limited clients. Do not report it as reserved
			// on the first attempt: CheckOne probes and retries.
			hold = true
			return Result{Username: name, Status: StatusReserved, Confidence: 35,
				Evidence: "redirect=/ (reserved or throttled)"}, true, "redirect to /"
		default:
			c.noteHealthy()
			return Result{Username: name, Status: StatusUnknown, Confidence: 20,
				Evidence: "unexpected redirect=" + loc}, false, ""
		}

	case 429:
		// Too many requests from this client: not the worker that got it
		// but every worker waits (CheckOne starts a shared pause of at
		// least Retry-After). The worker used to sleep alone, up to
		// RLWaitMax, while the others kept sending; the try did not count
		// as throttling and used up an attempt.
		atomic.AddInt64(&c.stats.rateLimit, 1)
		ra := resp.Header.Get("Retry-After")
		wait := parseRetryAfter(ra, c.cfg.RetryDelay)
		if wait > c.cfg.RLWaitMax {
			wait = c.cfg.RLWaitMax
		}
		if wait <= 0 {
			wait = c.cfg.RetryDelay
		}
		c.warnf("%s HTTP 429 (Retry-After %q): every request pauses for at least %v", at.who(), ra, wait)
		hold = true
		return Result{Username: name, Status: StatusError, Error: "HTTP 429 (rate limited)",
			throttleWait: wait}, true, "rate limited"

	case 200:
		bufPtr := c.pool.Get().(*[]byte)
		buf := *bufPtr
		if cap(buf) < c.cfg.ReadLimit {
			buf = make([]byte, c.cfg.ReadLimit)
		} else {
			buf = buf[:c.cfg.ReadLimit]
		}
		reg := rtrace.StartRegion(ctx, "body")
		n, readErr := io.ReadFull(io.LimitReader(bodyReader, int64(c.cfg.ReadLimit)), buf)
		reg.End()
		at.bodyDone, at.bytes = time.Now(), n
		if errors.Is(readErr, io.EOF) || errors.Is(readErr, io.ErrUnexpectedEOF) {
			readErr = nil
		}
		body := buf[:n]
		if readErr != nil {
			at.keepHead(body, true)
			*bufPtr = buf
			c.pool.Put(bufPtr)
			return Result{Username: name, Status: StatusError, Error: readErr.Error()}, isRetriableNet(readErr), readErr.Error()
		}
		c.debugf("%s body: %d bytes read (limit %d)", at.who(), n, c.cfg.ReadLimit)
		parseStart := time.Now()
		reg = rtrace.StartRegion(ctx, "parse")
		pi := ParseStatusDetailed(body)
		reg.End()
		at.parse = time.Since(parseStart)
		at.work += at.parse
		at.keepHead(body, !pi.Found || pi.Challenge)
		if pi.Challenge {
			*bufPtr = buf
			c.pool.Put(bufPtr)
			challenged = true
			c.noteChallenge()
			at.challengeRun = c.challengeRun.Load()
			return Result{Username: name, Status: StatusError, Error: "cloudflare challenge",
				Evidence: pi.Evidence}, true, "cloudflare challenge"
		}
		c.noteAnswer()
		c.noteHealthy()
		if c.cache != nil && pi.Found {
			// Best-effort: never let a read-only cache directory fail a
			// check. The error is in the trace and the debug line.
			t := time.Now()
			reg := rtrace.StartRegion(ctx, "cache-write")
			err := c.cache.put(name, reqURL, pi, body)
			reg.End()
			at.cacheWrite = time.Since(t)
			at.work += at.cacheWrite
			switch {
			case err != nil:
				at.cacheErr = err.Error()
				c.debugf("%s page cache write failed: %v", at.who(), err)
			case cacheableStatus(pi.Status):
				at.cacheWriteBytes = n
			}
		}
		if !pi.Found {
			c.debugf("%s STATUS NOT FOUND in %d bytes (increase -readlimit)", at.who(), n)
		} else {
			c.debugf("%s parsed -> %s (confidence=%d; %s; offset: %d bytes, %.1f%% of readLimit)",
				at.who(), pi.Status, pi.Confidence, pi.Evidence, pi.Offset, float64(pi.Offset)/float64(c.cfg.ReadLimit)*100)
		}
		*bufPtr = buf
		c.pool.Put(bufPtr)
		if !pi.Found {
			return Result{Username: name, Status: StatusUnknown,
				Confidence: pi.Confidence, Evidence: pi.Evidence}, false, ""
		}
		return Result{Username: name, Status: pi.Status, Offset: pi.Offset,
			Confidence: pi.Confidence, Evidence: pi.Evidence}, false, ""

	default:
		if resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusServiceUnavailable {
			// A proxy may drop cf-mitigated; the page itself still shows.
			head, _ := io.ReadAll(io.LimitReader(bodyReader, 8<<10))
			at.bodyDone, at.bytes = time.Now(), len(head)
			at.keepHead(head, true)
			if looksLikeChallenge(head) {
				challenged = true
				return c.challengeResult(at, resp.StatusCode)
			}
		}
		c.noteAnswer()
		c.debugf("%s unexpected HTTP %d", at.who(), resp.StatusCode)
		s := fmt.Sprintf("HTTP %d", resp.StatusCode)
		retryable := resp.StatusCode >= 500 && resp.StatusCode <= 599
		return Result{Username: name, Status: StatusError, Error: s}, retryable, s
	}
}

// challengeResult is the answer to a Cloudflare challenge: an error that is
// retried like the 200 interstitial, and counted toward ChallengeLimit.
func (c *Checker) challengeResult(at *attempt, code int) (Result, bool, string) {
	c.noteChallenge()
	at.challengeRun = c.challengeRun.Load()
	c.debugf("%s Cloudflare challenge (HTTP %d)", at.who(), code)
	msg := fmt.Sprintf("cloudflare challenge (HTTP %d)", code)
	return Result{Username: at.name, Status: StatusError, Error: msg,
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

// newHTTPClient builds the client of one Checker. Timeout bounds the
// whole request and every phase of it: the dial, TLS handshake and
// response-header timeouts used to be fixed at 10 s, so a timeout above
// 10 s (-timeout 30s) still gave up after 10 s. Every connection goes
// through Checker.dial, which counts its bytes and puts its life in the
// trace.
func (chk *Checker) newHTTPClient() *http.Client {
	c := chk.cfg
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
		DialContext: chk.dial(&net.Dialer{
			Timeout:   c.Timeout,
			KeepAlive: 30 * time.Second,
		}),
		TLSClientConfig: &tls.Config{
			MinVersion:         tls.VersionTLS12,
			InsecureSkipVerify: c.InsecureTLS,
			KeyLogWriter:       c.KeyLog,
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

// rateLimiter spaces requests evenly: every Wait gets the next start
// time, one interval after the previous one, and sleeps until it. It used
// to be a bucket of rps tokens filled by a ticker: during a pause (a
// throttle cool-down) the bucket filled up, and when the pause ended its
// tokens went out at once on top of the workers that had waited. With
// -rate 10 that was 21 requests in the first 100 ms after a 3 s pause,
// the kind of burst that gets a client throttled again. A pause does not
// bank starts now; the earliest next start is never before "now".
type rateLimiter struct {
	mu       sync.Mutex
	interval time.Duration
	next     time.Time // earliest start of the next request
}

func newRateLimiter(rps int) *rateLimiter {
	if rps <= 0 {
		return nil
	}
	interval := time.Second / time.Duration(rps)
	if interval <= 0 {
		interval = 1
	}
	return &rateLimiter{interval: interval}
}

// Wait blocks until the caller's start time, until ctx is done, or until
// stop is closed (the run gave up). A caller that gives up loses its start
// time; the next one is not moved forward, which matters only when a run
// is stopped.
func (rl *rateLimiter) Wait(ctx context.Context, stop <-chan struct{}) error {
	if rl == nil {
		return nil
	}
	rl.mu.Lock()
	now := time.Now()
	at := rl.next
	if at.Before(now) {
		at = now
	}
	rl.next = at.Add(rl.interval)
	rl.mu.Unlock()
	d := at.Sub(now)
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-stop:
		return errBlocked
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close is kept for Checker.Close; the limiter has no goroutine to stop.
func (rl *rateLimiter) Close() {}

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
	cacheErrors int64
	challenges  int64
	pauses      int64

	busy, inFlight, waitGate, waitPause, waitRate, waitRetry, waitProbe, waitConn int64

	connOpen, bytesIn, bytesOut int64
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
		CacheErrors: atomic.LoadInt64(&s.cacheErrors),
		Challenges:  atomic.LoadInt64(&s.challenges),
		Pauses:      atomic.LoadInt64(&s.pauses),

		Busy:      atomic.LoadInt64(&s.busy),
		InFlight:  atomic.LoadInt64(&s.inFlight),
		WaitGate:  atomic.LoadInt64(&s.waitGate),
		WaitPause: atomic.LoadInt64(&s.waitPause),
		WaitRate:  atomic.LoadInt64(&s.waitRate),
		WaitRetry: atomic.LoadInt64(&s.waitRetry),
		WaitProbe: atomic.LoadInt64(&s.waitProbe),
		WaitConn:  atomic.LoadInt64(&s.waitConn),

		ConnOpen: atomic.LoadInt64(&s.connOpen),
		BytesIn:  atomic.LoadInt64(&s.bytesIn),
		BytesOut: atomic.LoadInt64(&s.bytesOut),
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
	if c.ThrottleMax <= 0 {
		c.ThrottleMax = defaultThrottleMax
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

// maxNameToken caps how much of one separator-free token ScanNames keeps;
// anything longer cannot be a valid name.
const maxNameToken = 4096

// ScanNames splits a list into names the way both modes read lists: names
// are separated by spaces, tabs, new lines, commas and semicolons; a "|"
// ends the names of its line, so a results file ("@name | status") can be
// read back as a list; a leading "@" is stripped and names are
// lower-cased. It reads byte by byte, so one huge line (a single-line CSV
// export) works like one name per line, and memory does not grow with the
// file. fn gets every token in order, valid or not (a token longer than
// maxNameToken is cut there; it is invalid anyway); it may return an error
// to stop. The console used to read one name per line, so "alice, bob"
// was a single invalid name there while the web panel read two.
func ScanNames(r io.Reader, fn func(name string) error) error {
	// The list is read in 64 KB chunks and each chunk is split with a
	// lookup table, so the inner loop is a table lookup per byte; it used
	// to call bufio.Reader.ReadByte for every byte (about 120 MB/s here).
	buf := make([]byte, 64*1024)
	tok := make([]byte, 0, 64)
	skipLine := false
	emit := func() error {
		if len(tok) == 0 {
			return nil
		}
		// One leading "@" is dropped and the name lower-cased: ASCII in
		// place (one allocation per name), anything else by
		// strings.ToLower, which gives the same result for ASCII.
		t := tok
		if t[0] == '@' {
			t = t[1:]
		}
		ascii := true
		for i, c := range t {
			if c >= 0x80 {
				ascii = false
				break
			}
			if 'A' <= c && c <= 'Z' {
				t[i] = c + ('a' - 'A')
			}
		}
		name := string(t)
		if !ascii {
			name = strings.ToLower(name)
		}
		tok = tok[:0]
		if name == "" {
			return nil
		}
		return fn(name)
	}
	for {
		n, rerr := r.Read(buf)
		chunk := buf[:n]
		for len(chunk) > 0 {
			if skipLine {
				nl := bytes.IndexByte(chunk, '\n')
				if nl < 0 {
					chunk = nil
					break
				}
				chunk = chunk[nl+1:]
				skipLine = false
				continue
			}
			k := 0
			for k < len(chunk) && !nameSep[chunk[k]] {
				k++
			}
			if room := maxNameToken - len(tok); room > 0 {
				tok = append(tok, chunk[:min(k, room)]...)
			}
			if k == len(chunk) {
				break // the token goes on in the next chunk
			}
			if err := emit(); err != nil {
				return err
			}
			skipLine = chunk[k] == '|'
			chunk = chunk[k+1:]
		}
		if rerr != nil {
			if err := emit(); err != nil {
				return err
			}
			if rerr == io.EOF {
				return nil
			}
			return rerr
		}
	}
}

// nameSep marks the bytes that end a name in ScanNames. "|" also ends the
// names of its line.
var nameSep = [256]bool{'\n': true, '\r': true, ' ': true, '\t': true, ',': true, ';': true, '|': true}

// ParseNamesReader reads a list with ScanNames and returns the valid names
// without duplicates, in order, and the number of invalid ones.
func ParseNamesReader(r io.Reader) (names []string, rejected int, err error) {
	var seen NameSet
	err = ScanNames(r, func(name string) error {
		if ValidateUsername(name) != nil {
			rejected++
			return nil
		}
		if seen.Add(name) {
			names = append(names, name)
		}
		return nil
	})
	return names, rejected, err
}

// ParseNames is ParseNamesReader for a string (the text box of the panel).
func ParseNames(blob string) (names []string, rejected int) {
	names, rejected, _ = ParseNamesReader(strings.NewReader(blob))
	return names, rejected
}

// probeReuse is how old a probe result may be when a "302 /" only decides
// whether to pause all workers. The verdict "reserved" always takes a new
// probe, so a throttle that starts right after a healthy probe cannot turn
// into false "reserved" results.
const probeReuse = time.Second

// probe reports whether a random free name gets a normal answer (the
// client is not throttled). Concurrent callers share one request: a probe
// that started after the caller asked is fresh by definition, and an older
// result is reused when it is younger than maxAge (0 = always a new probe).
// It used to be one request per caller, and every "302 /" also paused all
// workers for BlockCooldown, about 3 s per attempt of a reserved name.
//
// Stats.WaitProbe counts the workers waiting for another worker's probe;
// the one that makes it is on the network (InFlight). It used to count
// in both.
func (c *Checker) probe(ctx context.Context, maxAge time.Duration, ids nameIDs) bool {
	asked := time.Now()
	atomic.AddInt64(&c.stats.waitProbe, 1)
	c.probeMu.Lock()
	atomic.AddInt64(&c.stats.waitProbe, -1)
	defer c.probeMu.Unlock()
	if c.probeStarted.After(asked) || (maxAge > 0 && !c.probeDone.IsZero() && time.Since(c.probeDone) < maxAge) {
		// Another worker's probe answers this one too: say so in the
		// trace, which otherwise showed a verdict with no probe behind it.
		if c.eventsOn() {
			c.event(TraceEvent{Kind: "probe", NID: ids.nid, Worker: ids.w, Reused: true,
				AgeMs: ms(time.Since(c.probeDone)), WaitMs: ms(time.Since(asked)), Healthy: boolPtr(c.probeOK)})
		}
		return c.probeOK
	}
	c.probeStarted = time.Now()
	c.probeOK = c.probeHealthy(ctx, ids)
	c.probeDone = time.Now()
	if c.probeOK {
		c.noteHealthy()
	}
	return c.probeOK
}

// probeHealthy requests a random username that should resolve to free/taken.
// It returns false when Fragment redirects even the probe to "/", which is
// the signature of IP/session throttling rather than a reserved name. The
// probe does not wait for a -rate start: it is one request shared by all
// workers per check, and behind a queue of starts the decision to pause
// could come many seconds late.
func (c *Checker) probeHealthy(ctx context.Context, ids nameIDs) bool {
	name := fmt.Sprintf("zzprobe%d", time.Now().UnixNano()%1_000_000_000_000)
	reqURL := fmt.Sprintf(FragmentURLTemplate, url.PathEscape(name))
	at := c.newAttempt(kindProbe, name, 0, reqURL)
	at.ids = ids // the name whose answer asked for the probe
	healthy, why := false, ""
	defer func() {
		at.done()
		if c.eventsOn() {
			e := at.event(Result{Error: why}, false)
			e.Healthy = boolPtr(healthy)
			c.event(e)
		}
		if c.debugOn() {
			verdict := "THROTTLED (redirected to / as well)"
			if healthy {
				verdict = "healthy"
			}
			if why != "" {
				verdict = why
			}
			c.debugf("%s", at.line(verdict))
		}
	}()
	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		why = "failed: " + err.Error()
		return false
	}
	req.Header.Set("User-Agent", c.cfg.UserAgent)
	req.Header.Set("Accept", "text/html,application/xhtml+xml")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")
	at.begin(req)
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), at.clientTrace()))
	resp, err := c.client.Do(req)
	if err != nil {
		why = "failed: " + err.Error()
		return false
	}
	at.response(resp)
	raw := &countReader{r: resp.Body, eof: at.bodyEOF}
	// The probe's page goes in the trace with "all", and with "anomalies"
	// when the answer is neither a page nor a redirect.
	if mode := at.bodyMode(); mode == BodiesAll ||
		mode == BodiesAnomalies && (resp.StatusCode < 200 || resp.StatusCode >= 400) {
		at.keepBody(raw)
	}
	drainBody(raw)
	resp.Body.Close()
	at.rawBytes = raw.n
	at.trailer = resp.Trailer
	at.done()
	switch {
	case resp.StatusCode == http.StatusOK:
		healthy = true
	case resp.StatusCode >= 300 && resp.StatusCode <= 399:
		healthy = strings.Contains(resp.Header.Get("Location"), "?query=")
	default:
		why = fmt.Sprintf("not healthy: HTTP %d", resp.StatusCode)
	}
	return healthy
}

// triggerBlock pauses every worker for cfg.BlockCooldown.
func (c *Checker) triggerBlock() { c.pauseFor(c.cfg.BlockCooldown) }

// pauseFor makes every request wait until d from now, unless a pause
// already lasts longer.
func (c *Checker) pauseFor(d time.Duration) {
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

// maxThrottlePause caps the pause that grows while Fragment throttles.
const maxThrottlePause = 2 * time.Minute

// throttled handles a sign that Fragment throttles this client: a bare
// "302 /" while the probe gets "/" too, or HTTP 429 (wait is its
// Retry-After). A sign that comes back while a pause is on belongs to
// that pause. Otherwise a new round starts: every request pauses, for
// BlockCooldown in the first round and twice as long in each next one up
// to maxThrottlePause (or for wait, when Fragment said how long), and the
// gate closes, so after the pause one request goes first and the others
// follow only if it gets a normal answer.
//
// A try during throttling says nothing about the name, so CheckOne does
// not count it as an attempt. Such tries used to use up the attempts
// within one 3 s pause and end as "persistent 302 /" errors, 150 of them
// in one real run. throttled returns true once the throttling has lasted
// ThrottleMax since the last normal answer: the run stops then, and the
// names without a verdict stay pending.
func (c *Checker) throttled(wait time.Duration) bool {
	if c.blocked.Load() {
		return true
	}
	c.throttleMu.Lock()
	now := time.Now()
	if !c.throttling.Load() {
		c.throttling.Store(true)
		c.throttleSince = now
	}
	lasted := now.Sub(c.throttleSince)
	if lasted >= c.cfg.ThrottleMax {
		c.throttleMu.Unlock()
		c.block(BlockedByThrottle, "Fragment has throttled this client for %v: stopping; the names without a verdict stay pending",
			lasted.Round(time.Second))
		return true
	}
	if c.paused() {
		c.throttleMu.Unlock()
		return false
	}
	c.throttleRounds++
	round := c.throttleRounds
	d := wait
	if d <= 0 {
		d = c.cfg.BlockCooldown
		for i := 1; i < round && d < maxThrottlePause; i++ {
			d *= 2
		}
	}
	if d > maxThrottlePause {
		d = maxThrottlePause
	}
	c.closeGate()
	c.pauseFor(d)
	c.throttleMu.Unlock()
	atomic.AddInt64(&c.stats.pauses, 1)
	reason := "throttle"
	if wait > 0 {
		reason = "429"
	}
	c.event(TraceEvent{Kind: "pause", PauseMs: ms(d), Round: round, Reason: reason, ThrottledMs: ms(lasted)})
	c.warnf("Fragment is throttling this client: every request pauses for %v (pause %d, throttled for %v)",
		d, round, lasted.Round(time.Second))
	return false
}

// noteHealthy records a normal answer: Fragment serves this client, so a
// throttled spell, if there was one, is over.
func (c *Checker) noteHealthy() {
	if !c.throttling.Load() {
		return
	}
	c.throttleMu.Lock()
	rounds := c.throttleRounds
	c.throttleRounds = 0
	c.throttleSince = time.Time{}
	c.throttling.Store(false)
	c.throttleMu.Unlock()
	if rounds > 0 {
		c.warnf("Fragment answers normally again after %d pause(s)", rounds)
		c.event(TraceEvent{Kind: "throttle_over", Round: rounds})
	}
}

// paused reports whether the shared cooldown is in force now.
func (c *Checker) paused() bool {
	until := c.blockUntil.Load()
	return until != 0 && time.Now().UnixNano() < until
}

// waitBlock sleeps until the shared cooldown expires, or ctx is cancelled.
// Another worker can push the deadline further while we sleep (a new
// "302 /"), so the deadline is read again after every timer.
func (c *Checker) waitBlock(ctx context.Context) error {
	for {
		if c.blocked.Load() {
			return errBlocked
		}
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
		case <-c.blockedCh:
			t.Stop()
			return errBlocked
		case <-t.C:
		}
	}
}
