// Package main implements fragment_checker — a production-ready, concurrent
// Fragment.com username availability checker.
//
// Version: 3.1
//
// Build (development):
//
//	go build -o fragment_checker ./...
//
// Build (release, stripped + reproducible):
//
//	CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o fragment_checker ./...
//
// Cross-compilation:
//
//	GOOS=linux   GOARCH=amd64 go build -o fragment_checker_linux   ./...
//	GOOS=darwin  GOARCH=arm64 go build -o fragment_checker_macos   ./...
//	GOOS=windows GOARCH=amd64 go build -o fragment_checker.exe     ./...
//
// Usage:
//
//	./fragment_checker -input names.txt -output results.txt
//	./fragment_checker -input names.txt -output results.txt -workers 500 -conns 8 -filter free
//	./fragment_checker -input names.txt -output results.txt -resume -append
//	./fragment_checker -input - -output results.txt           # stdin
//	./fragment_checker -input names.txt -output results.txt -debug
//	./fragment_checker -input names.txt -output results.txt -trace
//
// Log levels:
//
//	(default) — results + progress line once per second
//	-debug    — HTTP codes, body sizes, timings, connection reuse stats
//	-trace    — everything from -debug plus DNS / TCP / TLS / headers / body preview
//
// Failure handling:
//
//	-retries N       — maximum extra attempts per request (default 2)
//	-retry-delay D   — fixed pause between attempts (default 500ms)
//	-rl-wait-max D   — upper bound on the wait imposed by HTTP 429 (default 30s)
//
// SIGINT / SIGTERM trigger a graceful shutdown: in-flight requests complete,
// the output buffer is flushed, and the final statistics are printed.
package cli

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"html"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptrace"
	"net/url"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// version is the human-readable release tag printed on startup.
const Version = "3.1"

// ProgName is used in banners and examples; the unified binary sets it to
// "frgmnt check".
var ProgName = "fragment_checker"

// -----------------------------------------------------------------------------
// Constants
// -----------------------------------------------------------------------------

// fragmentURLTemplate is the URL template used to request the username page.
// It is declared as a var (not const) so tests can swap it for an httptest
// server URL. The production value never changes at runtime.
var fragmentURLTemplate = "https://fragment.com/username/%s"

const (
	// defaultUserAgent mimics a modern Chrome on Windows to blend in with
	// regular traffic. Fragment serves the same HTML for any UA, but using
	// a realistic UA avoids being flagged by upstream WAFs.
	defaultUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) " +
		"AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"

	// defaultReadLimit is the number of body bytes scanned for the status
	// marker (~13 KB plus a safety margin). Override with -readlimit.
	defaultReadLimit = 16384

	// safetyMargin is added to the maximum observed status offset when
	// reporting a recommended -readlimit value in the debug summary.
	safetyMargin = 2048

	// scannerBufMax is the maximum line size accepted by the input scanner.
	scannerBufMax = 1 << 20

	// HTML markers used by the tag-based status parser (v2.3+).
	//
	//   statusMarker      — status badge in the section header
	//                       ("Available" / "On auction" / ...). The
	//                       tm-status-XXX (avail|unavail|taken) class
	//                       suffix follows immediately.
	//   markerCountdown   — visible countdown timer (only on active auctions).
	//   markerMakeOffer   — JS hook class on the "Make an offer" button
	//                       (only on taken usernames).
	//   markerPlaceBid    — JS hook class on the visible "Place bid" button
	//                       (available / auction). Note: js-place-bid-popup
	//                       (popup form) is present everywhere, so we match
	//                       the -btn suffix specifically.
	statusMarker      = "tm-section-header-status"
	markerCountdown   = "tm-countdown-timer"
	markerMakeOffer   = "js-make-offer-btn"
	markerPlaceBid    = "js-place-bid-btn"
	markerBuyNow      = "js-buy-now-btn"
	markerPurchased   = "Purchased on"
	statusClassPrefix = "tm-status-"

	// HTTP defaults.
	defaultWorkers         = 500
	defaultMaxConnsPerHost = 8 // HTTP/2 multiplex: a few sockets carry hundreds of streams
	defaultTimeout         = 15 * time.Second
	defaultDialTimeout     = 10 * time.Second
	defaultTLSTimeout      = 10 * time.Second
	defaultRespHdrTimeout  = 10 * time.Second
	defaultIdleTimeout     = 60 * time.Second

	// Channel capacities.
	tasksChanCap  = 4096
	writerChanCap = 4096

	progressInterval = 1 * time.Second
	flushInterval    = 2 * time.Second
)

// -----------------------------------------------------------------------------
// Log level
// -----------------------------------------------------------------------------

// LogLevel controls the verbosity of structured logging.
type LogLevel int

const (
	// LevelNormal prints results and a periodic progress line only.
	LevelNormal LogLevel = iota
	// LevelDebug adds HTTP codes, body sizes, timings, and connection reuse.
	LevelDebug
	// LevelTrace adds DNS / TCP / TLS / header / body-preview events.
	LevelTrace
)

// logLevel is the active verbosity, set once by main from -debug/-trace.
var logLevel LogLevel

// debugf prints when logLevel >= LevelDebug.
func debugf(format string, args ...any) {
	if logLevel >= LevelDebug {
		log.Printf("[DEBUG] "+format, args...)
	}
}

// tracef prints when logLevel >= LevelTrace.
func tracef(format string, args ...any) {
	if logLevel >= LevelTrace {
		log.Printf("[TRACE] "+format, args...)
	}
}

// -----------------------------------------------------------------------------
// Status
// -----------------------------------------------------------------------------

// Status is the resolved availability state of a Fragment username.
type Status string

const (
	StatusFree      Status = "free"      // 302 -> /?query=NAME (username is free on Fragment)
	StatusReserved  Status = "reserved"  // 302 -> / (reserved/blocked, e.g. names starting with "tg")
	StatusAuction   Status = "auction"   // listed on an active auction
	StatusTaken     Status = "taken"     // already owned (Telegram-side)
	StatusSold      Status = "sold"      // purchased on Fragment, no longer listed
	StatusAvailable Status = "available" // can be bought / a new auction can be started
	StatusUnknown   Status = "unknown"   // page returned but status could not be determined
	StatusError     Status = "error"     // network / HTTP / parsing failure
)

// -----------------------------------------------------------------------------
// Result
// -----------------------------------------------------------------------------

// Result is what a worker reports for a single username.
type Result struct {
	Username string
	Status   Status
	Error    string
}

// -----------------------------------------------------------------------------
// Stats
//
// NOTE: all int64 fields are grouped at the top to guarantee 8-byte alignment
// of the atomic counters on 32-bit architectures. Do not add non-int64 fields
// above startTime.
// -----------------------------------------------------------------------------

// Stats aggregates per-status counters and timing histograms across all
// workers. All counter fields are updated lock-free via the sync/atomic
// package.
type Stats struct {
	total           int64
	checked         int64
	free            int64
	reserved        int64
	auction         int64
	taken           int64
	sold            int64
	available       int64
	unknown         int64
	errors          int64
	connNew         int64
	connReused      int64
	rateLimits      int64
	retries         int64
	statusOffsetMax int64
	cacheHits       int64
	cacheMisses     int64
	cacheStale      int64

	freeCount    int64
	freeSumUs    int64
	freeMinUs    int64
	freeMaxUs    int64
	nonfreeCount int64
	nonfreeSumUs int64
	nonfreeMinUs int64
	nonfreeMaxUs int64

	startTime time.Time
}

// newStats returns a Stats with the start time set and min-trackers seeded
// to MaxInt64 so the first observed sample wins the CAS.
func newStats() *Stats {
	return &Stats{
		startTime:    time.Now(),
		freeMinUs:    math.MaxInt64,
		nonfreeMinUs: math.MaxInt64,
	}
}

// record bumps the per-status counter and the overall checked counter.
// It returns the new value of checked so callers can drive progress
// reporting without an extra atomic load.
func (s *Stats) record(st Status) int64 {
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
	return atomic.AddInt64(&s.checked, 1)
}

// atomicMin sets *p = min(*p, v) using a CAS loop.
func atomicMin(p *int64, v int64) {
	for {
		old := atomic.LoadInt64(p)
		if old <= v {
			return
		}
		if atomic.CompareAndSwapInt64(p, old, v) {
			return
		}
	}
}

// atomicMax sets *p = max(*p, v) using a CAS loop.
func atomicMax(p *int64, v int64) {
	for {
		old := atomic.LoadInt64(p)
		if old >= v {
			return
		}
		if atomic.CompareAndSwapInt64(p, old, v) {
			return
		}
	}
}

// recordTiming feeds the per-bucket timing histogram (free/reserved vs. the
// rest). "error" results are intentionally excluded — they would skew the
// latency distribution.
func (s *Stats) recordTiming(st Status, dur time.Duration) {
	us := dur.Microseconds()
	switch st {
	case StatusFree, StatusReserved:
		atomic.AddInt64(&s.freeCount, 1)
		atomic.AddInt64(&s.freeSumUs, us)
		atomicMin(&s.freeMinUs, us)
		atomicMax(&s.freeMaxUs, us)
	case StatusAuction, StatusTaken, StatusSold, StatusAvailable, StatusUnknown:
		atomic.AddInt64(&s.nonfreeCount, 1)
		atomic.AddInt64(&s.nonfreeSumUs, us)
		atomicMin(&s.nonfreeMinUs, us)
		atomicMax(&s.nonfreeMaxUs, us)
	}
}

// recordStatusOffset tracks the maximum byte offset at which the status
// marker was found. Useful for tuning -readlimit.
func (s *Stats) recordStatusOffset(offset int) {
	atomicMax(&s.statusOffsetMax, int64(offset))
}

// snapshot returns a compact single-line summary for the progress logger.
func (s *Stats) snapshot() string {
	elapsed := time.Since(s.startTime).Seconds()
	checked := atomic.LoadInt64(&s.checked)
	rps := 0.0
	if elapsed > 0 {
		rps = float64(checked) / elapsed
	}
	total := atomic.LoadInt64(&s.total)
	return fmt.Sprintf(
		"%d/%d (%.1f req/s) | free=%d reserved=%d auction=%d available=%d taken=%d sold=%d unknown=%d err=%d | new=%d reuse=%d | 429=%d retry=%d",
		checked, total, rps,
		atomic.LoadInt64(&s.free),
		atomic.LoadInt64(&s.reserved),
		atomic.LoadInt64(&s.auction),
		atomic.LoadInt64(&s.available),
		atomic.LoadInt64(&s.taken),
		atomic.LoadInt64(&s.sold),
		atomic.LoadInt64(&s.unknown),
		atomic.LoadInt64(&s.errors),
		atomic.LoadInt64(&s.connNew),
		atomic.LoadInt64(&s.connReused),
		atomic.LoadInt64(&s.rateLimits),
		atomic.LoadInt64(&s.retries),
	)
}

// finalSummary returns the multi-line summary printed at shutdown.
// Debug mode adds per-bucket latency stats and a readLimit tuning hint.
func (s *Stats) finalSummary() string {
	elapsed := time.Since(s.startTime).Seconds()
	checked := atomic.LoadInt64(&s.checked)
	rps := 0.0
	if elapsed > 0 {
		rps = float64(checked) / elapsed
	}
	total := atomic.LoadInt64(&s.total)
	line := fmt.Sprintf(
		"Checked: %d/%d | Free: %d | Reserved: %d | Auction: %d | Available: %d | Taken: %d | Sold: %d | Unknown: %d | Errors: %d | Elapsed: %.1fs (%.1f req/s)",
		checked, total,
		atomic.LoadInt64(&s.free),
		atomic.LoadInt64(&s.reserved),
		atomic.LoadInt64(&s.auction),
		atomic.LoadInt64(&s.available),
		atomic.LoadInt64(&s.taken),
		atomic.LoadInt64(&s.sold),
		atomic.LoadInt64(&s.unknown),
		atomic.LoadInt64(&s.errors),
		elapsed, rps,
	)
	line += fmt.Sprintf("\n  Connections: new=%d, reused=%d | Rate-limits: %d | Retries: %d",
		atomic.LoadInt64(&s.connNew),
		atomic.LoadInt64(&s.connReused),
		atomic.LoadInt64(&s.rateLimits),
		atomic.LoadInt64(&s.retries),
	)
	line += fmt.Sprintf("\n  Page cache: hits=%d, stale=%d, misses=%d",
		atomic.LoadInt64(&s.cacheHits),
		atomic.LoadInt64(&s.cacheStale),
		atomic.LoadInt64(&s.cacheMisses),
	)
	if logLevel >= LevelDebug {
		fc := atomic.LoadInt64(&s.freeCount)
		if fc > 0 {
			avg := float64(atomic.LoadInt64(&s.freeSumUs)) / float64(fc) / 1000
			mn := float64(atomic.LoadInt64(&s.freeMinUs)) / 1000
			mx := float64(atomic.LoadInt64(&s.freeMaxUs)) / 1000
			line += fmt.Sprintf("\n  Timing FREE:     avg=%.0fms min=%.0fms max=%.0fms (n=%d)", avg, mn, mx, fc)
		}
		nc := atomic.LoadInt64(&s.nonfreeCount)
		if nc > 0 {
			avg := float64(atomic.LoadInt64(&s.nonfreeSumUs)) / float64(nc) / 1000
			mn := float64(atomic.LoadInt64(&s.nonfreeMinUs)) / 1000
			mx := float64(atomic.LoadInt64(&s.nonfreeMaxUs)) / 1000
			line += fmt.Sprintf("\n  Timing NON-FREE: avg=%.0fms min=%.0fms max=%.0fms (n=%d)", avg, mn, mx, nc)
		}
		maxOff := atomic.LoadInt64(&s.statusOffsetMax)
		if maxOff > 0 {
			line += fmt.Sprintf("\n  Status offset max: %d bytes (readLimit=%d, you can tighten it to %d via -readlimit)",
				maxOff, readLimit, maxOff+safetyMargin)
		}
		if unk := atomic.LoadInt64(&s.unknown); unk > 0 {
			line += fmt.Sprintf("\n  WARN: unknown=%d — either readLimit is too small or the redirect target is new; re-run with -trace", unk)
		}
	}
	return line
}

// -----------------------------------------------------------------------------
// Username validation
// -----------------------------------------------------------------------------

// usernameRe matches Fragment-style usernames: lowercase ASCII letters,
// digits and underscore, length 1..64. 64 is a generous upper bound that
// tolerates "odd" entries in million-row input files.
var usernameRe = regexp.MustCompile(`^[a-z0-9_]{1,64}$`)

// validateUsername rejects empty input and anything outside the Fragment
// alphabet. The caller has already stripped a leading "@" and lower-cased
// the value.
func validateUsername(name string) error {
	if name == "" {
		return errors.New("empty")
	}
	if !usernameRe.MatchString(name) {
		return errors.New("invalid characters or length")
	}
	return nil
}

// -----------------------------------------------------------------------------
// Fast tag-based status parser (replaces regex)
// -----------------------------------------------------------------------------

// statusTextMap maps the visible section-header label to a status. This is
// the strongest signal on a Fragment username page: tm-status-avail is used
// for BOTH auctions and fixed-price sales, while the label says which one.
var statusTextMap = map[string]Status{
	"on auction":  StatusAuction,
	"for sale":    StatusAvailable,
	"on sale":     StatusAvailable,
	"available":   StatusAvailable,
	"sold":        StatusSold,
	"taken":       StatusTaken,
	"reserved":    StatusReserved,
	"unavailable": StatusSold,
}

// parseStatusInfo classifies a Fragment username page. Detection priority:
//
//  1. visible label ("On auction", "For sale", "Sold", "Taken", ...)
//  2. js-buy-now-btn     -> available (fixed-price sale)
//  3. js-place-bid-btn + tm-countdown-timer -> auction
//  4. js-make-offer-btn  -> taken
//  5. "Purchased on"     -> sold
//  6. tm-status-* class  -> low-confidence fallback
//
// The old rule "countdown timer => auction" was wrong for the "For sale"
// layout: it also renders a countdown and tm-status-avail, so every
// fixed-price name was reported as an auction. The label and the buy-now
// hook resolve that ambiguity.
func parseStatusInfo(body []byte) (st Status, offset int, found bool, confidence int, evidence string) {
	if looksLikeChallenge(body) {
		return StatusUnknown, 0, false, 0, "cloudflare challenge page"
	}
	idx := bytes.Index(body, []byte(statusMarker))
	if idx < 0 {
		return StatusUnknown, 0, false, 0, ""
	}
	badges := collectStatusBadges(body, idx, 4, 2048)
	if len(badges) == 0 {
		return StatusUnknown, idx, false, 0, ""
	}
	chosen := badges[0]
	for _, b := range badges {
		if _, ok := statusTextMap[b.text]; ok {
			chosen = b
			break
		}
	}
	class := chosen.class
	text := chosen.text

	hasTimer := bytes.Contains(body, []byte(markerCountdown))
	hasMakeOffer := bytes.Contains(body, []byte(markerMakeOffer))
	hasPlaceBid := bytes.Contains(body, []byte(markerPlaceBid))
	hasBuyNow := bytes.Contains(body, []byte(markerBuyNow))
	hasPurchased := bytes.Contains(body, []byte(markerPurchased))

	parts := make([]string, 0, 8)
	if text != "" {
		parts = append(parts, fmt.Sprintf("text=%q", text))
	}
	if class != "" {
		parts = append(parts, "class="+class)
	}
	if len(badges) > 1 {
		bs := make([]string, 0, len(badges))
		for _, b := range badges {
			bs = append(bs, b.text+"/"+b.class)
		}
		parts = append(parts, "badges="+strings.Join(bs, "|"))
	}
	if hasTimer {
		parts = append(parts, "timer")
	}
	if hasMakeOffer {
		parts = append(parts, "make-offer")
	}
	if hasPlaceBid {
		parts = append(parts, "place-bid")
	}
	if hasBuyNow {
		parts = append(parts, "buy-now")
	}
	if hasPurchased {
		parts = append(parts, "purchased-on")
	}
	evidence = strings.Join(parts, "; ")

	if s, ok := statusTextMap[text]; ok {
		conf := 90
		if classMatchesStatus(s, class) {
			conf = 99
		}
		if signalsConflict(s, hasTimer, hasPlaceBid, hasBuyNow, hasMakeOffer) {
			conf -= 10
		}
		return s, idx, true, conf, evidence
	}

	switch {
	case hasBuyNow:
		return StatusAvailable, idx, true, 85, evidence
	case hasPlaceBid && hasTimer:
		return StatusAuction, idx, true, 90, evidence
	case hasPlaceBid:
		return StatusAvailable, idx, true, 60, evidence
	case hasMakeOffer:
		return StatusTaken, idx, true, 85, evidence
	case hasPurchased:
		return StatusSold, idx, true, 85, evidence
	case class == "taken":
		return StatusTaken, idx, true, 70, evidence
	case class == "unavail":
		return StatusSold, idx, true, 70, evidence
	case class == "avail":
		if hasTimer {
			return StatusUnknown, idx, false, 25, evidence
		}
		return StatusAvailable, idx, true, 45, evidence
	default:
		return StatusUnknown, idx, false, 0, evidence
	}
}

// statusBadge is one tm-section-header-status entry in the page header.
type statusBadge struct {
	text  string
	class string
}

// collectStatusBadges returns up to max badges within window bytes starting
// at first, so headers like "BANNED" + "Sold" are handled correctly.
func collectStatusBadges(body []byte, first, max, window int) []statusBadge {
	limit := first + window
	if limit > len(body) {
		limit = len(body)
	}
	var out []statusBadge
	pos := first
	for len(out) < max && pos < limit {
		i := bytes.Index(body[pos:limit], []byte(statusMarker))
		if i < 0 {
			break
		}
		at := pos + i
		cw := body[at+len(statusMarker):]
		if len(cw) > 512 {
			cw = cw[:512]
		}
		out = append(out, statusBadge{text: extractStatusText(body, at), class: extractStatusClass(cw)})
		pos = at + len(statusMarker)
	}
	return out
}

func classMatchesStatus(st Status, class string) bool {
	want := ""
	switch st {
	case StatusAuction, StatusAvailable:
		want = "avail"
	case StatusTaken:
		want = "taken"
	case StatusSold:
		want = "unavail"
	case StatusReserved:
		want = "reserved"
	}
	return want != "" && class == want
}

func signalsConflict(st Status, timer, placeBid, buyNow, makeOffer bool) bool {
	switch st {
	case StatusSold, StatusTaken:
		return timer || placeBid || buyNow
	case StatusAuction:
		// Auctions may also offer buy-now; the label still wins.
		return false
	case StatusAvailable:
		return makeOffer && !buyNow
	}
	return false
}

// extractStatusText returns the lower-cased label inside the status span.
func extractStatusText(body []byte, idx int) string {
	gt := bytes.IndexByte(body[idx:], '>')
	if gt < 0 {
		return ""
	}
	start := idx + gt + 1
	if start >= len(body) {
		return ""
	}
	rest := body[start:]
	end := bytes.Index(rest, []byte("</"))
	if end < 0 || end > 200 {
		if lt := bytes.IndexByte(rest, '<'); lt >= 0 && lt <= 200 {
			end = lt
		} else {
			return ""
		}
	}
	raw := rest[:end]
	if bytes.IndexByte(raw, '<') >= 0 {
		raw = stripTags(raw)
	}
	txt := html.UnescapeString(string(raw))
	return strings.ToLower(strings.Join(strings.Fields(txt), " "))
}

func stripTags(b []byte) []byte {
	out := make([]byte, 0, len(b))
	inTag := false
	for _, c := range b {
		switch {
		case c == '<':
			inTag = true
		case c == '>':
			inTag = false
		case !inTag:
			out = append(out, c)
		}
	}
	return out
}

func looksLikeChallenge(body []byte) bool {
	if len(body) > 4096 {
		body = body[:4096]
	}
	return bytes.Contains(body, []byte("cf-mitigated")) ||
		bytes.Contains(body, []byte("Just a moment...")) ||
		bytes.Contains(body, []byte("_cf_chl_opt"))
}

// extractStatusClass returns the class suffix that follows statusClassPrefix
// inside the given window. For ` tm-status-avail">…` it returns "avail".
func extractStatusClass(window []byte) string {
	i := bytes.Index(window, []byte(statusClassPrefix))
	if i < 0 {
		return ""
	}
	start := i + len(statusClassPrefix)
	if start >= len(window) {
		return ""
	}
	end := start
	for end < len(window) {
		c := window[end]
		if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' || c == '_' {
			end++
			continue
		}
		break
	}
	if end == start {
		return ""
	}
	return string(window[start:end])
}

// -----------------------------------------------------------------------------
// Body buffer pool
// -----------------------------------------------------------------------------

// readLimit is the per-response body size that we read for status parsing.
// Set once from the -readlimit flag in main and never mutated afterwards.
var readLimit = defaultReadLimit

// bodyBufPool recycles []byte slices of size readLimit. Pointers are stored
// (rather than slices directly) per the official sync.Pool recommendation,
// which avoids an allocation on each Put.
var bodyBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, readLimit)
		return &b
	},
}

// -----------------------------------------------------------------------------
// HTTP client
// -----------------------------------------------------------------------------

// httpConfig collects the HTTP transport tunables exposed via flags.
type httpConfig struct {
	maxConns           int
	timeout            time.Duration
	dialTimeout        time.Duration
	tlsTimeout         time.Duration
	respHdrTimeout     time.Duration
	idleTimeout        time.Duration
	insecureSkipVerify bool
}

// newHTTPClient builds an *http.Client configured for high-concurrency
// scraping of a single host with HTTP/2 multiplexing. Redirects are NOT
// followed automatically — the caller inspects the 3xx response directly
// to distinguish StatusFree from StatusReserved.
func newHTTPClient(c httpConfig) *http.Client {
	transport := &http.Transport{
		MaxIdleConns:          c.maxConns,
		MaxIdleConnsPerHost:   c.maxConns,
		MaxConnsPerHost:       c.maxConns,
		IdleConnTimeout:       c.idleTimeout,
		TLSHandshakeTimeout:   c.tlsTimeout,
		ResponseHeaderTimeout: c.respHdrTimeout,
		ExpectContinueTimeout: 1 * time.Second,
		ForceAttemptHTTP2:     true,
		// We set Accept-Encoding ourselves and decompress manually. This
		// gives us honest control over readLimit (16 KB of compressed
		// bytes already covers ~100 KB of HTML, so a single limit works
		// universally).
		DisableCompression: true,
		DialContext: (&net.Dialer{
			Timeout:   c.dialTimeout,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		TLSClientConfig: &tls.Config{
			MinVersion:         tls.VersionTLS12,
			InsecureSkipVerify: c.insecureSkipVerify,
		},
	}
	jar, _ := cookiejar.New(nil)
	return &http.Client{
		Transport: transport,
		Jar:       jar,
		Timeout:   c.timeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

// -----------------------------------------------------------------------------
// HTTP trace
// -----------------------------------------------------------------------------

// requestTrace captures per-request timing for the -trace and -debug
// summaries. One instance is created per HTTP attempt.
type requestTrace struct {
	username     string
	requestStart time.Time
	dnsStart     time.Time
	dnsDone      time.Time
	connectStart time.Time
	connectDone  time.Time
	tlsStart     time.Time
	tlsDone      time.Time
	gotFirstByte time.Time
	connReused   bool
	remoteAddr   string
	tlsVersion   string
}

// fullTrace returns an httptrace.ClientTrace that records every event
// (DNS / TCP / TLS / response body). Only attached at -trace level.
func (t *requestTrace) fullTrace(stats *Stats) *httptrace.ClientTrace {
	return &httptrace.ClientTrace{
		DNSStart: func(info httptrace.DNSStartInfo) {
			t.dnsStart = time.Now()
			tracef("@%s DNS start: %s", t.username, info.Host)
		},
		DNSDone: func(info httptrace.DNSDoneInfo) {
			t.dnsDone = time.Now()
			if info.Err != nil {
				tracef("@%s DNS FAILED: %v", t.username, info.Err)
				return
			}
			addrs := make([]string, len(info.Addrs))
			for i, a := range info.Addrs {
				addrs[i] = a.String()
			}
			tracef("@%s DNS -> %s (%.1fms)", t.username, strings.Join(addrs, ", "),
				ms(t.dnsDone.Sub(t.dnsStart)))
		},
		ConnectStart: func(network, addr string) {
			t.connectStart = time.Now()
			tracef("@%s TCP start -> %s %s", t.username, network, addr)
		},
		ConnectDone: func(network, addr string, err error) {
			t.connectDone = time.Now()
			t.remoteAddr = addr
			if err != nil {
				tracef("@%s TCP FAILED: %v", t.username, err)
				return
			}
			tracef("@%s TCP connected -> %s (%.1fms)", t.username, addr,
				ms(t.connectDone.Sub(t.connectStart)))
		},
		TLSHandshakeStart: func() {
			t.tlsStart = time.Now()
			tracef("@%s TLS start", t.username)
		},
		TLSHandshakeDone: func(state tls.ConnectionState, err error) {
			t.tlsDone = time.Now()
			if err != nil {
				tracef("@%s TLS FAILED: %v", t.username, err)
				return
			}
			switch state.Version {
			case tls.VersionTLS13:
				t.tlsVersion = "TLS1.3"
			case tls.VersionTLS12:
				t.tlsVersion = "TLS1.2"
			default:
				t.tlsVersion = fmt.Sprintf("0x%04x", state.Version)
			}
			tracef("@%s TLS done: %s proto=%s (%.1fms)",
				t.username, t.tlsVersion, state.NegotiatedProtocol,
				ms(t.tlsDone.Sub(t.tlsStart)))
		},
		GotConn: func(info httptrace.GotConnInfo) {
			t.connReused = info.Reused
			if info.Reused {
				atomic.AddInt64(&stats.connReused, 1)
				tracef("@%s conn REUSED (idle %v)", t.username, info.IdleTime)
			} else {
				atomic.AddInt64(&stats.connNew, 1)
				tracef("@%s conn NEW", t.username)
			}
		},
		GotFirstResponseByte: func() {
			t.gotFirstByte = time.Now()
			tracef("@%s TTFB=%.1fms", t.username,
				ms(t.gotFirstByte.Sub(t.requestStart)))
		},
	}
}

// connCounterTrace is a lightweight trace that only counts new vs. reused
// connections. Always attached in normal/debug mode so the connection
// statistics are available without paying for full tracing.
func connCounterTrace(stats *Stats) *httptrace.ClientTrace {
	return &httptrace.ClientTrace{
		GotConn: func(info httptrace.GotConnInfo) {
			if info.Reused {
				atomic.AddInt64(&stats.connReused, 1)
			} else {
				atomic.AddInt64(&stats.connNew, 1)
			}
		},
	}
}

// logSummary emits a one-line debug summary describing a finished request.
func (t *requestTrace) logSummary(status Status, bodySize int, totalDur time.Duration) {
	if logLevel < LevelDebug {
		return
	}
	parts := []string{
		fmt.Sprintf("@%-20s", t.username),
		fmt.Sprintf("-> %-9s", status),
		fmt.Sprintf("body=%5dB", bodySize),
		fmt.Sprintf("total=%6.0fms", ms(totalDur)),
	}
	if t.connReused {
		parts = append(parts, "conn=reused")
	} else {
		parts = append(parts, "conn=new")
		if !t.dnsStart.IsZero() && !t.dnsDone.IsZero() {
			parts = append(parts, fmt.Sprintf("dns=%.0fms", ms(t.dnsDone.Sub(t.dnsStart))))
		}
		if !t.connectStart.IsZero() && !t.connectDone.IsZero() {
			parts = append(parts, fmt.Sprintf("tcp=%.0fms", ms(t.connectDone.Sub(t.connectStart))))
		}
		if !t.tlsStart.IsZero() && !t.tlsDone.IsZero() {
			parts = append(parts, fmt.Sprintf("tls=%.0fms", ms(t.tlsDone.Sub(t.tlsStart))))
		}
	}
	if !t.gotFirstByte.IsZero() {
		parts = append(parts, fmt.Sprintf("ttfb=%.0fms", ms(t.gotFirstByte.Sub(t.requestStart))))
	}
	if t.remoteAddr != "" {
		parts = append(parts, t.remoteAddr)
	}
	debugf("%s", strings.Join(parts, " | "))
}

// ms converts a duration to milliseconds as float64, useful for formatting.
func ms(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000.0
}

// -----------------------------------------------------------------------------
// Retry-After parser
// -----------------------------------------------------------------------------

// parseRetryAfter understands both forms of the HTTP Retry-After header:
// delta-seconds (e.g. "30") and HTTP-date (e.g. "Fri, 31 Dec 1999 23:59:59 GMT").
// Returns fallback when the value is missing, malformed, or negative.
func parseRetryAfter(raw string, fallback time.Duration) time.Duration {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return fallback
	}
	// 1) Numeric (delta-seconds).
	if secs, err := strconv.Atoi(raw); err == nil {
		if secs < 0 {
			return fallback
		}
		return time.Duration(secs) * time.Second
	}
	// 2) HTTP-date.
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
// Single-username check (manual retries, no exponential backoff)
// -----------------------------------------------------------------------------

// checkConfig collects per-request retry tunables and the optional page cache.
type checkConfig struct {
	retries       int
	retryDelay    time.Duration
	rlWaitMax     time.Duration
	cache         *pageCache
	blockCooldown time.Duration
}

// cliBlockUntil is a unix-nano deadline shared by all workers after a bare
// 302 to "/" (reserved or throttled).
var cliBlockUntil atomic.Int64

// checkOne validates and resolves a single Fragment username, applying the
// configured retry policy. It always returns a Result — never panics.
// Fresh cached pages are answered without a network request; a stale cached
// verdict is used as a fallback if the network ultimately fails.
func checkOne(ctx context.Context, raw string, client *http.Client, stats *Stats, cfg checkConfig) Result {
	name := strings.ToLower(strings.TrimSpace(strings.TrimPrefix(raw, "@")))
	if err := validateUsername(name); err != nil {
		return Result{Username: raw, Status: StatusError, Error: "invalid: " + err.Error()}
	}

	var stale *Result
	if cfg.cache != nil {
		if ent, body, ok := cfg.cache.get(name); ok {
			st, _, found, conf, ev := parseStatusInfo(body)
			if !found {
				st, conf, ev = ent.Status, ent.Confidence, ent.Evidence
			}
			if cfg.cache.fresh(st, ent.FetchedAt) {
				atomic.AddInt64(&stats.cacheHits, 1)
				debugf("cache hit: %s -> %s (confidence=%d; %s)", name, st, conf, ev)
				return Result{name, st, ""}
			}
			atomic.AddInt64(&stats.cacheStale, 1)
			debugf("cache stale: %s -> %s; re-checking", name, st)
			stale = &Result{name, st, ""}
		} else {
			atomic.AddInt64(&stats.cacheMisses, 1)
		}
	}

	reqURL := fmt.Sprintf(fragmentURLTemplate, url.PathEscape(name))

	var lastErr string
	var lastRes Result
	for attempt := 0; attempt <= cfg.retries; attempt++ {
		if attempt > 0 {
			atomic.AddInt64(&stats.retries, 1)
			select {
			case <-ctx.Done():
				return Result{name, StatusError, "cancelled"}
			case <-time.After(cfg.retryDelay):
			}
		}
		res, retry, errStr := doRequest(ctx, name, reqURL, client, stats, cfg)
		if !retry {
			return res
		}
		lastErr = errStr
		lastRes = res
	}
	if lastRes.Status == StatusReserved {
		// Confirm with a probe: if even a random name is redirected to "/",
		// the client is throttled and this is not a reserved username.
		if probeHealthy(ctx, client) {
			debugf("@%s: redirect to / persisted after %d attempts; probe healthy -> reserved", name, cfg.retries+1)
			return lastRes
		}
		return Result{name, StatusError, "persistent 302 / (throttled; probe also blocked)"}
	}
	if stale != nil {
		debugf("network failed (%s); serving stale cache for %s", lastErr, name)
		return *stale
	}
	return Result{name, StatusError, "retries exhausted: " + lastErr}
}

// doRequest performs a single HTTP attempt. The boolean return value is
// true when the attempt should be retried (either due to a transient
// network error, a 5xx response, or a 429 rate-limit that was honoured
// in-place).
func doRequest(ctx context.Context, name, reqURL string, client *http.Client, stats *Stats, cfg checkConfig) (res Result, retry bool, errStr string) {
	if err := waitBlockCooldown(ctx, cfg.blockCooldown); err != nil {
		return Result{name, StatusError, "cancelled"}, false, "cancelled"
	}
	start := time.Now()

	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return Result{name, StatusError, err.Error()}, false, err.Error()
	}
	req.Header.Set("User-Agent", defaultUserAgent)
	req.Header.Set("Accept", "text/html,application/xhtml+xml")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")
	req.Header.Set("Accept-Encoding", "gzip")

	// Trace: full ClientTrace at -trace, lightweight conn counter otherwise.
	var trace *requestTrace
	if logLevel >= LevelTrace {
		trace = &requestTrace{username: name, requestStart: start}
		req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace.fullTrace(stats)))
	} else {
		req = req.WithContext(httptrace.WithClientTrace(req.Context(), connCounterTrace(stats)))
	}

	resp, err := client.Do(req)
	if err != nil {
		debugf("@%s REQUEST FAILED: %v (%.0fms)", name, err, ms(time.Since(start)))
		// Honour parent-context cancellations; retry only client-timeouts.
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			if ctx.Err() != nil {
				return Result{name, StatusError, "cancelled"}, false, "cancelled"
			}
			return Result{name, StatusError, err.Error()}, true, err.Error()
		}
		return Result{name, StatusError, err.Error()}, isRetriableNet(err), err.Error()
	}
	// Defer order: Close runs last (LIFO), drain runs first to free the
	// HTTP/1.1 socket back into the idle pool.
	defer resp.Body.Close()
	defer drainBody(resp.Body)

	// Manual gzip handling — we explicitly set Accept-Encoding: gzip and
	// DisableCompression: true. If Cloudflare ever returns br/deflate
	// (observed in some curl_trace dumps), we just read the raw bytes;
	// the parser will report unknown and a debug warning will point at
	// the issue.
	var bodyReader io.Reader = resp.Body
	ce := strings.ToLower(strings.TrimSpace(resp.Header.Get("Content-Encoding")))
	switch ce {
	case "gzip":
		gz, gzErr := gzip.NewReader(resp.Body)
		if gzErr != nil {
			debugf("@%s gzip error: %v", name, gzErr)
			return Result{name, StatusError, "gzip: " + gzErr.Error()}, false, gzErr.Error()
		}
		defer gz.Close()
		bodyReader = gz
	case "", "identity":
		// no decompression needed
	default:
		// Unexpected encoding — Cloudflare should have respected our
		// Accept-Encoding: gzip. Read raw; the parser will most likely
		// return unknown.
		debugf("@%s unexpected Content-Encoding: %q — reading raw", name, ce)
	}

	debugf("@%s HTTP %d | CE: %s | CT: %s | Server: %s | CF-Ray: %s",
		name, resp.StatusCode,
		resp.Header.Get("Content-Encoding"),
		resp.Header.Get("Content-Type"),
		resp.Header.Get("Server"),
		resp.Header.Get("Cf-Ray"))

	if logLevel >= LevelTrace {
		for k, vals := range resp.Header {
			tracef("@%s  header  %s: %s", name, k, strings.Join(vals, "; "))
		}
	}

	finish := func(st Status, bodySize int) {
		if trace != nil {
			trace.logSummary(st, bodySize, time.Since(start))
		}
	}

	switch resp.StatusCode {

	case 301, 302:
		loc := resp.Header.Get("Location")
		debugf("@%s REDIRECT -> %s", name, loc)
		switch {
		case strings.Contains(loc, "?query="):
			// /?query=NAME — the username is free on Fragment.
			stats.recordTiming(StatusFree, time.Since(start))
			finish(StatusFree, 0)
			return Result{name, StatusFree, ""}, false, ""
		case isReservedRedirect(loc):
			// A bare 302 to "/" is used both for rare reserved names and
			// for throttled clients. Pause all workers and retry instead of
			// reporting "reserved" on the first attempt.
			triggerBlockCooldown(cfg.blockCooldown)
			return Result{name, StatusReserved, ""}, true, "redirect to /"
		default:
			stats.recordTiming(StatusUnknown, time.Since(start))
			finish(StatusUnknown, 0)
			return Result{name, StatusUnknown, ""}, false, ""
		}

	case 429:
		atomic.AddInt64(&stats.rateLimits, 1)
		ra := resp.Header.Get("Retry-After")
		wait := parseRetryAfter(ra, cfg.retryDelay)
		if wait > cfg.rlWaitMax {
			wait = cfg.rlWaitMax
		}
		log.Printf("[WARN] @%s 429 Retry-After=%q sleeping=%v", name, ra, wait)
		// Release the body and free the connection BEFORE sleeping — this
		// frees a slot in the connection pool while we wait.
		drainBody(resp.Body)
		resp.Body.Close()
		select {
		case <-ctx.Done():
			return Result{name, StatusError, "cancelled"}, false, "cancelled"
		case <-time.After(wait):
		}
		return Result{}, true, "rate limited"

	case 200:
		bufPtr := bodyBufPool.Get().(*[]byte)
		buf := (*bufPtr)
		if cap(buf) < readLimit {
			buf = make([]byte, readLimit)
		} else {
			buf = buf[:readLimit]
		}
		n, readErr := io.ReadFull(io.LimitReader(bodyReader, int64(readLimit)), buf)
		// EOF / UnexpectedEOF are not errors — the body was just shorter
		// than readLimit.
		if errors.Is(readErr, io.EOF) || errors.Is(readErr, io.ErrUnexpectedEOF) {
			readErr = nil
		}
		body := buf[:n]
		if readErr != nil {
			*bufPtr = buf
			bodyBufPool.Put(bufPtr)
			finish(StatusError, n)
			return Result{name, StatusError, readErr.Error()}, isRetriableNet(readErr), readErr.Error()
		}
		debugf("@%s body: %d bytes read (limit %d)", name, n, readLimit)

		st, offset, found, confidence, evidence := parseStatusInfo(body)
		if found && cfg.cache != nil {
			cfg.cache.put(name, reqURL, st, confidence, evidence, body)
		}
		*bufPtr = buf
		bodyBufPool.Put(bufPtr)
		if found {
			stats.recordTiming(st, time.Since(start))
			stats.recordStatusOffset(offset)
			debugf("@%s parsed -> %s (confidence=%d; %s; offset: %d bytes, %.1f%% of readLimit)",
				name, st, confidence, evidence, offset, float64(offset)/float64(readLimit)*100)
			finish(st, n)
			return Result{name, st, ""}, false, ""
		}
		debugf("@%s STATUS NOT FOUND in %d bytes (increase -readlimit)", name, n)
		if logLevel >= LevelTrace && n > 0 {
			preview := string(body)
			if len(preview) > 300 {
				preview = preview[:300] + "…"
			}
			tracef("@%s body preview:\n%s", name, preview)
		}
		stats.recordTiming(StatusUnknown, time.Since(start))
		finish(StatusUnknown, n)
		return Result{name, StatusUnknown, ""}, false, ""

	default:
		debugf("@%s unexpected HTTP %d", name, resp.StatusCode)
		finish(StatusError, 0)
		// 5xx is retryable, anything else is not.
		s := fmt.Sprintf("HTTP %d", resp.StatusCode)
		retryable := resp.StatusCode >= 500 && resp.StatusCode <= 599
		return Result{name, StatusError, s}, retryable, s
	}
}

// isReservedRedirect detects redirects to "/" (or its absolute form) which
// Fragment uses to mark a username as reserved/blocked (for example, any
// name starting with "tg"). An empty Location is NOT treated as reserved —
// that is a malformed 302 and falls through to unknown.
func isReservedRedirect(loc string) bool {
	if loc == "/" {
		return true
	}
	if loc == "" {
		return false
	}
	// Absolute URL pointing at fragment.com/ with no path component
	// (or just "/") and no query string.
	if u, err := url.Parse(loc); err == nil {
		if u.Host == "" || u.Host == "fragment.com" {
			if (u.Path == "" || u.Path == "/") && u.RawQuery == "" {
				return true
			}
		}
	}
	return false
}

// isRetriableNet decides whether a transport-level error is worth a retry.
// Context cancellation is never retried. Everything else is — better to
// waste an attempt than to give up on a transient blip.
func isRetriableNet(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	var nerr net.Error
	if errors.As(err, &nerr) {
		// timeout / DNS / connection — retry.
		return true
	}
	return true
}

// drainBody drains the response body so the underlying connection can be
// returned to the idle pool under HTTP/1.1. Under HTTP/2 it is effectively
// a no-op for already-finished streams. Fragment pages are small (<300 KB)
// and the client.Timeout still applies, so this cannot stall indefinitely.
func drainBody(body io.Reader) {
	_, _ = io.Copy(io.Discard, body)
}

// -----------------------------------------------------------------------------
// Input streaming
// -----------------------------------------------------------------------------

// produceUsernames streams names from src (file or stdin) into tasks.
// When dedup is true, duplicates within the input are skipped. When skip is
// non-nil, names already present in it are dropped (used for -resume).
// Memory usage is O(unique-names) when dedup is on, O(1) otherwise.
func produceUsernames(
	ctx context.Context,
	src io.Reader,
	tasks chan<- string,
	dedup bool,
	skip map[string]struct{},
	stats *Stats,
) (rejected int64, err error) {
	scanner := bufio.NewScanner(src)
	scanner.Buffer(make([]byte, 64*1024), scannerBufMax)

	var seen map[string]struct{}
	if dedup {
		seen = make(map[string]struct{}, 1024)
	}

	for scanner.Scan() {
		raw := scanner.Text()
		name := strings.ToLower(strings.TrimSpace(strings.TrimPrefix(raw, "@")))
		if name == "" {
			continue
		}
		if dedup {
			if _, ok := seen[name]; ok {
				continue
			}
			seen[name] = struct{}{}
		}
		if skip != nil {
			if _, ok := skip[name]; ok {
				rejected++
				continue
			}
		}
		select {
		case <-ctx.Done():
			return rejected, ctx.Err()
		case tasks <- name:
			atomic.AddInt64(&stats.total, 1)
		}
	}
	return rejected, scanner.Err()
}

// -----------------------------------------------------------------------------
// Resume: load already-processed names from an existing output file
// -----------------------------------------------------------------------------

// loadResumeSet reads the existing output file and returns the set of
// usernames already present there. Returns nil, nil when the file does
// not exist (first run is allowed).
func loadResumeSet(path string) (map[string]struct{}, error) {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), scannerBufMax)
	set := make(map[string]struct{})
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "@") {
			continue
		}
		pipe := strings.Index(line, " | ")
		if pipe < 0 {
			continue
		}
		name := strings.ToLower(strings.TrimSpace(line[1:pipe]))
		if name != "" {
			set[name] = struct{}{}
		}
	}
	return set, scanner.Err()
}

// -----------------------------------------------------------------------------
// Filter parsing
// -----------------------------------------------------------------------------

// parseFilter accepts a comma-separated list of statuses (e.g. "free,auction")
// and returns a set used by workers to decide which results to write to the
// output file. An empty string disables filtering.
func parseFilter(raw string) (map[Status]bool, error) {
	if raw == "" {
		return nil, nil
	}
	filter := make(map[Status]bool)
	for _, p := range strings.Split(raw, ",") {
		p = strings.TrimSpace(strings.ToLower(p))
		switch Status(p) {
		case StatusFree, StatusReserved, StatusAuction,
			StatusTaken, StatusSold, StatusAvailable,
			StatusUnknown, StatusError:
			filter[Status(p)] = true
		default:
			return nil, fmt.Errorf("unknown status: %q", p)
		}
	}
	return filter, nil
}

// -----------------------------------------------------------------------------
// Sink: single writer goroutine for file + stdout (no tearing, no contention)
// -----------------------------------------------------------------------------

// sinkJob is one record to be written. Either side can be empty to skip
// writing to that target.
type sinkJob struct {
	fileLine    string // "" means: do not write to the output file
	stdoutLine  string // "" means: do not write to stdout
	stdoutColor string // ANSI color code, or "" for no color
}

// runSink consumes sinkJob values until the channel is closed. It does NOT
// react to ctx cancellation — that is intentional, to guarantee that every
// result already counted in Stats also lands on disk before shutdown.
// Errors from the final flush are reported via the done channel.
func runSink(
	jobs <-chan sinkJob,
	fileW *bufio.Writer,
	stdoutW *bufio.Writer,
	done chan<- error,
) {
	defer func() {
		ferr := fileW.Flush()
		serr := stdoutW.Flush()
		if ferr != nil {
			done <- fmt.Errorf("file flush: %w", ferr)
			return
		}
		if serr != nil {
			done <- fmt.Errorf("stdout flush: %w", serr)
			return
		}
		done <- nil
	}()

	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	const resetColor = "\033[0m"

	for {
		select {
		case <-ticker.C:
			// Periodic flush so results become visible on disk in real time.
			_ = fileW.Flush()
			_ = stdoutW.Flush()
		case job, ok := <-jobs:
			if !ok {
				return
			}
			if job.fileLine != "" {
				_, _ = fileW.WriteString(job.fileLine)
				_ = fileW.WriteByte('\n')
			}
			if job.stdoutLine != "" {
				if job.stdoutColor != "" {
					_, _ = stdoutW.WriteString(job.stdoutColor)
					_, _ = stdoutW.WriteString(job.stdoutLine)
					_, _ = stdoutW.WriteString(resetColor)
					_ = stdoutW.WriteByte('\n')
				} else {
					_, _ = stdoutW.WriteString(job.stdoutLine)
					_ = stdoutW.WriteByte('\n')
				}
			}
		}
	}
}

// -----------------------------------------------------------------------------
// Progress reporter
// -----------------------------------------------------------------------------

// runProgress prints a one-line snapshot of Stats every progressInterval.
// It also computes an "instant" requests-per-second value to help spot
// throughput stalls.
func runProgress(ctx context.Context, stats *Stats) {
	ticker := time.NewTicker(progressInterval)
	defer ticker.Stop()
	var lastChecked int64
	lastTime := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now()
			checked := atomic.LoadInt64(&stats.checked)
			delta := checked - lastChecked
			dt := now.Sub(lastTime).Seconds()
			instRPS := 0.0
			if dt > 0 {
				instRPS = float64(delta) / dt
			}
			lastChecked = checked
			lastTime = now
			log.Printf("[progress] %s | inst=%.0f r/s", stats.snapshot(), instRPS)
		}
	}
}

// -----------------------------------------------------------------------------
// Rate limiter (optional, only when -rate > 0)
// -----------------------------------------------------------------------------

// rateLimiter is a simple token-bucket. A background goroutine produces
// tokens at the configured rate; workers call Wait to consume them.
type rateLimiter struct {
	tokens chan struct{}
	stop   chan struct{}
}

// newRateLimiter returns nil when rps <= 0 (rate limiting disabled).
func newRateLimiter(rps int) *rateLimiter {
	if rps <= 0 {
		return nil
	}
	cap := rps
	if cap < 1 {
		cap = 1
	}
	rl := &rateLimiter{
		tokens: make(chan struct{}, cap),
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

// Wait blocks until a token is available or ctx is cancelled. A nil
// receiver is the zero-overhead "no limit" case.
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

// Close stops the token-producing goroutine. Safe on a nil receiver.
func (rl *rateLimiter) Close() {
	if rl == nil {
		return
	}
	close(rl.stop)
}

// -----------------------------------------------------------------------------
// Worker
// -----------------------------------------------------------------------------

// runWorker pulls usernames off tasks, checks each one, and forwards the
// formatted output to sink. ANSI colors are applied to stdout-only lines:
// green for free, magenta for reserved, yellow for auction, red for errors,
// gray for the catch-all in debug mode.
func runWorker(
	ctx context.Context,
	tasks <-chan string,
	sink chan<- sinkJob,
	client *http.Client,
	stats *Stats,
	cfg checkConfig,
	rl *rateLimiter,
	filter map[Status]bool,
	quiet bool,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case name, ok := <-tasks:
			if !ok {
				return
			}
			if err := rl.Wait(ctx); err != nil {
				return
			}
			result := checkOne(ctx, name, client, stats, cfg)
			stats.record(result.Status)

			// Build output lines.
			fileLine := fmt.Sprintf("@%s | %s", result.Username, result.Status)
			if result.Error != "" {
				fileLine += fmt.Sprintf(" (%s)", result.Error)
			}
			if filter != nil && !filter[result.Status] {
				fileLine = "" // do not write to the file
			}

			stdoutLine := ""
			stdoutColor := ""
			switch result.Status {
			case StatusFree:
				stdoutLine = fmt.Sprintf("@%s | %s", result.Username, result.Status)
				stdoutColor = "\033[92m" // bright green
			case StatusReserved:
				if !quiet {
					stdoutLine = fmt.Sprintf("@%s | %s", result.Username, result.Status)
					stdoutColor = "\033[35m" // magenta
				}
			case StatusAuction:
				if !quiet {
					stdoutLine = fmt.Sprintf("@%s | %s", result.Username, result.Status)
					stdoutColor = "\033[93m" // yellow
				}
			case StatusError:
				stdoutLine = fmt.Sprintf("@%s | %s (%s)", result.Username, result.Status, result.Error)
				stdoutColor = "\033[91m" // red
			default:
				if logLevel >= LevelDebug && !quiet {
					stdoutLine = fmt.Sprintf("@%s | %s", result.Username, result.Status)
					stdoutColor = "\033[90m" // gray
				}
			}

			if fileLine == "" && stdoutLine == "" {
				continue
			}

			// Blocking send: the sink lives until close(sink) in main, so
			// no result is lost during graceful shutdown.
			sink <- sinkJob{fileLine: fileLine, stdoutLine: stdoutLine, stdoutColor: stdoutColor}
		}
	}
}

// -----------------------------------------------------------------------------
// Main
// -----------------------------------------------------------------------------

// main wires together the input producer, worker pool, sink, progress
// reporter and signal handler, then waits for everything to drain before
// printing the final summary.
func Main(args []string) int {
	fs := flag.NewFlagSet(ProgName, flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	// Disable the standard log prefix; we use our own tags.
	log.SetFlags(log.Ltime)

	var (
		inputPath  = fs.String("input", "", "Path to the username list (use \"-\" for stdin)")
		baseURL    = fs.String("base-url", "", "Override Fragment base URL (testing/mirror)")
		outputPath = fs.String("output", "", "Path to the results file (required)")
		workers    = fs.Int("workers", defaultWorkers, "Number of parallel workers")
		maxConns   = fs.Int("conns", defaultMaxConnsPerHost, "Max TCP connections to fragment.com (HTTP/2 multiplex)")
		rateRPS    = fs.Int("rate", 0, "Global rate limit, requests per second (0 = unlimited)")
		retries    = fs.Int("retries", 2, "Max manual retries per request (no exponential backoff)")
		retryDelay = fs.Duration("retry-delay", 500*time.Millisecond, "Delay between retries")
		rlWaitMax  = fs.Duration("rl-wait-max", 30*time.Second, "Maximum wait honoured for HTTP 429 Retry-After")
		timeout    = fs.Duration("timeout", defaultTimeout, "Total HTTP request timeout")
		readLimitF = fs.Int("readlimit", defaultReadLimit, "Body bytes to read for status parsing (the -debug summary suggests an optimal value)")

		cacheDir     = fs.String("cache-dir", ".fragment_cache", "Directory for the on-disk page cache")
		cacheTTLFlag = fs.Duration("cache-ttl", defaultCacheTTL, "TTL for volatile pages (available/auction)")
		cacheSoldTTL = fs.Duration("cache-sold-ttl", defaultCacheSoldTTL, "TTL for sold/bought pages (negative = never expire)")
		noCache      = fs.Bool("no-cache", false, "Disable the on-disk page cache")

		filterRaw  = fs.String("filter", "", "Comma-separated status filter (free,reserved,auction,taken,sold,available,unknown,error)")
		appendFlag = fs.Bool("append", false, "Open the output file in append mode (do not truncate)")
		resume     = fs.Bool("resume", false, "Skip usernames already present in the output file")
		noDedup    = fs.Bool("no-dedup", false, "Do not deduplicate the input (saves RAM on huge files)")
		quiet      = fs.Bool("quiet", false, "Stdout: print only free results and errors")
		insecure   = fs.Bool("insecure", false, "Skip TLS certificate verification (debug only)")

		debugFlag = fs.Bool("debug", false, "Enable debug-level logging")
		traceFlag = fs.Bool("trace", false, "Enable trace-level logging (implies -debug)")
		showVer   = fs.Bool("version", false, "Print version and exit")
	)
	fs.Usage = func() { printUsage(fs) }
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}

	if *showVer {
		fmt.Println("fragment_checker", Version)
		return 0
	}

	switch {
	case *traceFlag:
		logLevel = LevelTrace
	case *debugFlag:
		logLevel = LevelDebug
	}

	if *inputPath == "" || *outputPath == "" {
		printUsage(fs)
		return 2
	}
	if *baseURL != "" {
		fragmentURLTemplate = strings.TrimRight(*baseURL, "/") + "/username/%s"
	}
	if *workers < 1 {
		*workers = 1
	}
	if *maxConns < 1 {
		*maxConns = 1
	}
	if *readLimitF < 1024 {
		*readLimitF = 1024
	}
	readLimit = *readLimitF

	// --- Open input ---
	var inReader io.Reader
	if *inputPath == "-" {
		inReader = os.Stdin
	} else {
		f, err := os.Open(*inputPath)
		if err != nil {
			log.Fatalf("Cannot open %s: %v", *inputPath, err)
		}
		defer f.Close()
		inReader = f
	}

	// --- Resume set ---
	var skip map[string]struct{}
	if *resume {
		s, err := loadResumeSet(*outputPath)
		if err != nil {
			log.Fatalf("Cannot read resume set from %s: %v", *outputPath, err)
		}
		skip = s
		log.Printf("[resume] loaded %d already-processed names", len(skip))
	}

	// --- Open output ---
	flags := os.O_CREATE | os.O_WRONLY
	if *appendFlag || *resume {
		flags |= os.O_APPEND
	} else {
		flags |= os.O_TRUNC
	}
	outFile, err := os.OpenFile(*outputPath, flags, 0o644)
	if err != nil {
		log.Fatalf("Cannot open %s: %v", *outputPath, err)
	}
	fileW := bufio.NewWriterSize(outFile, 64*1024)
	stdoutW := bufio.NewWriterSize(os.Stdout, 64*1024)

	// --- Filter ---
	filter, err := parseFilter(*filterRaw)
	if err != nil {
		log.Fatalf("Filter error: %v", err)
	}

	// --- Log level banner ---
	levelName := "normal"
	switch logLevel {
	case LevelDebug:
		levelName = "debug"
	case LevelTrace:
		levelName = "trace"
	}
	filterStr := *filterRaw
	if filterStr == "" {
		filterStr = "all"
	}
	log.Printf("%s v%s | workers=%d conns=%d retries=%d retry-delay=%v rate=%d timeout=%v filter=%s log=%s",
		ProgName, Version, *workers, *maxConns, *retries, *retryDelay, *rateRPS, *timeout, filterStr, levelName)

	// --- Context and signals ---
	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		select {
		case sig := <-sigCh:
			log.Printf("[INFO] received signal %s, shutting down gracefully…", sig)
			cancel()
		case <-ctx.Done():
		}
	}()

	// --- HTTP client ---
	client := newHTTPClient(httpConfig{
		maxConns:           *maxConns,
		timeout:            *timeout,
		dialTimeout:        defaultDialTimeout,
		tlsTimeout:         defaultTLSTimeout,
		respHdrTimeout:     defaultRespHdrTimeout,
		idleTimeout:        defaultIdleTimeout,
		insecureSkipVerify: *insecure,
	})

	// --- Stats ---
	stats := newStats()

	// --- Channels ---
	tasks := make(chan string, tasksChanCap)
	sink := make(chan sinkJob, writerChanCap)
	sinkDone := make(chan error, 1)

	var pc *pageCache
	if !*noCache {
		pc = newPageCache(*cacheDir, *cacheTTLFlag, *cacheSoldTTL)
	}
	cfg := checkConfig{
		retries:       *retries,
		retryDelay:    *retryDelay,
		rlWaitMax:     *rlWaitMax,
		cache:         pc,
		blockCooldown: 3 * time.Second,
	}

	rl := newRateLimiter(*rateRPS)
	defer rl.Close()

	// --- Start sink (lives until close(sink), independent of ctx) ---
	go runSink(sink, fileW, stdoutW, sinkDone)

	// --- Start progress reporter ---
	progCtx, progCancel := context.WithCancel(ctx)
	go runProgress(progCtx, stats)

	// --- Start workers ---
	var wg sync.WaitGroup
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runWorker(ctx, tasks, sink, client, stats, cfg, rl, filter, *quiet)
		}()
	}

	// --- Start input producer ---
	producerDone := make(chan error, 1)
	go func() {
		rejected, err := produceUsernames(ctx, inReader, tasks, !*noDedup, skip, stats)
		if rejected > 0 {
			log.Printf("[resume] skipped %d already-processed names", rejected)
		}
		close(tasks)
		producerDone <- err
	}()

	// --- Wait for producer (or cancellation) ---
	if err := <-producerDone; err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("[WARN] producer error: %v", err)
	}

	// --- Wait for workers ---
	wg.Wait()

	// --- Stop progress reporter ---
	progCancel()

	// --- Close sink and wait for the final flush ---
	close(sink)
	if err := <-sinkDone; err != nil {
		log.Printf("[ERROR] sink: %v", err)
	}

	// --- Close output file ---
	if err := outFile.Close(); err != nil {
		log.Printf("[ERROR] closing output: %v", err)
	}

	// --- Final summary ---
	log.Println(stats.finalSummary())
	return 0
}

// printUsage is installed as flag.Usage and is also called manually when
// required flags are missing.
func printUsage(fs *flag.FlagSet) {
	fmt.Fprintf(os.Stderr, "%s v%s — production-ready Fragment.com username checker\n\n", ProgName, Version)
	fmt.Fprintln(os.Stderr, "Build:")
	fmt.Fprintln(os.Stderr, "  go build -o frgmnt ./cmd/frgmnt    # unified binary")
	fmt.Fprintln(os.Stderr, "  go build -o fragment_checker ./... # standalone")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Examples:")
	fmt.Fprintf(os.Stderr, "  %s -input names.txt -output results.txt\n", ProgName)
	fmt.Fprintf(os.Stderr, "  %s -input names.txt -output results.txt -workers 500 -conns 8\n", ProgName)
	fmt.Fprintf(os.Stderr, "  %s -input names.txt -output results.txt -resume -append\n", ProgName)
	fmt.Fprintf(os.Stderr, "  cat names.txt | %s -input - -output results.txt -no-dedup\n", ProgName)
	fmt.Fprintf(os.Stderr, "  %s -input names.txt -output results.txt -debug -workers 200 -rate 1000\n", ProgName)
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Flags:")
	fs.PrintDefaults()
}

// probeHealthy requests a random username that should resolve to free/taken.
// A bare "/" redirect for the probe means the client is throttled, which
// prevents a transient rate limit from being reported as "reserved".
func probeHealthy(ctx context.Context, client *http.Client) bool {
	name := fmt.Sprintf("zzprobe%d", time.Now().UnixNano()%1_000_000_000_000)
	reqURL := fmt.Sprintf(fragmentURLTemplate, url.PathEscape(name))
	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return false
	}
	req.Header.Set("User-Agent", defaultUserAgent)
	req.Header.Set("Accept", "text/html,application/xhtml+xml")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")
	resp, err := client.Do(req)
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

// triggerBlockCooldown pauses every worker for d after a suspicious redirect.
func triggerBlockCooldown(d time.Duration) {
	if d <= 0 {
		return
	}
	until := time.Now().Add(d).UnixNano()
	for {
		old := cliBlockUntil.Load()
		if until <= old || cliBlockUntil.CompareAndSwap(old, until) {
			return
		}
	}
}

// waitBlockCooldown sleeps until the shared cooldown expires.
func waitBlockCooldown(ctx context.Context, d time.Duration) error {
	for {
		until := cliBlockUntil.Load()
		if until == 0 {
			return nil
		}
		left := time.Until(time.Unix(0, until))
		if left <= 0 {
			return nil
		}
		t := time.NewTimer(left)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
			return nil
		}
	}
}
